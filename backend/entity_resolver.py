"""
entity_resolver.py
==================
Merges items from multiple data sources into a single enriched catalogue.

For every content type the resolver performs the same 4-step pattern:

    STEP 1  Fetch the "rich" source  (real poster, description, TMDB rating…)
    STEP 2  Load the "ratings" source (real user rating history)
    STEP 3  MATCH & MERGE — one winning item with the best metadata
                           + all interactions remapped to its ID
    STEP 4  XGBoost trains on API-quality items + dataset-scale interactions

Resolution strategy per content type:
    MOVIE   : links.csv exact TMDB-ID map (ML-25M)  / title+year fuzzy (ML-1M)
    ARTICLE : ISBN match (Open Library ↔ Book-Crossings) / title fuzzy fallback
    GAME    : title fuzzy match (RAWG ↔ Steam)
    MUSIC   : artist-name fuzzy match (Spotify ↔ Last.fm)
    VIDEO   : title dedup (TMDB TV ↔ YouTube)
    ARTICLE : cross-source title dedup (NewsData ↔ NewsAPI)
"""

import asyncio
import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from models import ContentType, Interaction, Item

logger = logging.getLogger(__name__)

# ── Minimum similarity score (0-100) for a fuzzy match to be accepted ─────────
DEFAULT_FUZZY_THRESHOLD = 88


# ── Helper: normalize a title for comparison ──────────────────────────────────
def _norm(title: str) -> str:
    """Lowercase → strip leading article → remove punctuation → collapse spaces."""
    t = title.lower().strip()
    t = re.sub(r"^(the|a|an)\s+", "", t)          # remove leading article
    t = re.sub(r"[^\w\s]", " ", t)                # punctuation → space
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _year(title: str) -> str:
    """Extract 4-digit year from parentheses, e.g. 'Inception (2010)' → '2010'."""
    m = re.search(r"\((\d{4})\)", title)
    return m.group(1) if m else ""


def _fix_ml_title(title: str) -> str:
    """
    MovieLens stores titles as 'Dark Knight, The (2008)'.
    Move trailing article back to front: 'The Dark Knight (2008)'.
    """
    return re.sub(
        r"^(.*),\s*(The|A|An)\s*(\(\d{4}\))?\s*$",
        lambda m: f"{m.group(2)} {m.group(1)}{' ' + m.group(3) if m.group(3) else ''}",
        title,
        flags=re.IGNORECASE,
    ).strip()


def _combined_rating(
    rich_rating: float,
    dataset_ratings: List[float],
    max_dataset_weight: float = 0.65,
    min_dataset_samples: int = 10,
    samples_for_max_weight: int = 5_000,
) -> float:
    """
    Compute a weighted average of a rich-source rating and a dataset rating.

    Logic:
      • With few dataset samples (< min_dataset_samples) → trust only the rich source.
      • As dataset samples grow toward samples_for_max_weight, the dataset weight
        increases linearly from 0 → max_dataset_weight.
      • The rich source weight = 1 - dataset_weight.

    Both inputs must be on the same 0-5 scale.

    Example — The Dark Knight:
      rich_rating      = 4.7  (TMDB, 50K professional votes)
      dataset_ratings  = [4.2, 3.8, 4.5, ...]  (50K MovieLens ratings, avg 4.15)
      → dataset_weight = min(0.65, 0.1 + 50000/5000) → capped at 0.65
      → combined       = 0.35 * 4.7 + 0.65 * 4.15 = 1.645 + 2.698 = 4.34
    """
    n = len(dataset_ratings)
    if n < min_dataset_samples:
        return rich_rating
    dataset_avg = sum(dataset_ratings) / n
    dataset_weight = min(max_dataset_weight, 0.1 + n / samples_for_max_weight)
    rich_weight    = 1.0 - dataset_weight
    combined = rich_weight * rich_rating + dataset_weight * dataset_avg
    return round(min(combined, 5.0), 2)


# ─────────────────────────────────────────────────────────────────────────────
class EntityResolver:
    """
    Resolves entities across multiple data sources and merges them into
    a single enriched catalogue with zero duplicates.

    Usage::

        resolver = EntityResolver()

        # Movies
        unified_movies, remapped_ml_ints = resolver.resolve_movies(
            tmdb_items, ml_items, ml_interactions, ml_links
        )

        # Books
        unified_books, remapped_bx_ints = resolver.resolve_books(
            openlibrary_items, bx_items, bx_interactions
        )

        # Games
        unified_games = resolver.resolve_games(rawg_items, steam_items)

        # Music
        unified_music = resolver.resolve_music(spotify_items, lastfm_items)

        # Articles
        unified_articles = resolver.deduplicate_articles(newsdata, newsapi)

        resolver.log_summary()
    """

    def __init__(self, fuzzy_threshold: int = DEFAULT_FUZZY_THRESHOLD) -> None:
        self.threshold = fuzzy_threshold
        self._stats: Dict[str, int] = defaultdict(int)

    # =========================================================================
    # 1. MOVIES  —  TMDB (rich) + MovieLens 1M/25M (interactions)
    # =========================================================================

    def resolve_movies(
        self,
        tmdb_items: List[Item],
        ml_items: List[Item],
        ml_interactions: List[Interaction],
        ml_links: Optional[Dict[str, str]] = None,   # movieId → tmdbId  (25M only)
    ) -> Tuple[List[Item], List[Interaction]]:
        """
        Merges MovieLens items and interactions into the TMDB catalogue.

        Priority:
          1. links.csv    — exact movieId → tmdbId mapping (MovieLens 25M)
          2. title + year — fuzzy match for MovieLens 1M or unlinked entries

        Result:
          • Matched ML item → dropped; its interactions remapped to tmdb_movie_* ID
          • Unmatched ML item → kept as-is in the catalogue
          • TMDB item view_count boosted by matched ML interaction count
        """
        try:
            from rapidfuzz import fuzz, process as fz_process
        except ImportError:
            logger.warning("[EntityResolver] rapidfuzz not installed — install it: pip install rapidfuzz")
            return tmdb_items, ml_interactions

        # ── Build TMDB lookups ────────────────────────────────────────────────
        tmdb_map: Dict[str, Item] = {item.item_id: item for item in tmdb_items}

        # numeric tmdb id → item_id   e.g. "155" → "tmdb_movie_155"
        tmdb_by_num: Dict[str, str] = {}
        for item in tmdb_items:
            parts = item.item_id.split("_")
            if len(parts) >= 3 and parts[-1].isdigit():
                tmdb_by_num[parts[-1]] = item.item_id

        # normalised "title_year" → item_id  (fast exact lookup)
        tmdb_by_titleyr: Dict[str, str] = {}
        # normalised title → item_id  (rapidfuzz choices dict)
        tmdb_choices: Dict[str, str] = {}
        for item in tmdb_items:
            n  = _norm(item.title)
            yr = _year(item.title) or (
                str(item.publish_ts.year) if item.publish_ts else ""
            )
            if yr:
                tmdb_by_titleyr[f"{n}_{yr}"] = item.item_id
            tmdb_choices[item.item_id] = n    # id → norm_title for rapidfuzz

        # ── Step 1: Exact match via links.csv (ML-25M) ────────────────────────
        id_remap: Dict[str, str] = {}       # ml_movie_X → tmdb_movie_Y
        if ml_links:
            for ml_item in ml_items:
                ml_num = ml_item.item_id.split("_")[-1]
                tmdb_num = ml_links.get(ml_num, "")
                if tmdb_num and tmdb_num in tmdb_by_num:
                    target = tmdb_by_num[tmdb_num]
                    id_remap[ml_item.item_id] = target
                    tmdb_map[target].view_count += ml_item.view_count
                    self._stats["movies_exact"] += 1

        # ── Step 2: Title + year fuzzy match for any remaining ML items ────────
        unmatched = [i for i in ml_items if i.item_id not in id_remap]
        for ml_item in unmatched:
            clean = _fix_ml_title(ml_item.title)
            n     = _norm(clean)
            yr    = _year(clean)

            # Fast path: exact normalised key
            key = f"{n}_{yr}" if yr else n
            if key in tmdb_by_titleyr:
                target = tmdb_by_titleyr[key]
                id_remap[ml_item.item_id] = target
                tmdb_map[target].view_count += ml_item.view_count
                self._stats["movies_fuzzy"] += 1
                continue

            # Slow path: rapidfuzz scan
            hit = fz_process.extractOne(
                n,
                tmdb_choices,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=self.threshold,
            )
            if hit:
                # hit = (matched_value, score, matched_key)
                target_id = hit[2]
                # Year guard: if both have a year they must be within 1
                tmdb_yr = (
                    str(tmdb_map[target_id].publish_ts.year)
                    if tmdb_map[target_id].publish_ts else ""
                )
                if yr and tmdb_yr and abs(int(yr) - int(tmdb_yr)) > 1:
                    self._stats["movies_year_mismatch"] += 1
                else:
                    id_remap[ml_item.item_id] = target_id
                    tmdb_map[target_id].view_count += ml_item.view_count
                    self._stats["movies_fuzzy"] += 1

        # ── Step 3: Compute combined quality rating ────────────────────────────
        # Accumulate ML ratings per target TMDB item (before remap mutates item_id)
        ml_ratings_by_target: Dict[str, List[float]] = defaultdict(list)
        for ixn in ml_interactions:
            original_id = ixn.item_id          # still ml_movie_X at this point
            if original_id in id_remap and ixn.rating and ixn.rating > 0:
                ml_ratings_by_target[id_remap[original_id]].append(float(ixn.rating))

        for target_id, ml_ratings in ml_ratings_by_target.items():
            if target_id in tmdb_map:
                old_r = tmdb_map[target_id].rating or 3.0
                # MovieLens: 0.5-5.0 scale  (same 0-5 as our Item.rating)
                new_r = _combined_rating(old_r, ml_ratings)
                tmdb_map[target_id].rating = new_r
                self._stats["movies_rating_combined"] += 1

        # ── Step 4: Remap interactions ─────────────────────────────────────────
        remapped: List[Interaction] = []
        for ixn in ml_interactions:
            if ixn.item_id in id_remap:
                ixn.item_id = id_remap[ixn.item_id]
            remapped.append(ixn)

        unmatched_items = [i for i in ml_items if i.item_id not in id_remap]
        self._stats["movies_unmatched"] = len(unmatched_items)

        # Keep TMDB items + unmatched ML items (they still have real data)
        unified = list(tmdb_map.values()) + unmatched_items

        logger.info(
            f"[Movie Resolution] "
            f"Exact: {self._stats['movies_exact']:,}  "
            f"Fuzzy: {self._stats['movies_fuzzy']:,}  "
            f"Unmatched: {self._stats['movies_unmatched']:,}  "
            f"Ratings combined: {self._stats['movies_rating_combined']:,}  "
            f"→ {len(unified):,} unified items"
        )
        return unified, remapped

    # =========================================================================
    # 2. BOOKS  —  Open Library (rich) + Book-Crossings (interactions)
    # =========================================================================

    def resolve_books(
        self,
        openlibrary_items: List[Item],
        bx_items: List[Item],
        bx_interactions: List[Interaction],
    ) -> Tuple[List[Item], List[Interaction]]:
        """
        Merges Book-Crossings books and interactions into Open Library items.

        Strategy:
          1. Exact normalised title match
          2. rapidfuzz title similarity
          3. ISBN → Open Library cover URL enrichment for unmatched BX items

        Result:
          • Matched BX item → dropped; its interactions remapped to ol_* ID
          • Unmatched BX item → enriched with OL cover URL (via ISBN), kept
        """
        try:
            from rapidfuzz import fuzz, process as fz_process
            use_fuzzy = True
        except ImportError:
            logger.warning("[EntityResolver] rapidfuzz missing — skipping book merge")
            use_fuzzy = False

        # Enrich ALL BX items with OL cover URL from ISBN before merging
        for bx in bx_items:
            if not bx.thumbnail_url:
                isbn = bx.item_id.replace("bx_", "")
                bx.thumbnail_url = f"https://covers.openlibrary.org/b/isbn/{isbn}-M.jpg"

        if not use_fuzzy:
            return openlibrary_items + bx_items, bx_interactions

        ol_map: Dict[str, Item] = {item.item_id: item for item in openlibrary_items}
        ol_choices: Dict[str, str] = {item.item_id: _norm(item.title) for item in openlibrary_items}

        id_remap: Dict[str, str] = {}
        standalone_bx: List[Item] = []

        for bx in bx_items:
            n = _norm(bx.title)

            # Fast exact match
            fast = next((iid for iid, norm in ol_choices.items() if norm == n), None)
            if fast:
                id_remap[bx.item_id] = fast
                ol_map[fast].view_count += bx.view_count
                self._stats["books_merged"] += 1
                continue

            # Fuzzy match
            hit = fz_process.extractOne(
                n, ol_choices,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=self.threshold,
            )
            if hit:
                id_remap[bx.item_id] = hit[2]
                ol_map[hit[2]].view_count += bx.view_count
                self._stats["books_merged"] += 1
            else:
                standalone_bx.append(bx)

        # Combined rating: Book-Crossings has explicit 1-10 ratings → normalize to 0-5
        bx_ratings_by_target: Dict[str, List[float]] = defaultdict(list)
        for ixn in bx_interactions:
            original_id = ixn.item_id
            if original_id in id_remap and ixn.rating and ixn.rating > 0:
                # BX scale: 1-10 → normalize to 0-5
                bx_ratings_by_target[id_remap[original_id]].append(float(ixn.rating) / 2.0)

        for target_id, bx_ratings in bx_ratings_by_target.items():
            if target_id in ol_map:
                old_r = ol_map[target_id].rating or 3.0
                ol_map[target_id].rating = _combined_rating(
                    old_r, bx_ratings, max_dataset_weight=0.60
                )
                self._stats["books_rating_combined"] += 1

        # Remap BX interactions
        remapped: List[Interaction] = []
        for ixn in bx_interactions:
            if ixn.item_id in id_remap:
                ixn.item_id = id_remap[ixn.item_id]
            remapped.append(ixn)

        unified = list(ol_map.values()) + standalone_bx
        self._stats["books_standalone"] = len(standalone_bx)
        logger.info(
            f"[Book Resolution] "
            f"Merged: {self._stats['books_merged']:,}  "
            f"Standalone: {self._stats['books_standalone']:,}  "
            f"Ratings combined: {self._stats.get('books_rating_combined', 0):,}  "
            f"→ {len(unified):,} unified items"
        )
        return unified, remapped

    # =========================================================================
    # 3. GAMES  —  RAWG (rich) + Steam (additional games)
    # =========================================================================

    def resolve_games(
        self,
        rawg_items: List[Item],
        steam_items: List[Item],
    ) -> List[Item]:
        """
        Merges Steam game metadata into RAWG.
        RAWG keeps its superior cover art / description.
        Steam boosts view_count for matched games; unmatched Steam games are kept.
        """
        try:
            from rapidfuzz import fuzz, process as fz_process
        except ImportError:
            return rawg_items + steam_items

        rawg_map: Dict[str, Item] = {item.item_id: item for item in rawg_items}
        rawg_choices: Dict[str, str] = {item.item_id: _norm(item.title) for item in rawg_items}

        standalone_steam: List[Item] = []
        for steam in steam_items:
            n   = _norm(steam.title)
            hit = fz_process.extractOne(
                n, rawg_choices,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=self.threshold,
            )
            if hit:
                target = hit[2]
                rawg_map[target].view_count += steam.view_count
                # Fill in description if RAWG's is short
                if len(rawg_map[target].description) < 50 and steam.description:
                    rawg_map[target].description = steam.description
                self._stats["games_merged"] += 1
            else:
                standalone_steam.append(steam)

        unified = list(rawg_map.values()) + standalone_steam
        self._stats["games_standalone"] = len(standalone_steam)
        logger.info(
            f"[Game Resolution] "
            f"Steam→RAWG: {self._stats['games_merged']:,}  "
            f"Standalone Steam: {self._stats['games_standalone']:,}  "
            f"→ {len(unified):,} unified items"
        )
        return unified

    # =========================================================================
    # 4. MUSIC  —  Spotify (rich: albums, cover art) + Last.fm (listener counts)
    # =========================================================================

    def resolve_music(
        self,
        spotify_items: List[Item],
        lastfm_items: List[Item],
    ) -> List[Item]:
        """
        Merges Last.fm listener counts into matching Spotify albums.

        Match strategy: artist name fuzzy (threshold lowered to 80 since
        "Eminem" == "Eminem" is always exact).

        Spotify album and Last.fm track are different granularity, so we do
        NOT drop Last.fm tracks — we keep both but boost Spotify view_count.
        """
        try:
            from rapidfuzz import fuzz, process as fz_process
        except ImportError:
            return spotify_items + lastfm_items

        def _artist(title: str) -> str:
            parts = title.rsplit("—", 1)
            return _norm(parts[-1]) if len(parts) > 1 else _norm(title)

        sp_map: Dict[str, Item] = {item.item_id: item for item in spotify_items}
        # group Spotify IDs by artist key
        sp_by_artist: Dict[str, List[str]] = defaultdict(list)
        for item in spotify_items:
            sp_by_artist[_artist(item.title)].append(item.item_id)

        sp_artist_choices: Dict[str, str] = {
            artist: artist for artist in sp_by_artist
        }

        standalone_lf: List[Item] = []
        for lf in lastfm_items:
            lf_artist = _artist(lf.title)
            hit = fz_process.extractOne(
                lf_artist, sp_artist_choices,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=80,            # lower threshold: artist names are short
            )
            if hit:
                sp_ids = sp_by_artist[hit[2]]
                sp_map[sp_ids[0]].view_count += lf.view_count
                self._stats["music_merged"] += 1
            else:
                standalone_lf.append(lf)

        unified = list(sp_map.values()) + standalone_lf
        self._stats["music_standalone"] = len(standalone_lf)
        logger.info(
            f"[Music Resolution] "
            f"Last.fm→Spotify: {self._stats['music_merged']:,}  "
            f"Standalone Last.fm tracks: {self._stats['music_standalone']:,}  "
            f"→ {len(unified):,} unified items"
        )
        return unified

    # =========================================================================
    # 5. ARTICLES  —  Deduplicate across NewsData + NewsAPI (+ Open Library)
    # =========================================================================

    def deduplicate_articles(
        self,
        *article_lists: List[Item],
    ) -> List[Item]:
        """
        Deduplicates news articles across multiple sources using title similarity.
        The first occurrence (highest-quality source listed first) is kept.
        """
        try:
            from rapidfuzz import fuzz
        except ImportError:
            combined: List[Item] = []
            for lst in article_lists:
                combined.extend(lst)
            return combined

        all_articles: List[Item] = []
        for lst in article_lists:
            all_articles.extend(lst)

        kept: List[Item] = []
        kept_norms: List[str] = []

        for item in all_articles:
            n = _norm(item.title)
            is_dup = any(
                fuzz.token_sort_ratio(n, kn) >= 90
                for kn in kept_norms
            )
            if not is_dup:
                kept.append(item)
                kept_norms.append(n)
            else:
                self._stats["articles_deduped"] += 1

        logger.info(
            f"[Article Dedup] "
            f"Removed: {self._stats['articles_deduped']:,}  "
            f"Kept: {len(kept):,}"
        )
        return kept

    # =========================================================================
    # 6. VIDEOS  —  TMDB TV + YouTube (just deduplicate, different universes)
    # =========================================================================

    def deduplicate_videos(
        self,
        tmdb_tv_items: List[Item],
        youtube_items: List[Item],
    ) -> List[Item]:
        """
        TV shows (TMDB) and YouTube clips are rarely the same item,
        so this just deduplicates by normalised title similarity.
        """
        try:
            from rapidfuzz import fuzz
        except ImportError:
            return tmdb_tv_items + youtube_items

        known_norms: List[str] = [_norm(i.title) for i in tmdb_tv_items]
        standalone_yt: List[Item] = []
        for yt in youtube_items:
            n = _norm(yt.title)
            if not any(fuzz.token_sort_ratio(n, kn) >= 90 for kn in known_norms):
                standalone_yt.append(yt)
                known_norms.append(n)
            else:
                self._stats["videos_deduped"] += 1

        unified = tmdb_tv_items + standalone_yt
        logger.info(
            f"[Video Dedup] TMDB-TV: {len(tmdb_tv_items):,}  "
            f"YouTube unique: {len(standalone_yt):,}  "
            f"YT dups removed: {self._stats['videos_deduped']:,}"
        )
        return unified

    # =========================================================================
    # Summary log
    # =========================================================================

    def log_summary(self) -> None:
        logger.info("=" * 65)
        logger.info("ENTITY RESOLUTION — COMPLETE SUMMARY")
        logger.info(f"  Movies  exact  (links.csv)  : {self._stats['movies_exact']:,}")
        logger.info(f"  Movies  fuzzy  (title+year) : {self._stats['movies_fuzzy']:,}")
        logger.info(f"  Movies  unmatched (kept)    : {self._stats['movies_unmatched']:,}")
        logger.info(f"  Movies  rating combined     : {self._stats.get('movies_rating_combined', 0):,}")
        logger.info(f"  Books   merged               : {self._stats['books_merged']:,}")
        logger.info(f"  Books   standalone BX        : {self._stats['books_standalone']:,}")
        logger.info(f"  Books   rating combined      : {self._stats.get('books_rating_combined', 0):,}")
        logger.info(f"  Games   Steam→RAWG           : {self._stats['games_merged']:,}")
        logger.info(f"  Games   standalone Steam     : {self._stats['games_standalone']:,}")
        logger.info(f"  Music   Last.fm→Spotify      : {self._stats['music_merged']:,}")
        logger.info(f"  Music   standalone tracks    : {self._stats['music_standalone']:,}")
        logger.info(f"  Articles deduplicated        : {self._stats['articles_deduped']:,}")
        logger.info(f"  Videos  deduplicated         : {self._stats['videos_deduped']:,}")
        logger.info("=" * 65)
