"""
dataset_loader.py
=================
Downloads and parses FREE public datasets (no API key needed):

  - Book-Crossings   → ARTICLE  (278,000 books, from GroupLens)
  - Steam Games      → GAME     (real game metadata from UCSD)
  - Amazon Products  → PRODUCT  (electronics metadata from UCSD)
  - MovieLens 25M    → MOVIE    (upgrade from 1M — 25 million ratings)

All datasets are auto-downloaded on first run and cached locally so
subsequent boot are instant (no re-download).
"""

import asyncio
import csv
import io
import json
import logging
import os
import random
import zipfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import ContentType, Interaction, InteractionType, Item, User

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".dataset_cache")

# ── Dataset URLs ──────────────────────────────────────────────────────────────
BOOK_CROSSINGS_URL  = "https://files.grouplens.org/datasets/book-crossing/BX-CSV-Dump.zip"
ML_25M_URL          = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"

# Steam + Amazon: curated GitHub-hosted mirrors (small, fast, no auth needed)
STEAM_GAMES_URL     = (
    "https://raw.githubusercontent.com/nik-davis/steam-recommendation-study"
    "/master/data/steam_games.json"
)
AMAZON_ELEC_URL     = (
    "https://raw.githubusercontent.com/nicholasjhana/amazon-product-recommender"
    "/master/data/Electronics_5.json"
)

COUNTRIES = ["US", "UK", "CA", "AU", "DE", "FR", "IN", "JP", "BR", "MX"]
DEVICES   = ["web", "mobile", "tablet", "smart_tv"]


def _os_makedirs():
    os.makedirs(CACHE_DIR, exist_ok=True)


async def _download_cached(url: str, filename: str) -> bytes:
    """Download a file if not already cached; return raw bytes."""
    _os_makedirs()
    path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(path):
        logger.info(f"[Cache] Using cached {filename}")
        with open(path, "rb") as fh:
            return fh.read()

    logger.info(f"[Download] Fetching {url} …")
    async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
        try:
            r = await client.get(url)
            r.raise_for_status()
            data = r.content
            with open(path, "wb") as fh:
                fh.write(data)
            logger.info(f"[Download] {filename} saved ({len(data)//1024} KB)")
            return data
        except Exception as exc:
            logger.warning(f"[Download] Failed {url}: {exc}")
            return b""


def _rand_past_date(years: int = 5) -> datetime:
    days = random.randint(0, 365 * years)
    from datetime import timedelta
    return datetime.now(timezone.utc) - timedelta(days=days)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Book-Crossings (ARTICLE / books)
# ─────────────────────────────────────────────────────────────────────────────

class BookCrossingsLoader:
    """
    GroupLens Book-Crossings dataset.
    Files inside zip:
      BX-Books.csv         : ISBN ; Book-Title ; Book-Author ; Year ; Publisher ; Image-URL-S ; Image-URL-M ; Image-URL-L
      BX-Book-Ratings.csv  : User-ID ; ISBN ; Book-Rating
      BX-Users.csv         : User-ID ; Location ; Age
    """

    async def load(self, max_items: int = 5000, max_interactions: int = 100_000
                   ) -> Tuple[List[Item], List[User], List[Interaction]]:
        raw = await _download_cached(BOOK_CROSSINGS_URL, "bx.zip")
        if not raw:
            logger.warning("[BookCrossings] Download failed — returning empty.")
            return [], [], []

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._parse, raw, max_items, max_interactions)

    def _parse(self, raw: bytes, max_items: int, max_interactions: int
               ) -> Tuple[List[Item], List[User], List[Interaction]]:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            # Files use ISO-8859-1 encoding and semicolons
            books_txt   = zf.read("BX-Books.csv").decode("iso-8859-1", errors="replace")
            ratings_txt = zf.read("BX-Book-Ratings.csv").decode("iso-8859-1", errors="replace")
            users_txt   = zf.read("BX-Users.csv").decode("iso-8859-1", errors="replace")

        # ── Books ─────────────────────────────────────────────────────────────
        books: Dict[str, dict] = {}
        reader = csv.DictReader(io.StringIO(books_txt), delimiter=";", quotechar='"')
        count = 0
        for row in reader:
            if count >= max_items:
                break
            isbn = (row.get("ISBN") or "").strip()
            title = (row.get("Book-Title") or "").strip()
            if not isbn or not title:
                continue
            books[isbn] = {
                "title":  title,
                "author": (row.get("Book-Author") or "Unknown").strip(),
                "year":   (row.get("Year-Of-Publication") or "2000").strip(),
                "img":    (row.get("Image-URL-M") or "").strip(),
            }
            count += 1

        items = [
            Item(
                item_id=f"bx_{isbn}",
                title=info["title"],
                content_type=ContentType.ARTICLE,
                category="Books",
                tags=[info["author"], "Books"],
                description=f"By {info['author']}. Published: {info['year']}.",
                thumbnail_url=info["img"],
                rating=round(random.uniform(3.0, 5.0), 1),
                view_count=random.randint(50, 20_000),
                publish_ts=_rand_past_date(),
            )
            for isbn, info in books.items()
        ]

        # ── Users ─────────────────────────────────────────────────────────────
        users_raw: Dict[str, dict] = {}
        u_reader = csv.DictReader(io.StringIO(users_txt), delimiter=";", quotechar='"')
        for row in u_reader:
            uid = (row.get("User-ID") or "").strip()
            if uid:
                users_raw[uid] = {"location": (row.get("Location") or "").strip()}

        ct_values = [ct.value for ct in ContentType]
        users = [
            User(
                user_id=f"bx_user_{uid}",
                country=random.choice(COUNTRIES),
                device=random.choice(DEVICES),
                age_group=random.choice(["18-24", "25-34", "35-44", "45-54", "55-64"]),
                preferences=random.sample(ct_values, k=random.randint(1, 4)),
            )
            for uid in users_raw
        ]

        # ── Interactions from ratings ─────────────────────────────────────────
        all_ratings = []
        r_reader = csv.DictReader(io.StringIO(ratings_txt), delimiter=";", quotechar='"')
        for row in r_reader:
            uid  = (row.get("User-ID") or "").strip()
            isbn = (row.get("ISBN") or "").strip()
            try:
                score = float(row.get("Book-Rating", 0))
            except ValueError:
                score = 0.0
            if uid and isbn and isbn in books and score > 0:
                all_ratings.append((uid, isbn, score))

        sample = random.sample(all_ratings, min(max_interactions, len(all_ratings)))
        from movielens_loader import _rating_to_interaction, _dwell_from_type
        interactions = [
            Interaction(
                user_id=f"bx_user_{uid}",
                item_id=f"bx_{isbn}",
                interaction_type=_rating_to_interaction(score),
                timestamp=_rand_past_date(years=3),
                dwell_seconds=_dwell_from_type(_rating_to_interaction(score)),
                rating=score,
                context={"source": "book_crossings"},
            )
            for uid, isbn, score in sample
        ]

        logger.info(
            f"[BookCrossings] {len(items)} books, {len(users)} users, "
            f"{len(interactions)} interactions"
        )
        return items, users, interactions


# ─────────────────────────────────────────────────────────────────────────────
# 2. Steam Games (GAME)
# ─────────────────────────────────────────────────────────────────────────────

STEAM_GENRES = ["Action", "Adventure", "RPG", "Strategy",
                "Simulation", "Puzzle", "Sports", "Indie"]


class SteamLoader:
    """
    Downloads a curated Steam game metadata JSON (no auth needed).
    Falls back to an empty list if the download fails.
    """

    async def load(self, max_items: int = 2000) -> List[Item]:
        raw = await _download_cached(STEAM_GAMES_URL, "steam_games.json")
        if not raw:
            logger.warning("[Steam] Download failed — returning empty.")
            return []

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._parse, raw, max_items)

    def _parse(self, raw: bytes, max_items: int) -> List[Item]:
        try:
            games = json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            # File might be newline-delimited JSON
            lines = raw.decode("utf-8", errors="replace").strip().splitlines()
            games = []
            for line in lines:
                try:
                    games.append(json.loads(line))
                except Exception:
                    continue

        if isinstance(games, dict):
            games = list(games.values())

        items = []
        for g in games[:max_items]:
            if not isinstance(g, dict):
                continue
            name = (g.get("name") or g.get("app_name") or "").strip()
            if not name:
                continue
            genres  = g.get("genres", [])
            if isinstance(genres, list):
                genre_strs = [
                    (x if isinstance(x, str) else x.get("description", ""))
                    for x in genres
                ]
            else:
                genre_strs = []
            category = genre_strs[0] if genre_strs else random.choice(STEAM_GENRES)
            tags     = genre_strs[:5] or [category]

            price_raw = g.get("price", g.get("original_price", ""))
            is_free   = (isinstance(price_raw, str) and "free" in price_raw.lower()) or price_raw == 0

            items.append(Item(
                item_id=f"steam_{g.get('id', uuid_hex())}",
                title=name,
                content_type=ContentType.GAME,
                category=category,
                tags=tags,
                description=(
                    f"{g.get('short_description') or g.get('desc', '')} "
                    f"{'(Free to Play)' if is_free else ''}"
                ).strip(),
                thumbnail_url=g.get("header_image", g.get("img_url", "")),
                rating=round(random.uniform(3.0, 5.0), 1),
                view_count=random.randint(100, 500_000),
                publish_ts=_rand_past_date(),
            ))

        logger.info(f"[Steam] {len(items)} games loaded")
        return items


def uuid_hex() -> str:
    import uuid
    return uuid.uuid4().hex[:10]


# ─────────────────────────────────────────────────────────────────────────────
# 3. Amazon Products — Electronics (PRODUCT)
# ─────────────────────────────────────────────────────────────────────────────

class AmazonProductLoader:
    """
    Loads publicly available Amazon product metadata from a GitHub-hosted
    subset (no registration needed).  Falls back to empty list on failure.
    Content type: PRODUCT.
    """

    async def load(self, max_items: int = 2000, max_interactions: int = 50_000
                   ) -> Tuple[List[Item], List[Interaction]]:
        raw = await _download_cached(AMAZON_ELEC_URL, "amazon_electronics.json")
        if not raw:
            logger.warning("[Amazon] Download failed — returning empty.")
            return [], []

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._parse, raw, max_items, max_interactions)

    def _parse(self, raw: bytes, max_items: int, max_interactions: int
               ) -> Tuple[List[Item], List[Interaction]]:
        lines  = raw.decode("utf-8", errors="replace").strip().splitlines()
        items  = []
        seen_asins: set = set()

        # Category mapping for Electronics sub-categories
        categories = ["Electronics", "Computers", "Phones", "Cameras",
                      "Audio", "TV & Video", "Car Electronics", "Wearables"]

        for line in lines[:max_items * 3]:      # over-read, deduplicate below
            if len(items) >= max_items:
                break
            try:
                record = json.loads(line)
            except Exception:
                continue

            asin  = record.get("asin", "").strip()
            title = record.get("title", record.get("summary", "")).strip()
            if not asin or not title or asin in seen_asins:
                continue
            seen_asins.add(asin)

            cat   = record.get("category", [])
            if isinstance(cat, list):
                cat_str = cat[-1] if cat else "Electronics"
            else:
                cat_str = str(cat) or "Electronics"
            if cat_str not in categories:
                cat_str = "Electronics"

            score = record.get("overall", 0)
            try:
                score = float(score)
            except (TypeError, ValueError):
                score = 3.0

            items.append(Item(
                item_id=f"amz_{asin}",
                title=title[:200],
                content_type=ContentType.PRODUCT,
                category=cat_str,
                tags=[cat_str, "Electronics"],
                description=(record.get("reviewText") or record.get("description") or "")[:400],
                thumbnail_url=record.get("imUrl", record.get("image", "")),
                rating=round(min(score, 5.0), 1),
                view_count=random.randint(50, 50_000),
                publish_ts=_rand_past_date(),
            ))

        # Build synthetic interactions from ratings (reviewer → product)
        from movielens_loader import _rating_to_interaction, _dwell_from_type
        raw_ratings = []
        for line in lines[:max_interactions * 3]:
            if len(raw_ratings) >= max_interactions:
                break
            try:
                r = json.loads(line)
            except Exception:
                continue
            reviewer = r.get("reviewerID", "")
            asin     = r.get("asin", "")
            score    = r.get("overall", 0)
            if reviewer and asin and f"amz_{asin}" in {i.item_id for i in items}:
                try:
                    raw_ratings.append((reviewer, asin, float(score)))
                except (TypeError, ValueError):
                    continue

        interactions = [
            Interaction(
                user_id=f"amz_user_{uid}",
                item_id=f"amz_{asin}",
                interaction_type=_rating_to_interaction(score),
                timestamp=_rand_past_date(),
                dwell_seconds=_dwell_from_type(_rating_to_interaction(score)),
                rating=score,
                context={"source": "amazon_reviews"},
            )
            for uid, asin, score in raw_ratings[:max_interactions]
        ]

        logger.info(f"[Amazon] {len(items)} products, {len(interactions)} interactions")
        return items, interactions


# ─────────────────────────────────────────────────────────────────────────────
# 4. MovieLens 25M (upgrade from 1M)
# ─────────────────────────────────────────────────────────────────────────────

class MovieLens25MLoader:
    """
    Same schema as MovieLens1M loader but uses the much larger 25M dataset.
    62,000 movies, 162,000 users, 25,000,000 ratings.

    Files inside the zip use CSV format (unlike 1M which uses ::).
    """

    async def load(
        self,
        max_interactions: int = 500_000,
    ) -> Tuple[List[User], List[Item], List[Interaction], Dict[str, str]]:
        """
        Returns (users, items, interactions, links_dict)
        links_dict: movieId → tmdbId  (string → string)
        Used by EntityResolver for exact TMDB-ID-based movie merging.
        """
        raw = await _download_cached(ML_25M_URL, "ml-25m.zip")
        if not raw:
            logger.warning("[ML-25M] Download failed.")
            return [], [], [], {}

        logger.info("[ML-25M] Parsing (running in executor)…")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._parse, raw, max_interactions)

    def _parse(self, raw: bytes, max_interactions: int
               ) -> Tuple[List[User], List[Item], List[Interaction], Dict[str, str]]:
        """Parse movies.csv, ratings.csv, AND links.csv.
        Returns (users, items, interactions, links_dict).
        links_dict maps movieId → tmdbId (both as strings).
        """
        import re
        from movielens_loader import (
            GENRE_CATEGORY, _rating_to_interaction, _dwell_from_type
        )

        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            movies_txt  = zf.read("ml-25m/movies.csv").decode("utf-8")
            ratings_txt = zf.read("ml-25m/ratings.csv").decode("utf-8")
            # links.csv: movieId,imdbId,tmdbId  — exact TMDB ID mapping
            try:
                links_txt = zf.read("ml-25m/links.csv").decode("utf-8")
            except KeyError:
                links_txt = ""

        # ── links.csv: movieId → tmdbId ──────────────────────────────────────
        links_dict: Dict[str, str] = {}
        if links_txt:
            l_reader = csv.DictReader(io.StringIO(links_txt))
            for row in l_reader:
                mid   = (row.get("movieId") or "").strip()
                tmdb  = (row.get("tmdbId")  or "").strip()
                if mid and tmdb:
                    links_dict[mid] = tmdb

        movies: Dict[str, dict] = {}
        reader = csv.DictReader(io.StringIO(movies_txt))
        for row in reader:
            mid    = row.get("movieId", "").strip()
            title  = (row.get("title") or "").strip()
            genres = (row.get("genres") or "").split("|")
            if not mid or not title:
                continue
            year_m = re.search(r"\((\d{4})\)\s*$", title)
            year   = year_m.group(1) if year_m else "2000"
            clean  = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()
            cat    = GENRE_CATEGORY.get(genres[0], "Drama")
            movies[mid] = {"title": clean, "year": year, "genres": genres, "category": cat}

        # ── ratings.csv: userId,movieId,rating,timestamp ──────────────────────
        all_ratings = []
        r_reader = csv.DictReader(io.StringIO(ratings_txt))
        for row in r_reader:
            uid   = row.get("userId", "").strip()
            mid   = row.get("movieId", "").strip()
            score = row.get("rating", "0")
            ts    = row.get("timestamp", "0")
            if uid and mid and mid in movies:
                try:
                    all_ratings.append((uid, mid, float(score), int(ts)))
                except (ValueError, TypeError):
                    continue

        # Build User list from unique user IDs in sampled ratings
        ct_values = [ct.value for ct in ContentType]
        sample    = random.sample(all_ratings, min(max_interactions, len(all_ratings)))
        user_ids  = {uid for uid, *_ in sample}
        users = [
            User(
                user_id=f"ml25_user_{uid}",
                country=random.choice(COUNTRIES),
                device=random.choice(DEVICES),
                age_group=random.choice(["18-24", "25-34", "35-44", "45-54", "55-64"]),
                preferences=random.sample(ct_values, k=random.randint(1, 4)),
            )
            for uid in user_ids
        ]

        items = [
            Item(
                item_id=f"ml25_movie_{mid}",
                title=info["title"],
                content_type=ContentType.MOVIE,
                category=info["category"],
                tags=info["genres"],
                description=f"Year: {info['year']}. Genres: {', '.join(info['genres'])}.",
                thumbnail_url="",
                rating=round(random.uniform(2.5, 5.0), 1),
                view_count=random.randint(100, 500_000),
                publish_ts=datetime(int(info["year"]) if info["year"].isdigit() else 2000,
                                    6, 1, tzinfo=timezone.utc),
            )
            for mid, info in movies.items()
        ]

        interactions = [
            Interaction(
                user_id=f"ml25_user_{uid}",
                item_id=f"ml25_movie_{mid}",
                interaction_type=_rating_to_interaction(score),
                timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
                dwell_seconds=_dwell_from_type(_rating_to_interaction(score)),
                rating=score,
                context={"source": "movielens_25m"},
            )
            for uid, mid, score, ts in sample
        ]

        logger.info(
            f"[ML-25M] {len(items)} movies, {len(users)} users, "
            f"{len(interactions)} interactions, "
            f"{len(links_dict)} TMDB links"
        )
        return users, items, interactions, links_dict
