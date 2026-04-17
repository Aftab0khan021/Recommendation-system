"""
real_data_fetcher.py
====================
Fetches real items from 9 free public APIs/sources.

Sources included (all FREE):
  API Sources (need API key — free registration):
    TMDB          → movies (movie)         themoviedb.org/settings/api
    TMDB          → TV shows (video)       themoviedb.org/settings/api
    RAWG.io       → games (game)           rawg.io/apidocs
    Last.fm       → music/podcasts         last.fm/api/account/create
    Spotify       → music albums           developer.spotify.com (CLIENT_ID + SECRET)
    YouTube       → videos                 console.cloud.google.com (YouTube Data API v3)
    NewsData.io   → news articles          newsdata.io/register
    NewsAPI       → news articles (2nd)    newsapi.org/register

  No-key sources (work out of the box):
    Open Library  → books/articles         openlibrary.org
    Open Food Facts → products             world.openfoodfacts.org

Any key that is missing is silently skipped — other sources still run.
"""

import asyncio
import base64
import logging
import os
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import httpx

from models import Item, ContentType

logger = logging.getLogger(__name__)

# ── API credentials (set in backend/.env) ────────────────────────────────────
TMDB_API_KEY        = os.getenv("TMDB_API_KEY", "")
TMDB_BASE           = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE     = "https://image.tmdb.org/t/p/w500"

RAWG_API_KEY        = os.getenv("RAWG_API_KEY", "")
RAWG_BASE           = "https://api.rawg.io/api"

NEWSDATA_API_KEY    = os.getenv("NEWSDATA_API_KEY", "")
NEWSDATA_BASE       = "https://newsdata.io/api/1"

NEWSAPI_KEY         = os.getenv("NEWSAPI_KEY", "")

LASTFM_API_KEY      = os.getenv("LASTFM_API_KEY", "")
LASTFM_BASE         = "https://ws.audioscrobbler.com/2.0"

YOUTUBE_API_KEY     = os.getenv("YOUTUBE_API_KEY", "")

SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")

# ── Genre / category maps ────────────────────────────────────────────────────
TMDB_MOVIE_GENRE: Dict[int, str] = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Sci-Fi",
    53: "Thriller", 10752: "War", 37: "Western",
}
TMDB_TV_GENRE: Dict[int, str] = {
    10759: "Action", 16: "Animation", 35: "Comedy", 80: "Crime",
    99: "Documentary", 18: "Drama", 10751: "Family", 9648: "Mystery",
    10765: "Sci-Fi", 10766: "Soap", 37: "Western",
}
YOUTUBE_CATEGORIES: Dict[str, str] = {
    "1": "Film", "2": "Autos", "10": "Music", "15": "Pets",
    "17": "Sports", "19": "Travel", "20": "Gaming",
    "22": "People", "23": "Comedy", "24": "Entertainment",
    "25": "News", "26": "How-To", "27": "Education",
    "28": "Science", "29": "Nonprofits",
}
MUSIC_GENRES = [
    "Pop", "Rock", "Hip-Hop", "Electronic", "Jazz",
    "Classical", "Country", "R&B", "Indie", "Metal",
]


def _rand_past_date(max_days: int = 365 * 3) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=random.randint(0, max_days))


class RealDataFetcher:
    """
    Async context-manager that fetches from all 10 real-data sources.

    Usage::
        async with RealDataFetcher() as fetcher:
            items = await fetcher.fetch_all()
    """

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "RealDataFetcher":
        self._client = httpx.AsyncClient(timeout=25.0, follow_redirects=True)
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    async def _get(self, url: str, params: dict) -> dict:
        try:
            r = await self._client.get(url, params=params)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            logger.warning(f"API GET failed [{url}]: {exc}")
            return {}

    # =========================================================================
    # 1. TMDB — Movies
    # =========================================================================
    async def fetch_tmdb_movies(self, pages: int = 10) -> List[Item]:
        if not TMDB_API_KEY:
            logger.warning("TMDB_API_KEY not set — skipping movies.")
            return []
        items: List[Item] = []
        for page in range(1, pages + 1):
            data = await self._get(f"{TMDB_BASE}/movie/popular", {
                "api_key": TMDB_API_KEY, "language": "en-US", "page": page,
            })
            for m in data.get("results", []):
                genres  = [TMDB_MOVIE_GENRE.get(g, "Drama") for g in m.get("genre_ids", [])]
                poster  = m.get("poster_path") or ""
                items.append(Item(
                    item_id=f"tmdb_movie_{m['id']}",
                    title=m.get("title", "Unknown"),
                    content_type=ContentType.MOVIE,
                    category=genres[0] if genres else "Drama",
                    tags=genres,
                    description=m.get("overview", ""),
                    thumbnail_url=f"{TMDB_IMAGE_BASE}{poster}" if poster else "",
                    rating=round(m.get("vote_average", 0) / 2, 1),
                    view_count=m.get("vote_count", 0),
                    publish_ts=_rand_past_date(),
                ))
            await asyncio.sleep(0.25)
        logger.info(f"[TMDB Movies] {len(items)} items")
        return items

    # =========================================================================
    # 2. TMDB — TV Shows
    # =========================================================================
    async def fetch_tmdb_tv(self, pages: int = 5) -> List[Item]:
        if not TMDB_API_KEY:
            logger.warning("TMDB_API_KEY not set — skipping TV shows.")
            return []
        items: List[Item] = []
        for page in range(1, pages + 1):
            data = await self._get(f"{TMDB_BASE}/tv/popular", {
                "api_key": TMDB_API_KEY, "language": "en-US", "page": page,
            })
            for s in data.get("results", []):
                genres = [TMDB_TV_GENRE.get(g, "Drama") for g in s.get("genre_ids", [])]
                poster = s.get("poster_path") or ""
                items.append(Item(
                    item_id=f"tmdb_tv_{s['id']}",
                    title=s.get("name", "Unknown"),
                    content_type=ContentType.VIDEO,
                    category=genres[0] if genres else "Drama",
                    tags=genres,
                    description=s.get("overview", ""),
                    thumbnail_url=f"{TMDB_IMAGE_BASE}{poster}" if poster else "",
                    rating=round(s.get("vote_average", 0) / 2, 1),
                    view_count=s.get("vote_count", 0),
                    publish_ts=_rand_past_date(),
                ))
            await asyncio.sleep(0.25)
        logger.info(f"[TMDB TV] {len(items)} items")
        return items

    # =========================================================================
    # 3. RAWG.io — Games
    # =========================================================================
    async def fetch_rawg_games(self, pages: int = 10) -> List[Item]:
        if not RAWG_API_KEY:
            logger.warning("RAWG_API_KEY not set — skipping games.")
            return []
        items: List[Item] = []
        for page in range(1, pages + 1):
            data = await self._get(f"{RAWG_BASE}/games", {
                "key": RAWG_API_KEY, "page": page,
                "page_size": 40, "ordering": "-rating",
            })
            for g in data.get("results", []):
                genres   = [x["name"] for x in g.get("genres", [])]
                tags_raw = [t["name"] for t in g.get("tags", [])[:5]]
                platforms = ", ".join(p["platform"]["name"] for p in g.get("platforms", [])[:3])
                items.append(Item(
                    item_id=f"rawg_{g['id']}",
                    title=g.get("name", "Unknown"),
                    content_type=ContentType.GAME,
                    category=genres[0] if genres else "Action",
                    tags=genres + tags_raw,
                    description=(f"Released: {g.get('released','N/A')}. "
                                 f"Metacritic: {g.get('metacritic','N/A')}. "
                                 f"Platforms: {platforms}"),
                    thumbnail_url=g.get("background_image", ""),
                    rating=round(g.get("rating", 0), 1),
                    view_count=g.get("ratings_count", 0),
                    publish_ts=_rand_past_date(),
                ))
            await asyncio.sleep(0.25)
        logger.info(f"[RAWG] {len(items)} items")
        return items

    # =========================================================================
    # 4. Open Library — Books  (NO API KEY NEEDED)
    # =========================================================================
    async def fetch_open_library_books(
        self,
        subjects: Optional[List[str]] = None,
        limit_per_subject: int = 80,
    ) -> List[Item]:
        subjects = subjects or [
            "science", "technology", "history", "fiction",
            "biography", "business", "psychology", "art",
        ]
        items: List[Item] = []
        seen: set = set()
        for subject in subjects:
            data = await self._get(
                f"https://openlibrary.org/subjects/{subject}.json",
                {"limit": limit_per_subject},
            )
            for w in data.get("works", []):
                key = w.get("key", "")
                if not key or key in seen:
                    continue
                seen.add(key)
                cover_id  = w.get("cover_id")
                thumbnail = (f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg"
                             if cover_id else "")
                authors = [a.get("name", "") for a in w.get("authors", [])]
                items.append(Item(
                    item_id=f"ol_{key.replace('/works/','')}",
                    title=w.get("title", "Unknown"),
                    content_type=ContentType.ARTICLE,
                    category=subject.capitalize(),
                    tags=[subject] + authors[:2],
                    description=(f"By {', '.join(authors[:2]) or 'Unknown Author'}. "
                                 f"Subject: {subject.capitalize()}."),
                    thumbnail_url=thumbnail,
                    rating=round(random.uniform(3.0, 5.0), 1),
                    view_count=random.randint(100, 50_000),
                    publish_ts=_rand_past_date(),
                ))
            await asyncio.sleep(0.5)
        logger.info(f"[Open Library] {len(items)} items")
        return items

    # =========================================================================
    # 5. NewsData.io — Articles
    # =========================================================================
    async def fetch_newsdata_articles(self) -> List[Item]:
        if not NEWSDATA_API_KEY:
            logger.warning("NEWSDATA_API_KEY not set — skipping NewsData articles.")
            return []
        items: List[Item] = []
        categories = ["technology", "science", "business", "health", "sports", "entertainment"]
        for cat in categories:
            data = await self._get(f"{NEWSDATA_BASE}/news", {
                "apikey": NEWSDATA_API_KEY,
                "country": "us", "language": "en", "category": cat,
            })
            for article in data.get("results", []):
                title = article.get("title") or ""
                if not title:
                    continue
                items.append(Item(
                    item_id=f"newsdata_{uuid.uuid4().hex[:12]}",
                    title=title,
                    content_type=ContentType.ARTICLE,
                    category=cat.capitalize(),
                    tags=[cat, article.get("source_name", "News")],
                    description=(article.get("description") or article.get("content") or ""),
                    thumbnail_url=article.get("image_url") or "",
                    rating=round(random.uniform(3.0, 4.5), 1),
                    view_count=random.randint(500, 100_000),
                    publish_ts=_rand_past_date(max_days=30),
                ))
            await asyncio.sleep(1.2)
        logger.info(f"[NewsData] {len(items)} items")
        return items

    # =========================================================================
    # 6. NewsAPI (newsapi.org) — Articles (second news source)
    # =========================================================================
    async def fetch_newsapi_articles(self, pages: int = 3) -> List[Item]:
        if not NEWSAPI_KEY:
            logger.warning("NEWSAPI_KEY not set — skipping NewsAPI articles.")
            return []
        items: List[Item] = []
        topics = ["technology", "science", "business", "health", "sports", "entertainment"]
        for topic in topics:
            for page in range(1, pages + 1):
                data = await self._get("https://newsapi.org/v2/top-headlines", {
                    "apiKey": NEWSAPI_KEY, "category": topic,
                    "country": "us", "pageSize": 100, "page": page,
                })
                articles = data.get("articles", [])
                if not articles:
                    break
                for article in articles:
                    title = (article.get("title") or "").strip()
                    if not title or title == "[Removed]":
                        continue
                    items.append(Item(
                        item_id=f"newsapi_{uuid.uuid4().hex[:12]}",
                        title=title[:200],
                        content_type=ContentType.ARTICLE,
                        category=topic.capitalize(),
                        tags=[topic, article.get("source", {}).get("name", "News")],
                        description=(article.get("description") or article.get("content") or "")[:500],
                        thumbnail_url=article.get("urlToImage") or "",
                        rating=round(random.uniform(3.0, 4.5), 1),
                        view_count=random.randint(500, 200_000),
                        publish_ts=_rand_past_date(max_days=7),
                    ))
                await asyncio.sleep(1.0)
        logger.info(f"[NewsAPI] {len(items)} items")
        return items

    # =========================================================================
    # 7. Last.fm — Music + Podcasts
    # =========================================================================
    async def fetch_lastfm_tracks(self, pages: int = 10) -> List[Item]:
        if not LASTFM_API_KEY:
            logger.warning("LASTFM_API_KEY not set — skipping Last.fm music.")
            return []
        items: List[Item] = []
        for page in range(1, pages + 1):
            data = await self._get(LASTFM_BASE, {
                "method": "chart.gettoptracks",
                "api_key": LASTFM_API_KEY,
                "format": "json", "page": page, "limit": 50,
            })
            tracks = data.get("tracks", {}).get("track", [])
            for i, t in enumerate(tracks):
                artist    = t.get("artist", {}).get("name", "Unknown")
                ctype     = ContentType.PODCAST if i % 5 == 0 else ContentType.MUSIC
                genre     = random.choice(MUSIC_GENRES)
                images    = t.get("image", [])
                thumbnail = images[-1].get("#text", "") if images else ""
                items.append(Item(
                    item_id=f"lastfm_{uuid.uuid4().hex[:12]}",
                    title=f"{t.get('name','Unknown')} — {artist}",
                    content_type=ctype,
                    category=genre,
                    tags=[artist, genre, "Music"],
                    description=(f"Artist: {artist}. "
                                 f"Listeners: {t.get('listeners','N/A')}. "
                                 f"Play count: {t.get('playcount','N/A')}"),
                    thumbnail_url=thumbnail,
                    rating=round(random.uniform(3.5, 5.0), 1),
                    view_count=int(t.get("listeners", 0) or 0),
                    duration_seconds=random.randint(120, 360),
                    publish_ts=_rand_past_date(),
                ))
            await asyncio.sleep(0.3)
        logger.info(f"[Last.fm] {len(items)} items")
        return items

    # =========================================================================
    # 8. Spotify Web API — Music Albums (Client Credentials, no user login)
    # =========================================================================
    async def fetch_spotify_music(self, pages: int = 10) -> List[Item]:
        if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
            logger.warning("SPOTIFY_CLIENT_ID/SECRET not set — skipping Spotify.")
            return []
        # Get OAuth token via Client Credentials
        try:
            creds_b64 = base64.b64encode(
                f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
            ).decode()
            token_r = await self._client.post(
                "https://accounts.spotify.com/api/token",
                headers={"Authorization": f"Basic {creds_b64}"},
                data={"grant_type": "client_credentials"},
            )
            token_r.raise_for_status()
            token = token_r.json().get("access_token", "")
        except Exception as exc:
            logger.warning(f"[Spotify] Token fetch failed: {exc}")
            return []

        headers = {"Authorization": f"Bearer {token}"}
        items: List[Item] = []
        markets = ["US", "GB", "AU", "CA", "IN"]
        for market in markets:
            try:
                r = await self._client.get(
                    "https://api.spotify.com/v1/browse/new-releases",
                    headers=headers,
                    params={"country": market, "limit": 50},
                )
                r.raise_for_status()
                albums = r.json().get("albums", {}).get("items", [])
            except Exception as exc:
                logger.warning(f"[Spotify] new-releases {market}: {exc}")
                continue
            for album in albums:
                artists  = [a["name"] for a in album.get("artists", [])]
                artist   = artists[0] if artists else "Unknown"
                images   = album.get("images", [])
                thumb    = images[0].get("url", "") if images else ""
                genre    = random.choice(MUSIC_GENRES)
                items.append(Item(
                    item_id=f"spotify_{album.get('id', uuid.uuid4().hex[:10])}",
                    title=f"{album.get('name','Unknown')} — {artist}",
                    content_type=ContentType.MUSIC,
                    category=genre,
                    tags=[artist, genre, album.get("album_type", "album").capitalize()],
                    description=(f"Artist: {artist}. "
                                 f"Released: {album.get('release_date','N/A')}. "
                                 f"Tracks: {album.get('total_tracks','?')}"),
                    thumbnail_url=thumb,
                    rating=round(random.uniform(3.0, 5.0), 1),
                    view_count=random.randint(1_000, 5_000_000),
                    duration_seconds=random.randint(180, 3_600),
                    publish_ts=_rand_past_date(),
                ))
            await asyncio.sleep(0.3)
        logger.info(f"[Spotify] {len(items)} items")
        return items

    # =========================================================================
    # 9. YouTube Data API v3 — Videos
    # =========================================================================
    async def fetch_youtube_videos(self, pages: int = 5) -> List[Item]:
        if not YOUTUBE_API_KEY:
            logger.warning("YOUTUBE_API_KEY not set — skipping YouTube videos.")
            return []
        items: List[Item] = []
        next_page_token: Optional[str] = None
        for _ in range(pages):
            params: dict = {
                "part": "snippet,statistics",
                "chart": "mostPopular",
                "regionCode": "US",
                "maxResults": 50,
                "key": YOUTUBE_API_KEY,
            }
            if next_page_token:
                params["pageToken"] = next_page_token
            data = await self._get("https://www.googleapis.com/youtube/v3/videos", params)
            if not data:
                break
            for v in data.get("items", []):
                snippet    = v.get("snippet", {})
                stats      = v.get("statistics", {})
                cat_id     = snippet.get("categoryId", "24")
                category   = YOUTUBE_CATEGORIES.get(str(cat_id), "Entertainment")
                thumbnails = snippet.get("thumbnails", {})
                thumb      = (thumbnails.get("high") or thumbnails.get("medium") or
                              thumbnails.get("default") or {}).get("url", "")
                tags       = snippet.get("tags", [category])[:10]
                items.append(Item(
                    item_id=f"yt_{v.get('id', uuid.uuid4().hex[:10])}",
                    title=snippet.get("title", "Unknown")[:200],
                    content_type=ContentType.VIDEO,
                    category=category,
                    tags=tags,
                    description=snippet.get("description", "")[:500],
                    thumbnail_url=thumb,
                    rating=round(random.uniform(3.0, 5.0), 1),
                    view_count=int(stats.get("viewCount", 0) or 0),
                    publish_ts=_rand_past_date(),
                ))
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            await asyncio.sleep(0.5)
        logger.info(f"[YouTube] {len(items)} items")
        return items

    # =========================================================================
    # 10. Open Food Facts — Products  (NO API KEY NEEDED)
    # =========================================================================
    async def fetch_open_food_facts(self, pages: int = 5) -> List[Item]:
        items: List[Item] = []
        food_categories = [
            "beverages", "snacks", "cereals", "dairy", "meats",
            "breads", "chocolates", "fruits", "vegetables", "frozen-foods",
        ]
        for category in food_categories[:pages]:
            data = await self._get("https://world.openfoodfacts.org/cgi/search.pl", {
                "action": "process",
                "tagtype_0": "categories", "tag_contains_0": "contains", "tag_0": category,
                "json": "1", "page_size": 50,
                "fields": "id,product_name,brands,image_url,nutriscore_grade",
            })
            for p in data.get("products", []):
                name = (p.get("product_name") or "").strip()
                if not name:
                    continue
                brand = (p.get("brands") or "").split(",")[0].strip()
                nutri = p.get("nutriscore_grade", "").upper()
                items.append(Item(
                    item_id=f"off_{p.get('id', uuid.uuid4().hex[:10])}",
                    title=f"{name}{' — ' + brand if brand else ''}",
                    content_type=ContentType.PRODUCT,
                    category=category.replace("-", " ").title(),
                    tags=[category, brand] if brand else [category, "Food"],
                    description=(f"Brand: {brand or 'N/A'}. "
                                 f"Category: {category}. "
                                 f"Nutriscore: {nutri or 'N/A'}"),
                    thumbnail_url=p.get("image_url", ""),
                    rating=round(random.uniform(3.0, 5.0), 1),
                    view_count=random.randint(100, 50_000),
                    publish_ts=_rand_past_date(),
                ))
            await asyncio.sleep(0.5)
        logger.info(f"[Open Food Facts] {len(items)} items")
        return items

    # =========================================================================
    # fetch_all — runs all 10 sources concurrently (flat list, backward compat)
    # =========================================================================
    async def fetch_all(self) -> List[Item]:
        """Run all 10 fetchers concurrently and return deduplicated items."""
        sources = await self.fetch_by_source()
        all_items: List[Item] = []
        for lst in sources.values():
            all_items.extend(lst)

        # Deduplicate by item_id
        seen: set = set()
        unique: List[Item] = []
        for item in all_items:
            if item.item_id not in seen:
                seen.add(item.item_id)
                unique.append(item)

        from collections import Counter
        breakdown = Counter(i.content_type.value for i in unique)
        logger.info(f"Total unique real items fetched: {len(unique)}")
        logger.info(f"Breakdown by type: {dict(breakdown)}")
        return unique

    # =========================================================================
    # fetch_by_source — returns items grouped by source for EntityResolver
    # =========================================================================
    async def fetch_by_source(self) -> Dict[str, List[Item]]:
        """
        Fetch from all 10 sources and return items grouped by source name.
        Used by EntityResolver so each resolver gets the correct source list.

        Returns a dict with keys:
            tmdb_movies, tmdb_tv, rawg, openlibrary,
            newsdata,    newsapi, lastfm, spotify,
            youtube,     food
        """
        logger.info("Fetching from all 10 real-data sources (grouped by source) …")
        results = await asyncio.gather(
            self.fetch_tmdb_movies(pages=10),
            self.fetch_tmdb_tv(pages=5),
            self.fetch_rawg_games(pages=10),
            self.fetch_open_library_books(),
            self.fetch_newsdata_articles(),
            self.fetch_newsapi_articles(pages=2),
            self.fetch_lastfm_tracks(pages=10),
            self.fetch_spotify_music(pages=10),
            self.fetch_youtube_videos(pages=5),
            self.fetch_open_food_facts(pages=5),
            return_exceptions=True,
        )
        keys = [
            "tmdb_movies", "tmdb_tv", "rawg", "openlibrary",
            "newsdata",    "newsapi", "lastfm", "spotify",
            "youtube",     "food",
        ]
        grouped: Dict[str, List[Item]] = {}
        for key, result in zip(keys, results):
            if isinstance(result, Exception):
                logger.warning(f"[{key}] fetcher raised: {result}")
                grouped[key] = []
            else:
                grouped[key] = result or []
            logger.info(f"  {key:15s}: {len(grouped[key]):,} items")
        return grouped

