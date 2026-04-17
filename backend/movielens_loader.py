"""
movielens_loader.py
===================
Downloads the MovieLens 1M dataset (1,000,209 real ratings from 6,040 users
on 3,900 movies) from the University of Minnesota GroupLens server, then maps
it into our User / Item / Interaction Pydantic models.

No API key or registration required — the dataset is freely available for
non-commercial use under the GroupLens license.

Usage::

    loader = MovieLensLoader()
    users, items, interactions = await loader.load(max_interactions=200_000)
"""

import asyncio
import io
import logging
import os
import random
import re
import zipfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import ContentType, Interaction, InteractionType, Item, User

logger = logging.getLogger(__name__)

ML_1M_URL   = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
# Cache the zip locally so repeated starts don't re-download
ML_CACHE_DIR = os.path.join(os.path.dirname(__file__), ".ml_cache")
ML_CACHE_ZIP = os.path.join(ML_CACHE_DIR, "ml-1m.zip")

# ML-1M genre string → our category name
GENRE_CATEGORY: Dict[str, str] = {
    "Action": "Action", "Adventure": "Adventure", "Animation": "Animation",
    "Children's": "Family", "Comedy": "Comedy", "Crime": "Crime",
    "Documentary": "Documentary", "Drama": "Drama", "Fantasy": "Fantasy",
    "Film-Noir": "Thriller", "Horror": "Horror", "Musical": "Music",
    "Mystery": "Mystery", "Romance": "Romance", "Sci-Fi": "Sci-Fi",
    "Thriller": "Thriller", "War": "Drama", "Western": "Western",
}

# ML-1M age code → our age_group label
AGE_GROUP: Dict[str, str] = {
    "1":  "18-24", "18": "18-24", "25": "25-34",
    "35": "35-44", "45": "45-54", "50": "55-64", "56": "65+",
}

COUNTRIES  = ["US", "UK", "CA", "AU", "DE", "FR", "IN", "JP", "BR", "MX"]
DEVICES    = ["web", "mobile", "tablet", "smart_tv"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rating_to_interaction(rating: float) -> InteractionType:
    """Map a numeric 1-5 rating to a meaningful interaction type."""
    if rating >= 4.0:
        return InteractionType.LIKE
    if rating <= 2.0:
        return InteractionType.DISLIKE
    return InteractionType.VIEW


def _dwell_from_type(itype: InteractionType) -> int:
    """Return a realistic dwell time (seconds) for the given interaction."""
    if itype == InteractionType.LIKE:
        return random.randint(300, 7_200)   # 5 min – 2 hr
    if itype == InteractionType.VIEW:
        return random.randint(60, 600)      # 1 – 10 min
    return random.randint(5, 120)           # quick skim / instant dislike


# ── Main class ────────────────────────────────────────────────────────────────

class MovieLensLoader:
    """
    Downloads the MovieLens 1M zip on first run and caches it locally.
    Subsequent boots load from cache — no network traffic.
    """

    # ── Download ──────────────────────────────────────────────────────────────
    async def _download(self) -> bytes:
        os.makedirs(ML_CACHE_DIR, exist_ok=True)

        if os.path.exists(ML_CACHE_ZIP):
            logger.info(f"[MovieLens] Using cached zip at {ML_CACHE_ZIP}")
            with open(ML_CACHE_ZIP, "rb") as fh:
                return fh.read()

        logger.info(f"[MovieLens] Downloading from {ML_1M_URL} …")
        async with httpx.AsyncClient(timeout=180.0, follow_redirects=True) as client:
            resp = await client.get(ML_1M_URL)
            resp.raise_for_status()
            data = resp.content

        with open(ML_CACHE_ZIP, "wb") as fh:
            fh.write(data)
        logger.info(f"[MovieLens] Downloaded {len(data) / 1_048_576:.1f} MB and cached.")
        return data

    # ── Parse zip (CPU-bound — run in executor) ───────────────────────────────
    def _parse_zip(self, raw: bytes) -> Tuple[
        Dict[str, dict],   # movie_id → {title, year, genres, category}
        List[tuple],       # (user_id, movie_id, rating_float, unix_ts)
        Dict[str, dict],   # user_id  → {age_group, gender}
    ]:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            movies_txt  = zf.read("ml-1m/movies.dat").decode("latin-1")
            ratings_txt = zf.read("ml-1m/ratings.dat").decode("latin-1")
            users_txt   = zf.read("ml-1m/users.dat").decode("latin-1")

        # ── movies.dat → MovieID::Title::Genres ──────────────────────────────
        movies: Dict[str, dict] = {}
        for line in movies_txt.strip().splitlines():
            parts = line.strip().split("::")
            if len(parts) < 3:
                continue
            mid, title_raw, genres_str = parts[0], parts[1], parts[2]
            genres    = genres_str.split("|")
            year_m    = re.search(r"\((\d{4})\)\s*$", title_raw)
            year      = year_m.group(1) if year_m else "2000"
            clean     = re.sub(r"\s*\(\d{4}\)\s*$", "", title_raw).strip()
            category  = GENRE_CATEGORY.get(genres[0], "Drama")
            movies[mid] = {"title": clean, "year": year,
                           "genres": genres, "category": category}

        # ── ratings.dat → UserID::MovieID::Rating::Timestamp ────────────────
        ratings: List[tuple] = []
        for line in ratings_txt.strip().splitlines():
            parts = line.strip().split("::")
            if len(parts) < 4:
                continue
            ratings.append((parts[0], parts[1], float(parts[2]), int(parts[3])))

        # ── users.dat → UserID::Gender::Age::Occupation::Zip ────────────────
        users: Dict[str, dict] = {}
        for line in users_txt.strip().splitlines():
            parts = line.strip().split("::")
            if len(parts) < 4:
                continue
            users[parts[0]] = {
                "gender":    parts[1],
                "age_group": AGE_GROUP.get(parts[2], "25-34"),
            }

        return movies, ratings, users

    # ── Schema mappers ────────────────────────────────────────────────────────
    def _build_users(self, raw: Dict[str, dict]) -> List[User]:
        content_types = [ct.value for ct in ContentType]
        result = []
        for uid, info in raw.items():
            result.append(User(
                user_id=f"ml_user_{uid}",
                age_group=info["age_group"],
                country=random.choice(COUNTRIES),
                device=random.choice(DEVICES),
                preferences=random.sample(content_types, k=random.randint(1, 4)),
            ))
        return result

    def _build_items(self, raw: Dict[str, dict]) -> List[Item]:
        result = []
        for mid, info in raw.items():
            try:
                year = int(info["year"])
                pub_ts = datetime(year, 6, 1, tzinfo=timezone.utc)
            except ValueError:
                pub_ts = datetime(2000, 6, 1, tzinfo=timezone.utc)

            result.append(Item(
                item_id=f"ml_movie_{mid}",
                title=info["title"],
                content_type=ContentType.MOVIE,
                category=info["category"],
                tags=info["genres"],
                description=(f"Year: {info['year']}. "
                             f"Genres: {', '.join(info['genres'])}."),
                thumbnail_url="",    # MovieLens has no posters
                rating=round(random.uniform(2.5, 5.0), 1),
                view_count=random.randint(100, 100_000),
                publish_ts=pub_ts,
            ))
        return result

    def _build_interactions(
        self,
        ratings: List[tuple],
        movies:  Dict[str, dict],
        max_count: int,
    ) -> List[Interaction]:
        sample = random.sample(ratings, min(max_count, len(ratings)))
        result = []
        for uid, mid, score, ts_unix in sample:
            if mid not in movies:
                continue
            itype = _rating_to_interaction(score)
            result.append(Interaction(
                user_id=f"ml_user_{uid}",
                item_id=f"ml_movie_{mid}",
                interaction_type=itype,
                timestamp=datetime.fromtimestamp(ts_unix, tz=timezone.utc),
                dwell_seconds=_dwell_from_type(itype),
                rating=score,
                context={"source": "movielens_1m"},
            ))
        return result

    # ── Public entry point ────────────────────────────────────────────────────
    async def load(
        self,
        max_interactions: int = 200_000,
    ) -> Tuple[List[User], List[Item], List[Interaction]]:
        """
        Download (or load from cache), parse, and return:
          (users, items, interactions)

        max_interactions caps how many of the 1M ratings are imported so the
        initial seed stays fast (200K ≈ 15–30 seconds).  Set to 1_000_000
        for the full dataset.
        """
        raw_bytes = await self._download()

        logger.info("[MovieLens] Parsing dataset (running in executor)…")
        loop   = asyncio.get_running_loop()
        movies, ratings, users_raw = await loop.run_in_executor(
            None, self._parse_zip, raw_bytes
        )
        logger.info(
            f"[MovieLens] Parsed: {len(movies)} movies, "
            f"{len(ratings)} ratings, {len(users_raw)} users"
        )

        users        = self._build_users(users_raw)
        items        = self._build_items(movies)
        interactions = self._build_interactions(ratings, movies, max_interactions)

        logger.info(
            f"[MovieLens] Ready → "
            f"{len(users)} users, {len(items)} items, {len(interactions)} interactions"
        )
        return users, items, interactions
