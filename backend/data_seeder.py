"""
data_seeder.py
==============
Real-data seeding pipeline.  All fake / Faker-generated data has been removed.

The ONLY entry point is:

    seeder = DataSeeder()
    await seeder.seed_real_data()

This runs a 4-step pipeline:
    STEP 1  Fetch rich metadata from 10 real APIs in parallel
    STEP 2  Download & parse free public datasets (auto-cached)
    STEP 3  Match & merge across sources via EntityResolver
    STEP 4  Insert unified catalogue + remapped interactions into MongoDB
"""

import asyncio
import logging
import os
from collections import Counter
from typing import List

from database import get_db_manager
from models import ContentType

logger = logging.getLogger(__name__)


class DataSeeder:
    """Orchestrates real-data ingestion from 13 free public sources."""

    # =========================================================================
    # Main entry point
    # =========================================================================

    async def seed_real_data(self, max_ml_interactions: int = 200_000) -> None:
        """
        4-step intelligent seeding pipeline for ALL content types.

        Sources used
        ────────────
        APIs (parallel):
            TMDB movies/TV, RAWG games, Spotify music, Last.fm tracks,
            YouTube videos, Open Library books, NewsData articles,
            NewsAPI articles, Open Food Facts products

        Datasets (auto-downloaded, cached in .dataset_cache/):
            MovieLens 1M or 25M   — MOVIE interactions + links.csv TMDB map
            Book-Crossings         — ARTICLE (books) + ratings
            Steam                  — GAME items
            Amazon Electronics     — PRODUCT items + ratings

        Merging (EntityResolver):
            MOVIE   links.csv exact TMDB-ID map / title+year fuzzy
            BOOK    title fuzzy (Open Library ↔ Book-Crossings)
            GAME    title fuzzy (RAWG ↔ Steam)
            MUSIC   artist fuzzy (Spotify ↔ Last.fm)
            ARTICLE title dedup (NewsData ↔ NewsAPI)
            VIDEO   title dedup (TMDB TV ↔ YouTube)
        """
        try:
            db = await get_db_manager()
            existing_users = await db.db.users.count_documents({})
            existing_items = await db.db.items.count_documents({})
            if existing_users > 0 and existing_items > 0:
                logger.info(
                    f"Already seeded ({existing_users:,} users, "
                    f"{existing_items:,} items) — skipping."
                )
                return

            load_extra = os.getenv("LOAD_EXTRA_DATASETS", "true").lower() == "true"
            ml_size    = os.getenv("ML_DATASET_SIZE", "1m").lower()

            # ── STEP 1: Fetch rich API items grouped by source ────────────────
            logger.info("=" * 65)
            logger.info("STEP 1 — Fetching rich metadata from 10 APIs in parallel")
            logger.info("=" * 65)
            from real_data_fetcher import RealDataFetcher
            async with RealDataFetcher() as fetcher:
                sources = await fetcher.fetch_by_source()

            # ── STEP 2: Load datasets (auto-downloaded, cached) ───────────────
            logger.info("=" * 65)
            logger.info("STEP 2 — Loading free datasets (auto-download + cache)")
            logger.info("=" * 65)
            from dataset_loader import (
                BookCrossingsLoader, SteamLoader,
                AmazonProductLoader, MovieLens25MLoader,
            )
            from movielens_loader import MovieLensLoader

            # MovieLens: 1M (fast) or 25M (rich, exact TMDB IDs via links.csv)
            ml_links: dict = {}
            if ml_size == "25m":
                logger.info("Loading MovieLens 25M (links.csv → exact TMDB mapping)…")
                ml_users, ml_items, ml_interactions, ml_links = \
                    await MovieLens25MLoader().load(max_interactions=max_ml_interactions)
                logger.info(f"ML-25M links loaded: {len(ml_links):,} TMDB ID mappings")
            else:
                logger.info("Loading MovieLens 1M (title+year fuzzy matching)…")
                ml_users, ml_items, ml_interactions = \
                    await MovieLensLoader().load(max_interactions=max_ml_interactions)

            bx_items, bx_users, bx_interactions = [], [], []
            steam_items: list = []
            amz_items, amz_interactions = [], []

            if load_extra:
                logger.info("Loading Book-Crossings (books + ratings)…")
                bx_items, bx_users, bx_interactions = await BookCrossingsLoader().load(
                    max_items=5_000, max_interactions=100_000,
                )
                logger.info(f"  Book-Crossings: {len(bx_items):,} books, "
                            f"{len(bx_interactions):,} interactions")

                logger.info("Loading Steam games metadata…")
                steam_items = await SteamLoader().load(max_items=2_000)
                logger.info(f"  Steam: {len(steam_items):,} games")

                logger.info("Loading Amazon Electronics metadata + ratings…")
                amz_items, amz_interactions = await AmazonProductLoader().load(
                    max_items=2_000, max_interactions=50_000,
                )
                logger.info(f"  Amazon: {len(amz_items):,} products, "
                            f"{len(amz_interactions):,} interactions")

            # ── STEP 3: MATCH & MERGE via EntityResolver ──────────────────────
            logger.info("=" * 65)
            logger.info("STEP 3 — Entity Resolution: match + merge across all sources")
            logger.info("=" * 65)
            from entity_resolver import EntityResolver
            resolver = EntityResolver()

            # 3a. MOVIES: TMDB (rich) + MovieLens (ratings) → unified
            logger.info("  [3a] Movies: TMDB ↔ MovieLens …")
            unified_movies, remapped_ml_ints = resolver.resolve_movies(
                tmdb_items=sources["tmdb_movies"],
                ml_items=ml_items,
                ml_interactions=ml_interactions,
                ml_links=ml_links or None,
            )

            # 3b. BOOKS: Open Library (rich) + Book-Crossings (ratings) → unified
            logger.info("  [3b] Books: Open Library ↔ Book-Crossings …")
            unified_books, remapped_bx_ints = resolver.resolve_books(
                openlibrary_items=sources["openlibrary"],
                bx_items=bx_items,
                bx_interactions=bx_interactions,
            )

            # 3c. GAMES: RAWG (rich) + Steam (additional) → unified
            logger.info("  [3c] Games: RAWG ↔ Steam …")
            unified_games = resolver.resolve_games(
                rawg_items=sources["rawg"],
                steam_items=steam_items,
            )

            # 3d. MUSIC: Spotify (rich) + Last.fm (listener counts) → unified
            logger.info("  [3d] Music: Spotify ↔ Last.fm …")
            unified_music = resolver.resolve_music(
                spotify_items=sources["spotify"],
                lastfm_items=sources["lastfm"],
            )

            # 3e. ARTICLES: NewsData + NewsAPI → deduplicated
            logger.info("  [3e] Articles: NewsData ↔ NewsAPI deduplication …")
            unified_articles = resolver.deduplicate_articles(
                sources["newsdata"],
                sources["newsapi"],
            )

            # 3f. VIDEOS: TMDB TV + YouTube → deduplicated
            logger.info("  [3f] Videos: TMDB TV ↔ YouTube deduplication …")
            unified_videos = resolver.deduplicate_videos(
                tmdb_tv_items=sources["tmdb_tv"],
                youtube_items=sources["youtube"],
            )

            # 3g. PRODUCTS: Open Food Facts + Amazon Electronics
            unified_products = sources["food"] + amz_items

            resolver.log_summary()

            # ── Per-type item counts (info only, no synthetic fallback) ───────
            all_resolved = (
                unified_movies + unified_books + unified_games
                + unified_music + unified_articles + unified_videos
                + unified_products
            )
            type_counts = Counter(item.content_type.value for item in all_resolved)
            logger.info("Resolved item counts by content type:")
            for ct in ContentType:
                n = type_counts.get(ct.value, 0)
                status = "✅" if n >= 10 else "⚠️  LOW"
                logger.info(f"  {ct.value:12s}: {n:,}  {status}")

            # ── STEP 4: INSERT into MongoDB ───────────────────────────────────
            logger.info("=" * 65)
            logger.info("STEP 4 — Inserting unified catalogue into MongoDB")
            logger.info("=" * 65)

            # Items
            if all_resolved:
                await db.db.items.insert_many(
                    [i.model_dump() for i in all_resolved], ordered=False
                )
            logger.info(f"Items inserted: {len(all_resolved):,}")

            # Users (from MovieLens + Book-Crossings — real anonymised IDs)
            all_users = ml_users + bx_users
            if all_users:
                await db.db.users.insert_many(
                    [u.model_dump() for u in all_users], ordered=False
                )
            logger.info(f"Users inserted: {len(all_users):,}")

            # Interactions (remapped to winning item IDs)
            all_interactions = remapped_ml_ints + remapped_bx_ints + amz_interactions
            await db.bulk_insert_interactions(all_interactions)

            # ── Final summary ─────────────────────────────────────────────────
            logger.info("=" * 65)
            logger.info("REAL DATA SEEDING — COMPLETE!")
            final_users = await db.db.users.count_documents({})
            final_items = await db.db.items.count_documents({})
            final_ints  = await db.db.interactions.count_documents({})
            logger.info(f"  Items        : {final_items:,}")
            logger.info(f"  Users        : {final_users:,}")
            logger.info(f"  Interactions : {final_ints:,}")
            logger.info("=" * 65)
            logger.info("XGBoost trains on TMDB-quality items + ML-scale interactions")

        except Exception as exc:
            logger.error(f"Error during real data seeding: {exc}")
            raise
