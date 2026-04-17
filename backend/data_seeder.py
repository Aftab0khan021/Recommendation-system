import asyncio
import logging
import os
from faker import Faker
import random
from typing import List
from datetime import datetime, timedelta, timezone
from models import User, Item, Interaction, ContentType, InteractionType
from database import get_db_manager

logger = logging.getLogger(__name__)

fake = Faker()

class DataSeeder:
    def __init__(self):
        # MED-5 fix: seed inside __init__, not at module level, so import doesn't
        # affect global random state in production code paths.
        Faker.seed(42)
        random.seed(42)

        self.categories_by_type = {
            ContentType.VIDEO: ['Entertainment', 'Education', 'Sports', 'Music', 'News', 'Comedy', 'Tutorial'],
            ContentType.MOVIE: ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Thriller'],
            ContentType.ARTICLE: ['Technology', 'Science', 'Health', 'Business', 'Politics', 'Lifestyle', 'Travel'],
            ContentType.PRODUCT: ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty', 'Food'],
            ContentType.MUSIC: ['Pop', 'Rock', 'Hip-Hop', 'Classical', 'Jazz', 'Electronic', 'Country'],
            ContentType.PODCAST: ['Technology', 'Business', 'Health', 'Comedy', 'News', 'Education', 'True Crime'],
            ContentType.COURSE: ['Programming', 'Business', 'Design', 'Language', 'Science', 'Art', 'Health'],
            ContentType.GAME: ['Action', 'Strategy', 'RPG', 'Puzzle', 'Sports', 'Adventure', 'Simulation']
        }

        self.sample_thumbnails = [
            "https://images.unsplash.com/photo-1611532736597-de2d4265fba3?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1518709268805-4e9042af2176?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1560472354-b33ff0c44a43?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1526628953301-3e589a6a8b74?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1551434678-e076c223a692?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1573164713714-d95e436ab8d6?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1555066931-4365d14bab8c?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1542831371-29b0f74f9713?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=400&h=300&fit=crop",
            "https://images.unsplash.com/photo-1517077304055-6e89abbf09b0?w=400&h=300&fit=crop"
        ]

    async def generate_users(self, count: int = 500) -> List[User]:
        """Generate fake users"""
        users = []
        countries = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'IN', 'JP', 'BR', 'MX']
        devices = ['web', 'mobile', 'tablet', 'smart_tv']
        age_groups = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']

        for _ in range(count):
            preferences = random.sample([ct.value for ct in ContentType], k=random.randint(1, 4))

            user = User(
                country=random.choice(countries),
                device=random.choice(devices),
                age_group=random.choice(age_groups),
                preferences=preferences,
                # BUG-3 fix: ensure timezone-aware datetime
                signup_ts=fake.date_time_between(start_date='-2y', end_date='now').replace(tzinfo=timezone.utc)
            )
            users.append(user)

        logger.info(f"Generated {len(users)} users")
        return users

    async def generate_items(self, count: int = 2000) -> List[Item]:
        """Generate fake items"""
        items = []

        for _ in range(count):
            content_type = random.choice(list(ContentType))
            category = random.choice(self.categories_by_type[content_type])

            tag_pool = [
                category.lower(), content_type.value,
                fake.word(), fake.word(), fake.color_name(),
                random.choice(['trending', 'popular', 'new', 'featured', 'recommended'])
            ]
            tags = random.sample(tag_pool, k=random.randint(2, 5))

            title = self.generate_title_by_type(content_type, category)
            description = fake.text(max_nb_chars=200)

            item = Item(
                title=title,
                content_type=content_type,
                category=category,
                tags=tags,
                description=description,
                thumbnail_url=random.choice(self.sample_thumbnails),
                # BUG-3 fix: ensure timezone-aware datetime
                publish_ts=fake.date_time_between(start_date='-1y', end_date='now').replace(tzinfo=timezone.utc),
                duration_seconds=random.randint(30, 3600) if content_type in [
                    ContentType.VIDEO, ContentType.MUSIC, ContentType.PODCAST
                ] else None,
                rating=round(random.uniform(1.0, 5.0), 1),
                view_count=random.randint(0, 100000)
            )
            items.append(item)

        logger.info(f"Generated {len(items)} items")
        return items

    def generate_title_by_type(self, content_type: ContentType, category: str) -> str:
        """Generate realistic titles based on content type"""
        if content_type == ContentType.VIDEO:
            templates = [
                f"How to {fake.sentence(nb_words=3).rstrip('.')}",
                f"Top 10 {category} {fake.word()}s",
                f"{fake.sentence(nb_words=4).rstrip('.')} - {category}",
                f"Amazing {category} Compilation"
            ]
        elif content_type == ContentType.MOVIE:
            templates = [
                f"The {fake.word().title()} {category}",
                f"{fake.first_name()}'s {category} Adventure",
                f"{fake.word().title()} {fake.word().title()}",
                f"Return of the {fake.word().title()}"
            ]
        elif content_type == ContentType.ARTICLE:
            templates = [
                f"The Future of {category}",
                f"Understanding {category} in 2024",
                f"5 Ways {category} is Changing",
                f"Why {category} Matters Now"
            ]
        elif content_type == ContentType.PRODUCT:
            templates = [
                f"Premium {category} {fake.word().title()}",
                f"Best {category} for {fake.word()}",
                f"Professional {category} Set",
                f"Luxury {fake.word().title()} {category}"
            ]
        elif content_type == ContentType.MUSIC:
            templates = [
                f"{fake.word().title()} {category} Mix",
                f"Best of {category} 2024",
                f"{fake.first_name()}'s {category} Hits",
                f"Chill {category} Vibes"
            ]
        elif content_type == ContentType.PODCAST:
            templates = [
                f"The {category} Show",
                f"Deep Dive: {category}",
                f"{category} Weekly",
                f"Conversations about {category}"
            ]
        elif content_type == ContentType.COURSE:
            templates = [
                f"Complete {category} Course",
                f"Learn {category} in 30 Days",
                f"Advanced {category} Masterclass",
                f"{category} for Beginners"
            ]
        elif content_type == ContentType.GAME:
            templates = [
                f"{fake.word().title()} {category}",
                f"Ultimate {category} Challenge",
                f"{category} Adventure",
                f"Epic {category} Quest"
            ]
        else:
            templates = [f"{fake.sentence(nb_words=4).rstrip('.')}"]

        return random.choice(templates)

    async def generate_interactions(self, users: List[User], items: List[Item], count: int = 20000) -> List[Interaction]:
        """Generate realistic user interactions"""
        interactions = []

        user_preferences = {}
        for user in users:
            preferred_types = user.preferences if user.preferences else [random.choice(list(ContentType)).value]
            user_preferences[user.user_id] = preferred_types

        for _ in range(count):
            user = random.choice(users)

            if random.random() < 0.7:
                preferred_items = [item for item in items if item.content_type.value in user_preferences[user.user_id]]
                item = random.choice(preferred_items) if preferred_items else random.choice(items)
            else:
                item = random.choice(items)

            interaction_type = self.choose_interaction_type(item.content_type)
            dwell_seconds = self.generate_dwell_time(interaction_type, item.content_type)
            rating = self.generate_rating(interaction_type) if random.random() < 0.3 else None

            context = {
                'source': random.choice(['home_page', 'search', 'recommendation', 'category_browse']),
                'device': user.device,
                'session_id': fake.uuid4()
            }

            interaction = Interaction(
                user_id=user.user_id,
                item_id=item.item_id,
                interaction_type=interaction_type,
                # BUG-3 fix: ensure timezone-aware datetime
                timestamp=fake.date_time_between(start_date='-6m', end_date='now').replace(tzinfo=timezone.utc),
                dwell_seconds=dwell_seconds,
                rating=rating,
                context=context
            )
            interactions.append(interaction)

        interactions.sort(key=lambda x: x.timestamp)

        logger.info(f"Generated {len(interactions)} interactions")
        return interactions

    def choose_interaction_type(self, content_type: ContentType) -> InteractionType:
        """Choose realistic interaction type based on content"""
        # BUG-16 fix: include DISLIKE so the dead code path in generate_rating() is reachable
        if content_type == ContentType.PRODUCT:
            return random.choices(
                [InteractionType.VIEW, InteractionType.LIKE, InteractionType.PURCHASE,
                 InteractionType.BOOKMARK, InteractionType.DISLIKE],
                weights=[48, 18, 10, 18, 6]
            )[0]
        elif content_type in [ContentType.VIDEO, ContentType.MOVIE]:
            return random.choices(
                [InteractionType.VIEW, InteractionType.LIKE, InteractionType.SHARE,
                 InteractionType.BOOKMARK, InteractionType.DISLIKE],
                weights=[55, 23, 10, 5, 7]
            )[0]
        else:
            return random.choices(
                [InteractionType.VIEW, InteractionType.LIKE, InteractionType.SHARE,
                 InteractionType.CLICK, InteractionType.DISLIKE],
                weights=[47, 28, 10, 8, 7]
            )[0]

    def generate_dwell_time(self, interaction_type: InteractionType, content_type: ContentType) -> int:
        """Generate realistic dwell time based on interaction and content type"""
        if interaction_type == InteractionType.VIEW:
            if content_type in [ContentType.VIDEO, ContentType.MOVIE]:
                return random.randint(30, 1800)
            elif content_type in [ContentType.ARTICLE, ContentType.COURSE]:
                return random.randint(60, 600)
            else:
                return random.randint(10, 300)
        elif interaction_type in [InteractionType.LIKE, InteractionType.SHARE]:
            return random.randint(30, 120)
        else:
            return random.randint(5, 60)

    def generate_rating(self, interaction_type: InteractionType) -> float:
        """Generate realistic ratings based on interaction type"""
        if interaction_type == InteractionType.LIKE:
            return round(random.uniform(3.5, 5.0), 1)
        elif interaction_type == InteractionType.DISLIKE:
            return round(random.uniform(1.0, 2.5), 1)
        else:
            return round(random.uniform(2.0, 5.0), 1)

    async def seed_all_data(self, users_count: int = 500, items_count: int = 2000, interactions_count: int = 20000):
        """Seed all data in the database using synthetic (Faker) data."""
        try:
            db = await get_db_manager()

            existing_users = await db.db.users.count_documents({})
            existing_items = await db.db.items.count_documents({})

            if existing_users > 0 or existing_items > 0:
                logger.info(f"Data already exists: {existing_users} users, {existing_items} items")
                return

            logger.info("Starting synthetic data seeding process...")

            # Generate and bulk-insert users
            users = await self.generate_users(users_count)
            user_docs = [u.model_dump() for u in users]
            if user_docs:
                await db.db.users.insert_many(user_docs, ordered=False)
            logger.info(f"Bulk-inserted {len(users)} users")

            # Generate and bulk-insert items
            items = await self.generate_items(items_count)
            item_docs = [i.model_dump() for i in items]
            if item_docs:
                await db.db.items.insert_many(item_docs, ordered=False)
            logger.info(f"Bulk-inserted {len(items)} items")

            # Generate and bulk-insert interactions (Bug #15 fix: single insert_many)
            interactions = await self.generate_interactions(users, items, interactions_count)
            await db.bulk_insert_interactions(interactions)

            logger.info("Synthetic data seeding completed successfully!")

        except Exception as e:
            logger.error(f"Error seeding data: {e}")
            raise

    async def seed_real_data(self, max_ml_interactions: int = 200_000):
        """
        4-step intelligent seeding pipeline for ALL content types:

        STEP 1  Fetch "rich" items from APIs  (real poster, description, rating)
        STEP 2  Load "ratings" datasets       (real human interaction history)
        STEP 3  MATCH & MERGE via EntityResolver:
                  Movies  : links.csv  exact TMDB-ID map  (ML-25M)
                            title+year fuzzy match         (ML-1M / fallback)
                  Books   : title fuzzy match  (Open Library ↔ Book-Crossings)
                  Games   : title fuzzy match  (RAWG ↔ Steam)
                  Music   : artist fuzzy match (Spotify ↔ Last.fm)
                  Articles: title dedup        (NewsData ↔ NewsAPI)
                  Videos  : title dedup        (TMDB TV ↔ YouTube)
        STEP 4  Insert unified items + remapped interactions → XGBoost trains
                on API-quality metadata + dataset-scale real interactions
        """
        try:
            db = await get_db_manager()
            existing_users = await db.db.users.count_documents({})
            existing_items = await db.db.items.count_documents({})
            if existing_users > 0 and existing_items > 0:
                logger.info(
                    f"Already seeded ({existing_users} users, "
                    f"{existing_items} items) — skipping."
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

            # MovieLens: 1M (fast) or 25M (rich, exact TMDB IDs)
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
                logger.info("Loading Steam games metadata…")
                steam_items = await SteamLoader().load(max_items=2_000)
                logger.info("Loading Amazon Electronics metadata + ratings…")
                amz_items, amz_interactions = await AmazonProductLoader().load(
                    max_items=2_000, max_interactions=50_000,
                )

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

            # 3g. PRODUCTS: Open Food Facts + Amazon Electronics (no dedup needed)
            unified_products = sources["food"] + amz_items

            resolver.log_summary()

            # ── Faker fallback for any content type still < 50 items ──────────
            from collections import Counter
            from models import ContentType as CT
            all_resolved = (
                unified_movies + unified_books + unified_games
                + unified_music + unified_articles + unified_videos
                + unified_products
            )
            type_counts = Counter(item.content_type.value for item in all_resolved)
            logger.info(f"Resolved item counts by type: {dict(type_counts)}")

            fallback_items = []
            for ct in CT:
                if type_counts.get(ct.value, 0) < 50:
                    needed = 50 - type_counts.get(ct.value, 0)
                    logger.info(f"Generating {needed} synthetic fallback: {ct.value}")
                    batch = await self.generate_items(needed)
                    for item in batch:
                        item.content_type = ct
                        item.item_id = f"fake_{ct.value}_{item.item_id}"
                    fallback_items.extend(batch)

            # ── STEP 4: INSERT into MongoDB ───────────────────────────────────
            logger.info("=" * 65)
            logger.info("STEP 4 — Inserting unified catalogue into MongoDB")
            logger.info("=" * 65)

            # Insert all unified items
            all_items = all_resolved + fallback_items
            if all_items:
                await db.db.items.insert_many(
                    [i.model_dump() for i in all_items], ordered=False
                )
            logger.info(f"Unified items inserted: {len(all_items):,}")

            # Insert unmatched MovieLens movie items (those with no TMDB match)
            # They already appear in unified_movies (resolve_movies keeps them)

            # Insert users
            all_users = ml_users + bx_users
            if all_users:
                await db.db.users.insert_many(
                    [u.model_dump() for u in all_users], ordered=False
                )
            logger.info(f"Users inserted: {len(all_users):,}")

            # Insert ALL remapped interactions
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
