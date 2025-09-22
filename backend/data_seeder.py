import asyncio
import logging
from faker import Faker
import random
from datetime import datetime, timedelta
from models import User, Item, Interaction, ContentType, InteractionType
from database import get_db_manager

logger = logging.getLogger(__name__)
fake = Faker()

class DataSeeder:
    def __init__(self):
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
                signup_ts=fake.date_time_between(start_date='-2y', end_date='now')
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
            
            # Generate relevant tags based on content type and category
            tag_pool = [
                category.lower(), content_type.value, 
                fake.word(), fake.word(), fake.color_name(),
                random.choice(['trending', 'popular', 'new', 'featured', 'recommended'])
            ]
            tags = random.sample(tag_pool, k=random.randint(2, 5))
            
            # Generate titles based on content type
            title = self.generate_title_by_type(content_type, category)
            description = fake.text(max_nb_chars=200)
            
            item = Item(
                title=title,
                content_type=content_type,
                category=category,
                tags=tags,
                description=description,
                thumbnail_url=random.choice(self.sample_thumbnails),
                publish_ts=fake.date_time_between(start_date='-1y', end_date='now'),
                duration_seconds=random.randint(30, 3600) if content_type in [ContentType.VIDEO, ContentType.MUSIC, ContentType.PODCAST] else None,
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
        
        # Create user preferences based on their profile
        user_preferences = {}
        for user in users:
            # Users prefer content types from their preferences
            preferred_types = user.preferences if user.preferences else [random.choice(list(ContentType)).value]
            user_preferences[user.user_id] = preferred_types
        
        for _ in range(count):
            user = random.choice(users)
            
            # Bias item selection towards user preferences
            if random.random() < 0.7:  # 70% chance to pick preferred content
                preferred_items = [item for item in items if item.content_type.value in user_preferences[user.user_id]]
                if preferred_items:
                    item = random.choice(preferred_items)
                else:
                    item = random.choice(items)
            else:
                item = random.choice(items)
            
            # Generate realistic interaction patterns
            interaction_type = self.choose_interaction_type(item.content_type)
            dwell_seconds = self.generate_dwell_time(interaction_type, item.content_type)
            rating = self.generate_rating(interaction_type) if random.random() < 0.3 else None
            
            # Generate realistic context
            context = {
                'source': random.choice(['home_page', 'search', 'recommendation', 'category_browse']),
                'device': user.device,
                'session_id': fake.uuid4()
            }
            
            interaction = Interaction(
                user_id=user.user_id,
                item_id=item.item_id,
                interaction_type=interaction_type,
                timestamp=fake.date_time_between(start_date='-6m', end_date='now'),
                dwell_seconds=dwell_seconds,
                rating=rating,
                context=context
            )
            interactions.append(interaction)
        
        # Sort by timestamp for realistic patterns
        interactions.sort(key=lambda x: x.timestamp)
        
        logger.info(f"Generated {len(interactions)} interactions")
        return interactions
    
    def choose_interaction_type(self, content_type: ContentType) -> InteractionType:
        """Choose realistic interaction type based on content"""
        if content_type == ContentType.PRODUCT:
            return random.choices(
                [InteractionType.VIEW, InteractionType.LIKE, InteractionType.PURCHASE, InteractionType.BOOKMARK],
                weights=[50, 20, 10, 20]
            )[0]
        elif content_type in [ContentType.VIDEO, ContentType.MOVIE]:
            return random.choices(
                [InteractionType.VIEW, InteractionType.LIKE, InteractionType.SHARE, InteractionType.BOOKMARK],
                weights=[60, 25, 10, 5]
            )[0]
        else:
            return random.choices(
                [InteractionType.VIEW, InteractionType.LIKE, InteractionType.SHARE, InteractionType.CLICK],
                weights=[50, 30, 10, 10]
            )[0]
    
    def generate_dwell_time(self, interaction_type: InteractionType, content_type: ContentType) -> int:
        """Generate realistic dwell time based on interaction and content type"""
        if interaction_type == InteractionType.VIEW:
            if content_type in [ContentType.VIDEO, ContentType.MOVIE]:
                return random.randint(30, 1800)  # 30 seconds to 30 minutes
            elif content_type in [ContentType.ARTICLE, ContentType.COURSE]:
                return random.randint(60, 600)   # 1 to 10 minutes
            else:
                return random.randint(10, 300)   # 10 seconds to 5 minutes
        elif interaction_type in [InteractionType.LIKE, InteractionType.SHARE]:
            return random.randint(30, 120)       # 30 seconds to 2 minutes
        else:
            return random.randint(5, 60)         # 5 seconds to 1 minute
    
    def generate_rating(self, interaction_type: InteractionType) -> float:
        """Generate realistic ratings based on interaction type"""
        if interaction_type == InteractionType.LIKE:
            return round(random.uniform(3.5, 5.0), 1)  # Likes tend to be higher ratings
        elif interaction_type == InteractionType.DISLIKE:
            return round(random.uniform(1.0, 2.5), 1)  # Dislikes tend to be lower ratings
        else:
            return round(random.uniform(2.0, 5.0), 1)  # General distribution
    
    async def seed_all_data(self, users_count: int = 500, items_count: int = 2000, interactions_count: int = 20000):
        """Seed all data in the database"""
        try:
            db = await get_db_manager()
            
            # Check if data already exists
            existing_users = await db.db.users.count_documents({})
            existing_items = await db.db.items.count_documents({})
            
            if existing_users > 0 or existing_items > 0:
                logger.info(f"Data already exists: {existing_users} users, {existing_items} items")
                return
            
            logger.info("Starting data seeding process...")
            
            # Generate and insert users
            users = await self.generate_users(users_count)
            for user in users:
                await db.insert_user(user)
            logger.info(f"Inserted {len(users)} users")
            
            # Generate and insert items
            items = await self.generate_items(items_count)
            for item in items:
                await db.insert_item(item)
            logger.info(f"Inserted {len(items)} items")
            
            # Generate and insert interactions
            interactions = await self.generate_interactions(users, items, interactions_count)
            for interaction in interactions:
                await db.log_interaction(interaction)
            logger.info(f"Inserted {len(interactions)} interactions")
            
            logger.info("Data seeding completed successfully!")
            
        except Exception as e:
            logger.error(f"Error seeding data: {e}")
            raise