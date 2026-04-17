import os
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from models import User, Item, Interaction, ContentType, InteractionType
import json
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)

# Lock to prevent concurrent singleton initialisation (Bug #23 fix)
_init_lock = asyncio.Lock()


class DatabaseManager:
    def __init__(self):
        self.mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
        self.db_name = os.getenv("DB_NAME", "recommendation_system")
        self.client = None
        self.db = None

    async def initialize(self):
        """Initialize database connections"""
        try:
            self.client = AsyncIOMotorClient(self.mongo_url)
            self.db = self.client[self.db_name]

            # Create collections and indexes
            await self.create_indexes()
            logger.info("Database connections initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def create_indexes(self):
        """Create necessary database indexes"""
        try:
            # Users collection indexes
            await self.db.users.create_index("user_id", unique=True)

            # Items collection indexes
            await self.db.items.create_index("item_id", unique=True)
            await self.db.items.create_index("content_type")
            await self.db.items.create_index("category")
            await self.db.items.create_index(
                [("title", "text"), ("description", "text"), ("tags", "text")]
            )

            # Interactions collection indexes
            await self.db.interactions.create_index("interaction_id", unique=True)
            await self.db.interactions.create_index("user_id")
            await self.db.interactions.create_index("item_id")
            await self.db.interactions.create_index("timestamp")
            await self.db.interactions.create_index([("user_id", 1), ("timestamp", -1)])

            logger.info("Database indexes created successfully")

        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

    async def insert_user(self, user: User) -> bool:
        """Insert a new user"""
        try:
            user_dict = user.model_dump()
            await self.db.users.update_one(
                {"user_id": user.user_id},
                {"$set": user_dict},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error inserting user: {e}")
            return False

    async def insert_item(self, item: Item) -> bool:
        """Insert a new item"""
        try:
            item_dict = item.model_dump()
            await self.db.items.update_one(
                {"item_id": item.item_id},
                {"$set": item_dict},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error inserting item: {e}")
            return False

    async def log_interaction(self, interaction: Interaction) -> bool:
        """Log user interaction"""
        try:
            interaction_dict = interaction.model_dump()

            # Insert interaction
            await self.db.interactions.insert_one(interaction_dict)

            # Update user interaction count
            await self.db.users.update_one(
                {"user_id": interaction.user_id},
                {"$inc": {"total_interactions": 1}}
            )

            # Update item view count for view interactions
            if interaction.interaction_type == InteractionType.VIEW:
                await self.db.items.update_one(
                    {"item_id": interaction.item_id},
                    {"$inc": {"view_count": 1}}
                )

            return True
        except Exception as e:
            logger.error(f"Error logging interaction: {e}")
            return False

    async def bulk_insert_interactions(self, interactions: List[Interaction]) -> bool:
        """
        Bulk-insert interactions using insert_many for performance (Bug #15 fix).
        Skips per-document user/item counter updates to avoid 60k roundtrips during seed.
        Counters can be rebuilt from the interactions collection on demand.
        """
        try:
            docs = [i.model_dump() for i in interactions]
            if docs:
                await self.db.interactions.insert_many(docs, ordered=False)
            logger.info(f"Bulk inserted {len(docs)} interactions")
            return True
        except Exception as e:
            logger.error(f"Error bulk inserting interactions: {e}")
            return False

    async def get_user_interactions(self, user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get user's recent interactions"""
        try:
            pipeline = [
                {"$match": {"user_id": user_id}},
                {"$sort": {"timestamp": -1}},
                {"$limit": limit},
                {
                    "$lookup": {
                        "from": "items",
                        "localField": "item_id",
                        "foreignField": "item_id",
                        "as": "item_info"
                    }
                },
                {"$unwind": {"path": "$item_info", "preserveNullAndEmptyArrays": True}},  # BUG-5 fix
                {
                    "$project": {
                        "interaction_id": 1,
                        "user_id": 1,
                        "item_id": 1,
                        "interaction_type": 1,
                        "timestamp": 1,
                        "dwell_seconds": 1,
                        "rating": 1,
                        "context": 1,
                        "title": "$item_info.title",
                        "category": "$item_info.category",
                        "content_type": "$item_info.content_type"
                    }
                }
            ]

            cursor = self.db.interactions.aggregate(pipeline)
            results = await cursor.to_list(length=limit)
            return results

        except Exception as e:
            logger.error(f"Error getting user interactions: {e}")
            return []

    async def get_popular_items(self, content_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Get popular items"""
        try:
            match_filter = {}
            if content_type:
                match_filter["content_type"] = content_type

            cursor = self.db.items.find(match_filter).sort(
                [("view_count", -1), ("rating", -1)]
            ).limit(limit)
            results = await cursor.to_list(length=limit)

            # Remove MongoDB _id (not JSON-serializable)
            return [{k: v for k, v in item.items() if k != '_id'} for item in results]

        except Exception as e:
            logger.error(f"Error getting popular items: {e}")
            return []

    async def get_similar_items(self, item_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get items similar to given item (based on category and tags)"""
        try:
            ref_item = await self.db.items.find_one({"item_id": item_id})
            if not ref_item:
                return []

            # Bug #11 fix: use $elemMatch for tag regex instead of $in with regex objects
            match_filter = {
                "item_id": {"$ne": item_id},
                "$or": [
                    {"category": ref_item["category"]},
                    {"tags": {"$elemMatch": {"$in": ref_item.get("tags", [])}}}
                ]
            }

            cursor = self.db.items.find(match_filter).sort(
                [("rating", -1), ("view_count", -1)]
            ).limit(limit)
            results = await cursor.to_list(length=limit)

            return [{k: v for k, v in item.items() if k != '_id'} for item in results]

        except Exception as e:
            logger.error(f"Error getting similar items: {e}")
            return []

    async def search_items(self, query: str, content_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Search items using text search"""
        try:
            match_filter: Dict[str, Any] = {"$text": {"$search": query}}
            if content_type:
                match_filter["content_type"] = content_type

            cursor = self.db.items.find(
                match_filter,
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]).limit(limit)

            results = await cursor.to_list(length=limit)
            return [{k: v for k, v in item.items() if k != '_id'} for item in results]

        except Exception as e:
            logger.error(f"Error searching items (text index): {e}")
            # Fallback to regex search
            try:
                regex_filter: Dict[str, Any] = {
                    "$or": [
                        {"title": {"$regex": query, "$options": "i"}},
                        {"description": {"$regex": query, "$options": "i"}},
                        {"category": {"$regex": query, "$options": "i"}},
                        # Bug #11 fix: $elemMatch for tag regex instead of $in with regex
                        {"tags": {"$elemMatch": {"$regex": query, "$options": "i"}}}
                    ]
                }
                if content_type:
                    regex_filter["content_type"] = content_type

                cursor = self.db.items.find(regex_filter).sort(
                    [("view_count", -1), ("rating", -1)]
                ).limit(limit)
                results = await cursor.to_list(length=limit)
                return [{k: v for k, v in item.items() if k != '_id'} for item in results]

            except Exception as e2:
                logger.error(f"Error in fallback search: {e2}")
                return []

    async def get_user_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user profile with interaction statistics"""
        try:
            user = await self.db.users.find_one({"user_id": user_id})
            if not user:
                return None

            # Bug #4 fix: strip non-serializable _id before returning
            user = {k: v for k, v in user.items() if k != '_id'}

            # Get interaction statistics
            pipeline = [
                {"$match": {"user_id": user_id}},
                {
                    "$lookup": {
                        "from": "items",
                        "localField": "item_id",
                        "foreignField": "item_id",
                        "as": "item_info"
                    }
                },
                {"$unwind": {"path": "$item_info", "preserveNullAndEmptyArrays": True}},  # NEW-1 fix
                {
                    "$group": {
                        "_id": "$user_id",
                        "categories": {"$push": "$item_info.category"},
                        "content_types": {"$push": "$item_info.content_type"},
                        "tags": {"$push": "$item_info.tags"},
                        "avg_dwell_time": {"$avg": "$dwell_seconds"},
                        "total_interactions": {"$sum": 1},
                        "avg_rating": {"$avg": "$rating"}
                    }
                }
            ]

            cursor = self.db.interactions.aggregate(pipeline)
            stats = await cursor.to_list(length=1)

            if stats:
                stat = stats[0]
                stat.pop("_id", None)  # remove _id from stats sub-doc too
                user["interaction_stats"] = stat

            return user

        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return None

    async def get_system_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        try:
            stats: Dict[str, Any] = {}

            # Basic counts
            stats["total_users"] = await self.db.users.count_documents({})
            stats["total_items"] = await self.db.items.count_documents({})
            stats["total_interactions"] = await self.db.interactions.count_documents({})

            # Active users in last 24 hours
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            active = await self.db.interactions.distinct("user_id", {"timestamp": {"$gte": yesterday}})
            stats["active_users_24h"] = len(active)

            # Interactions in last 24 hours
            stats["interactions_24h"] = await self.db.interactions.count_documents(
                {"timestamp": {"$gte": yesterday}}
            )

            # Items by content type
            pipeline = [
                {"$group": {"_id": "$content_type", "count": {"$sum": 1}}}
            ]
            cursor = self.db.items.aggregate(pipeline)
            items_by_type = await cursor.to_list(length=None)
            stats["items_by_type"] = {item["_id"]: item["count"] for item in items_by_type}

            # Popular categories in last 7 days
            week_ago = datetime.now(timezone.utc) - timedelta(days=7)
            pipeline = [
                {"$match": {"timestamp": {"$gte": week_ago}}},
                {
                    "$lookup": {
                        "from": "items",
                        "localField": "item_id",
                        "foreignField": "item_id",
                        "as": "item_info"
                    }
                },
                {"$unwind": {"path": "$item_info", "preserveNullAndEmptyArrays": True}},  # HIGH-2 fix
                {
                    "$group": {
                        "_id": "$item_info.category",
                        "interaction_count": {"$sum": 1}
                    }
                },
                {"$sort": {"interaction_count": -1}},
                {"$limit": 10}
            ]

            cursor = self.db.interactions.aggregate(pipeline)
            popular_categories = await cursor.to_list(length=10)
            stats["popular_categories"] = popular_categories

            return stats

        except Exception as e:
            logger.error(f"Error getting system stats: {e}")
            return {}

    async def cleanup(self):
        """Cleanup database connections"""
        if self.client:
            self.client.close()


# Global database instance
db_manager: Optional[DatabaseManager] = None


async def get_db_manager() -> DatabaseManager:
    """
    Return the singleton DatabaseManager, initialising it exactly once even
    under concurrent async calls (Bug #23 fix: asyncio.Lock).
    """
    global db_manager
    if db_manager is not None:
        return db_manager
    async with _init_lock:
        if db_manager is None:
            db_manager = DatabaseManager()
            await db_manager.initialize()
    return db_manager