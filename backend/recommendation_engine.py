import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import joblib
import logging
import asyncio
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, Counter
from models import ContentType, InteractionType
from database import get_db_manager

logger = logging.getLogger(__name__)

# Lock to guard model swap during retrain (Bug #20 fix)
_model_lock = asyncio.Lock()

# Paths for model persistence (MISSING-8 fix)
_MODEL_DIR = os.getenv("MODEL_DIR", "./models")
_MODEL_PATH = os.path.join(_MODEL_DIR, "xgb_model.joblib")
_SCALER_PATH = os.path.join(_MODEL_DIR, "scaler.joblib")


class RecommendationEngine:
    def __init__(self):
        self.xgb_model = None
        self.scaler = StandardScaler()
        self.tfidf_vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.item_embeddings: Dict[str, np.ndarray] = {}
        self.user_profiles: Dict[str, Any] = {}
        self.co_visitation_graph: Dict[str, Counter] = defaultdict(Counter)
        self.is_trained = False
        # Pre-built embedding matrix for vectorized cosine similarity (Bug #8 fix)
        self._embedding_ids: List[str] = []
        self._embedding_matrix: Optional[np.ndarray] = None

    async def initialize(self):
        """Initialize the recommendation engine"""
        logger.info("Initializing recommendation engine...")
        await self.build_co_visitation_graph()

        # MISSING-8 fix: try loading persisted model before training from scratch
        if os.path.exists(_MODEL_PATH) and os.path.exists(_SCALER_PATH):
            try:
                loaded_model = joblib.load(_MODEL_PATH)
                loaded_scaler = joblib.load(_SCALER_PATH)
                await self.generate_item_embeddings()
                await self.build_user_profiles()
                async with _model_lock:
                    self.xgb_model = loaded_model
                    self.scaler = loaded_scaler
                    self.is_trained = True
                logger.info(f"Loaded persisted model from {_MODEL_DIR} (skipping retrain)")
            except Exception as load_err:
                logger.warning(f"Could not load persisted model ({load_err}); training from scratch")
                await self.train_model()
        else:
            await self.train_model()

        logger.info("Recommendation engine initialized successfully")

    # ------------------------------------------------------------------
    # Co-visitation graph
    # ------------------------------------------------------------------
    async def build_co_visitation_graph(self):
        """Build co-visitation graph from user interactions"""
        try:
            db = await get_db_manager()

            pipeline = [
                {"$match": {"interaction_type": {"$in": ["view", "click", "like"]}}},
                {"$sort": {"user_id": 1, "timestamp": 1}},
                {
                    "$group": {
                        "_id": "$user_id",
                        "interactions": {
                            "$push": {
                                "item_id": "$item_id",
                                "timestamp": "$timestamp"
                            }
                        }
                    }
                }
            ]

            cursor = db.db.interactions.aggregate(pipeline)
            user_data = await cursor.to_list(length=None)

            self.co_visitation_graph = defaultdict(Counter)

            for user in user_data:
                interactions = user["interactions"]
                current_session: List[str] = []
                last_timestamp = None

                for interaction in interactions:
                    timestamp = interaction["timestamp"]
                    item_id = interaction["item_id"]

                    if last_timestamp and (timestamp - last_timestamp).total_seconds() > 3600:
                        if len(current_session) > 1:
                            self._update_co_visitation_graph(current_session)
                        current_session = [item_id]
                    else:
                        current_session.append(item_id)

                    last_timestamp = timestamp

                if len(current_session) > 1:
                    self._update_co_visitation_graph(current_session)

            logger.info(f"Built co-visitation graph with {len(self.co_visitation_graph)} items")

        except Exception as e:
            logger.error(f"Error building co-visitation graph: {e}")
            # Bug #6 fix: reset to empty so callers know data is unavailable
            self.co_visitation_graph = defaultdict(Counter)

    def _update_co_visitation_graph(self, session_items: List[str]):
        """Update co-visitation graph with session items"""
        for i, item_a in enumerate(session_items):
            for item_b in session_items[i + 1: i + 6]:  # Look ahead 5 items
                self.co_visitation_graph[item_a][item_b] += 1
                self.co_visitation_graph[item_b][item_a] += 1

    # ------------------------------------------------------------------
    # Item embeddings (TF-IDF)
    # ------------------------------------------------------------------
    async def generate_item_embeddings(self):
        """Generate TF-IDF embeddings for items"""
        try:
            db = await get_db_manager()

            cursor = db.db.items.find({})
            items = await cursor.to_list(length=None)

            items_data = []
            item_ids = []

            for item in items:
                text_features = f"{item['title']} {item['description']} {item['category']}"
                if item.get('tags'):
                    text_features += " " + " ".join(item['tags'])

                items_data.append(text_features)
                item_ids.append(item['item_id'])

            if items_data:
                tfidf_matrix = self.tfidf_vectorizer.fit_transform(items_data)

                self.item_embeddings = {}
                for i, item_id in enumerate(item_ids):
                    self.item_embeddings[item_id] = tfidf_matrix[i].toarray()[0]

                # Bug #8 fix: pre-build contiguous matrix + ordered id list
                self._embedding_ids = list(self.item_embeddings.keys())
                self._embedding_matrix = np.array(
                    [self.item_embeddings[iid] for iid in self._embedding_ids]
                )

            logger.info(f"Generated embeddings for {len(self.item_embeddings)} items")

        except Exception as e:
            logger.error(f"Error generating item embeddings: {e}")
            # Bug #6 fix: reset so callers know data is unavailable
            self.item_embeddings = {}
            self._embedding_ids = []
            self._embedding_matrix = None

    # ------------------------------------------------------------------
    # User profiles
    # ------------------------------------------------------------------
    async def build_user_profiles(self):
        """Build user profiles based on interaction history"""
        try:
            db = await get_db_manager()

            pipeline = [
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "user_id",
                        "foreignField": "user_id",
                        "as": "user_info"
                    }
                },
                {"$unwind": {"path": "$user_info", "preserveNullAndEmptyArrays": True}},  # BUG-6 fix
                {
                    "$lookup": {
                        "from": "items",
                        "localField": "item_id",
                        "foreignField": "item_id",
                        "as": "item_info"
                    }
                },
                {"$unwind": {"path": "$item_info", "preserveNullAndEmptyArrays": True}},  # BUG-6 fix
                {
                    "$group": {
                        "_id": "$user_id",
                        "user_info": {"$first": "$user_info"},
                        "categories": {"$push": "$item_info.category"},
                        "content_types": {"$push": "$item_info.content_type"},
                        "tags": {"$push": "$item_info.tags"},
                        "dwell_times": {"$push": "$dwell_seconds"},
                        "ratings": {"$push": "$rating"},
                        "interaction_count": {"$sum": 1}
                    }
                }
            ]

            cursor = db.db.interactions.aggregate(pipeline)
            user_data = await cursor.to_list(length=None)

            self.user_profiles = {}

            for user in user_data:
                user_id = user["_id"]
                user_info = user["user_info"]

                all_tags: List[str] = []
                for tag_list in user["tags"]:
                    if tag_list:
                        all_tags.extend(tag_list)

                valid_dwells = [d for d in user["dwell_times"] if d is not None and d > 0]
                avg_dwell_time = float(np.mean(valid_dwells)) if valid_dwells else 0.0
                ratings = [r for r in user["ratings"] if r is not None]
                avg_rating = float(np.mean(ratings)) if ratings else 0.0

                self.user_profiles[user_id] = {
                    'demographics': {
                        'country': user_info.get('country', 'US'),
                        'device': user_info.get('device', 'web'),
                        'age_group': user_info.get('age_group', '25-34'),
                        'preferences': user_info.get('preferences', [])
                    },
                    'categories': Counter(user["categories"]),
                    'content_types': Counter(user["content_types"]),
                    'tags': Counter(all_tags),
                    'avg_dwell_time': avg_dwell_time,
                    'interaction_count': user["interaction_count"],
                    'avg_rating': avg_rating
                }

            logger.info(f"Built profiles for {len(self.user_profiles)} users")

        except Exception as e:
            logger.error(f"Error building user profiles: {e}")
            # Bug #6 fix: reset so callers know data is unavailable
            self.user_profiles = {}

    # ------------------------------------------------------------------
    # Training data
    # ------------------------------------------------------------------
    async def prepare_training_data(self) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare training data for XGBoost model"""
        try:
            db = await get_db_manager()

            pipeline = [
                {
                    "$match": {
                        "interaction_type": {"$in": ["view", "like", "purchase", "bookmark"]},
                        "dwell_seconds": {"$gt": 10}
                    }
                },
                {
                    "$lookup": {
                        "from": "items",
                        "localField": "item_id",
                        "foreignField": "item_id",
                        "as": "item_info"
                    }
                },
                {"$unwind": "$item_info"},
                {"$limit": 10000}
            ]

            cursor = db.db.interactions.aggregate(pipeline)
            positive_interactions = await cursor.to_list(length=10000)

            # Bug #7 fix: fetch real random items for negative sampling
            all_items = await db.db.items.find({}).to_list(length=None)
            all_item_ids = [item["item_id"] for item in all_items]
            all_item_map = {item["item_id"]: item for item in all_items}

            all_users = await db.db.users.find({}, {"user_id": 1}).to_list(length=None)
            all_user_ids = [user["user_id"] for user in all_users]

            features = []
            labels = []

            # Positive interactions
            for interaction in positive_interactions:
                feature_vector = self.extract_features(interaction, True)
                if feature_vector is not None:
                    features.append(feature_vector)
                    if interaction['interaction_type'] in ['like', 'purchase', 'bookmark']:
                        labels.append(1.0)
                    elif interaction['dwell_seconds'] > 60:
                        labels.append(1.0)
                    else:
                        labels.append(0.7)

            # Bug #7 fix: negative samples use REAL item data from the DB
            negative_count = min(len(positive_interactions), 5000)
            for _ in range(negative_count):
                user_id = np.random.choice(all_user_ids)
                item_id = np.random.choice(all_item_ids)
                real_item = all_item_map.get(item_id, {})

                fake_interaction = {
                    'user_id': user_id,
                    'item_id': item_id,
                    'interaction_type': 'view',
                    'dwell_seconds': 0,
                    'rating': None,
                    'item_info': real_item  # real item data, not hardcoded defaults
                }

                feature_vector = self.extract_features(fake_interaction, False)
                if feature_vector is not None:
                    features.append(feature_vector)
                    labels.append(0.0)

            logger.info(f"Prepared {len(features)} training samples")
            return np.array(features), np.array(labels)

        except Exception as e:
            logger.error(f"Error preparing training data: {e}")
            return np.array([]), np.array([])

    # ------------------------------------------------------------------
    # Feature extraction
    # ------------------------------------------------------------------
    def extract_features(self, interaction: Dict[str, Any], is_positive: bool) -> Optional[List[float]]:
        """Extract features for ML model"""
        try:
            user_id = interaction['user_id']
            item_info = interaction.get('item_info', {})

            user_profile = self.user_profiles.get(user_id, {})
            user_interaction_count = user_profile.get('interaction_count', 0)
            user_avg_dwell = user_profile.get('avg_dwell_time', 0)
            user_avg_rating = user_profile.get('avg_rating', 0)

            item_rating = item_info.get('rating', 0.0)
            view_count = item_info.get('view_count', 0)

            # Bug #3 fix: use timezone-aware now() to subtract from MongoDB datetimes
            publish_ts = item_info.get('publish_ts', datetime.now(timezone.utc))
            if isinstance(publish_ts, str):
                publish_ts = datetime.fromisoformat(publish_ts.replace('Z', '+00:00'))
            # Ensure publish_ts is timezone-aware
            if publish_ts.tzinfo is None:
                publish_ts = publish_ts.replace(tzinfo=timezone.utc)
            hours_since_publish = (datetime.now(timezone.utc) - publish_ts).total_seconds() / 3600

            content_type_features = [0] * len(ContentType)
            try:
                content_type_idx = list(ContentType).index(ContentType(item_info.get('content_type', 'video')))
                content_type_features[content_type_idx] = 1
            except (ValueError, KeyError):
                # Bug #19 fix: only catch specific, expected exceptions
                pass

            user_categories = user_profile.get('categories', Counter())
            category_affinity = (
                user_categories.get(item_info.get('category', ''), 0) /
                max(user_interaction_count, 1)
            )

            features = [
                user_interaction_count,
                user_avg_dwell,
                user_avg_rating,
                item_rating,
                np.log1p(view_count),
                np.log1p(max(hours_since_publish, 0)),
                category_affinity,
                interaction.get('dwell_seconds', 0),
            ] + content_type_features

            return features

        except Exception as e:
            logger.error(f"Error extracting features: {e}")
            return None

    # ------------------------------------------------------------------
    # Model training (thread-safe via lock — Bug #20 fix)
    # ------------------------------------------------------------------
    async def train_model(self):
        """Train the XGBoost ranking model"""
        try:
            await self.generate_item_embeddings()
            await self.build_user_profiles()

            X, y = await self.prepare_training_data()

            if len(X) == 0:
                logger.warning("No training data available, skipping model training")
                return

            X_scaled = self.scaler.fit_transform(X)

            new_model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42
            )
            new_model.fit(X_scaled, y)

            # Bug #20 fix: acquire lock before swapping model/scaler/flag atomically
            async with _model_lock:
                self.xgb_model = new_model
                self.is_trained = True

            # MISSING-8 fix: persist model to disk so restarts skip retraining
            try:
                os.makedirs(_MODEL_DIR, exist_ok=True)
                joblib.dump(new_model, _MODEL_PATH)
                joblib.dump(self.scaler, _SCALER_PATH)
                logger.info(f"Model persisted to {_MODEL_DIR}")
            except Exception as save_err:
                logger.warning(f"Could not persist model to disk: {save_err}")

            logger.info("XGBoost model trained successfully")

        except Exception as e:
            logger.error(f"Error training model: {e}")

    # ------------------------------------------------------------------
    # Candidate generation
    # ------------------------------------------------------------------
    async def generate_candidates(self, user_id: str, content_type: Optional[str] = None, n_candidates: int = 500) -> List[str]:
        """Generate candidate items using multiple strategies"""
        candidates: set = set()

        try:
            user_interactions = await self.get_user_recent_items(user_id, limit=10)
            for item_id in user_interactions:
                similar_items = self.co_visitation_graph.get(item_id, {})
                candidates.update(list(similar_items.keys())[:20])

            db = await get_db_manager()
            popular_items = await db.get_popular_items(content_type, limit=100)
            candidates.update([item['item_id'] for item in popular_items])

            if user_interactions and self.item_embeddings:
                for item_id in user_interactions[:3]:
                    similar_items_list = await self.get_content_similar_items(item_id, limit=50)
                    candidates.update(similar_items_list)

            seen_items = set(user_interactions)
            candidates = candidates - seen_items

            # Bug #14 fix: sort for deterministic selection before slicing
            candidate_list = sorted(candidates)[:n_candidates]

            if len(candidate_list) < n_candidates:
                additional_items = await db.get_popular_items(
                    content_type, limit=n_candidates - len(candidate_list)
                )
                for item in additional_items:
                    if item['item_id'] not in candidates and item['item_id'] not in seen_items:
                        candidate_list.append(item['item_id'])

            logger.info(f"Generated {len(candidate_list)} candidates for user {user_id}")
            return candidate_list

        except Exception as e:
            logger.error(f"Error generating candidates: {e}")
            return []

    async def get_user_recent_items(self, user_id: str, limit: int = 50) -> List[str]:
        """Get user's recent interaction items"""
        try:
            db = await get_db_manager()
            interactions = await db.get_user_interactions(user_id, limit)
            return [interaction['item_id'] for interaction in interactions]
        except Exception as e:
            logger.error(f"Error getting user recent items: {e}")
            return []

    async def get_content_similar_items(self, item_id: str, limit: int = 20) -> List[str]:
        """
        Get content-similar items using embeddings.
        Bug #8 fix: vectorized matrix multiplication instead of O(n²) loop.
        """
        try:
            if item_id not in self.item_embeddings or self._embedding_matrix is None:
                return []

            item_embedding = self.item_embeddings[item_id].reshape(1, -1)
            # Single vectorized call — O(n) instead of O(n²) individual calls
            sims = cosine_similarity(item_embedding, self._embedding_matrix)[0]

            # Pair up (id, similarity), exclude self
            id_sim_pairs = [
                (self._embedding_ids[i], sims[i])
                for i in range(len(self._embedding_ids))
                if self._embedding_ids[i] != item_id
            ]
            id_sim_pairs.sort(key=lambda x: x[1], reverse=True)
            return [iid for iid, _ in id_sim_pairs[:limit]]

        except Exception as e:
            logger.error(f"Error getting content similar items: {e}")
            return []

    # ------------------------------------------------------------------
    # Ranking
    # ------------------------------------------------------------------
    async def rank_candidates(self, user_id: str, candidate_ids: List[str]) -> List[Dict[str, Any]]:
        """Rank candidate items using ML model (thread-safe read — Bug #20 fix)"""
        try:
            async with _model_lock:
                is_trained = self.is_trained
                model = self.xgb_model

            if not is_trained or not candidate_ids or model is None:
                return await self.popularity_ranking(candidate_ids)

            db = await get_db_manager()

            cursor = db.db.items.find({"item_id": {"$in": candidate_ids}})
            items = await cursor.to_list(length=len(candidate_ids))

            scored_items = []

            for item in items:
                fake_interaction = {
                    'user_id': user_id,
                    'item_id': item['item_id'],
                    'interaction_type': 'view',
                    'dwell_seconds': 0,
                    'rating': None,
                    'item_info': item
                }

                features = self.extract_features(fake_interaction, False)
                if features:
                    features_scaled = self.scaler.transform([features])
                    score = model.predict(features_scaled)[0]

                    scored_items.append({
                        'item_id': item['item_id'],
                        'title': item['title'],
                        'content_type': item['content_type'],
                        'category': item['category'],
                        'description': item['description'],
                        'thumbnail_url': item['thumbnail_url'],
                        'rating': item['rating'],
                        'view_count': item['view_count'],
                        'ml_score': float(score),
                        'tags': item.get('tags', [])
                    })

            scored_items.sort(key=lambda x: x['ml_score'], reverse=True)

            logger.info(f"Ranked {len(scored_items)} items for user {user_id}")
            return scored_items

        except Exception as e:
            logger.error(f"Error ranking candidates: {e}")
            return await self.popularity_ranking(candidate_ids)

    async def popularity_ranking(self, candidate_ids: List[str]) -> List[Dict[str, Any]]:
        """Fallback popularity-based ranking"""
        try:
            db = await get_db_manager()

            if candidate_ids:
                cursor = db.db.items.find(
                    {"item_id": {"$in": candidate_ids}}
                ).sort([("view_count", -1), ("rating", -1)])
            else:
                cursor = db.db.items.find({}).sort(
                    [("view_count", -1), ("rating", -1)]
                ).limit(50)

            items = await cursor.to_list(length=max(len(candidate_ids), 1))  # BUG-9 fix: don't hard-cap at 50

            return [
                {
                    'item_id': item['item_id'],
                    'title': item['title'],
                    'content_type': item['content_type'],
                    'category': item['category'],
                    'description': item['description'],
                    'thumbnail_url': item['thumbnail_url'],
                    'rating': item['rating'],
                    'view_count': item['view_count'],
                    'ml_score': 0.0,
                    'tags': item.get('tags', [])
                }
                for item in items
            ]

        except Exception as e:
            logger.error(f"Error in popularity ranking: {e}")
            return []

    async def get_recommendations(self, user_id: str, n: int = 10, content_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Main recommendation function"""
        try:
            candidates = await self.generate_candidates(user_id, content_type, n_candidates=500)
            ranked_items = await self.rank_candidates(user_id, candidates)

            if content_type:
                ranked_items = [item for item in ranked_items if item['content_type'] == content_type]

            return ranked_items[:n]

        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []


# ------------------------------------------------------------------
# Singleton helper (thread-safe — Bug #23 pattern applied here too)
# ------------------------------------------------------------------
recommendation_engine: Optional[RecommendationEngine] = None
_engine_lock = asyncio.Lock()


async def get_recommendation_engine() -> RecommendationEngine:
    global recommendation_engine
    if recommendation_engine is not None:
        return recommendation_engine
    async with _engine_lock:
        if recommendation_engine is None:
            recommendation_engine = RecommendationEngine()
            await recommendation_engine.initialize()
    return recommendation_engine