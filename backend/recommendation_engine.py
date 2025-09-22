import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, Counter
from models import ContentType, InteractionType
from database import get_db_manager

logger = logging.getLogger(__name__)

class RecommendationEngine:
    def __init__(self):
        self.xgb_model = None
        self.scaler = StandardScaler()
        self.tfidf_vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.item_embeddings = {}
        self.user_profiles = {}
        self.co_visitation_graph = defaultdict(Counter)
        self.is_trained = False
        
    async def initialize(self):
        """Initialize the recommendation engine"""
        logger.info("Initializing recommendation engine...")
        await self.build_co_visitation_graph()
        await self.train_model()
        logger.info("Recommendation engine initialized successfully")
    
    async def build_co_visitation_graph(self):
        """Build co-visitation graph from user interactions"""
        try:
            db = await get_db_manager()
            
            # Get user sessions (interactions within 1 hour)
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
            
            # Group interactions by user sessions
            self.co_visitation_graph = defaultdict(Counter)
            
            for user in user_data:
                interactions = user["interactions"]
                current_session = []
                last_timestamp = None
                
                for interaction in interactions:
                    timestamp = interaction["timestamp"]
                    item_id = interaction["item_id"]
                    
                    # Start new session if gap > 1 hour
                    if last_timestamp and (timestamp - last_timestamp).total_seconds() > 3600:
                        if len(current_session) > 1:
                            self._update_co_visitation_graph(current_session)
                        current_session = [item_id]
                    else:
                        current_session.append(item_id)
                    
                    last_timestamp = timestamp
                
                # Process last session
                if len(current_session) > 1:
                    self._update_co_visitation_graph(current_session)
            
            logger.info(f"Built co-visitation graph with {len(self.co_visitation_graph)} items")
            
        except Exception as e:
            logger.error(f"Error building co-visitation graph: {e}")
    
    def _update_co_visitation_graph(self, session_items: List[str]):
        """Update co-visitation graph with session items"""
        for i, item_a in enumerate(session_items):
            for item_b in session_items[i+1:i+6]:  # Look ahead 5 items
                self.co_visitation_graph[item_a][item_b] += 1
                self.co_visitation_graph[item_b][item_a] += 1
    
    async def generate_item_embeddings(self):
        """Generate TF-IDF embeddings for items"""
        try:
            db = await get_db_manager()
            
            cursor = db.db.items.find({})
            items = await cursor.to_list(length=None)
            
            items_data = []
            item_ids = []
            
            for item in items:
                # Combine text features
                text_features = f"{item['title']} {item['description']} {item['category']}"
                if item.get('tags'):
                    text_features += " " + " ".join(item['tags'])
                
                items_data.append(text_features)
                item_ids.append(item['item_id'])
            
            if items_data:
                # Generate TF-IDF embeddings
                tfidf_matrix = self.tfidf_vectorizer.fit_transform(items_data)
                
                # Store embeddings
                self.item_embeddings = {}
                for i, item_id in enumerate(item_ids):
                    self.item_embeddings[item_id] = tfidf_matrix[i].toarray()[0]
            
            logger.info(f"Generated embeddings for {len(self.item_embeddings)} items")
            
        except Exception as e:
            logger.error(f"Error generating item embeddings: {e}")
    
    async def build_user_profiles(self):
        """Build user profiles based on interaction history"""
        try:
            db = await get_db_manager()
            
            # Get user interaction data
            pipeline = [
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "user_id",
                        "foreignField": "user_id",
                        "as": "user_info"
                    }
                },
                {"$unwind": "$user_info"},
                {
                    "$lookup": {
                        "from": "items",
                        "localField": "item_id",
                        "foreignField": "item_id",
                        "as": "item_info"
                    }
                },
                {"$unwind": "$item_info"},
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
                
                # Flatten tags
                all_tags = []
                for tag_list in user["tags"]:
                    if tag_list:
                        all_tags.extend(tag_list)
                
                # Calculate averages
                avg_dwell_time = np.mean([d for d in user["dwell_times"] if d is not None and d > 0]) or 0
                ratings = [r for r in user["ratings"] if r is not None]
                avg_rating = np.mean(ratings) if ratings else 0
                
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
    
    async def prepare_training_data(self) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare training data for XGBoost model"""
        try:
            db = await get_db_manager()
            
            # Get positive interactions
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
                {"$limit": 10000}  # Limit for performance
            ]
            
            cursor = db.db.interactions.aggregate(pipeline)
            positive_interactions = await cursor.to_list(length=10000)
            
            # Get all items and users for negative sampling
            all_items = await db.db.items.find({}, {"item_id": 1}).to_list(length=None)
            all_item_ids = [item["item_id"] for item in all_items]
            
            all_users = await db.db.users.find({}, {"user_id": 1}).to_list(length=None)
            all_user_ids = [user["user_id"] for user in all_users]
            
            features = []
            labels = []
            
            # Process positive interactions
            for interaction in positive_interactions:
                feature_vector = self.extract_features(interaction, True)
                if feature_vector is not None:
                    features.append(feature_vector)
                    # Label based on interaction quality
                    if interaction['interaction_type'] in ['like', 'purchase', 'bookmark']:
                        labels.append(1.0)
                    elif interaction['dwell_seconds'] > 60:
                        labels.append(1.0)
                    else:
                        labels.append(0.7)  # Viewed but lower quality
            
            # Generate negative samples
            negative_samples = min(len(positive_interactions), 5000)
            for _ in range(negative_samples):
                user_id = np.random.choice(all_user_ids)
                item_id = np.random.choice(all_item_ids)
                
                # Create fake negative interaction
                fake_interaction = {
                    'user_id': user_id,
                    'item_id': item_id,
                    'interaction_type': 'view',
                    'dwell_seconds': 0,
                    'rating': None,
                    'item_info': {
                        'category': 'unknown',
                        'content_type': 'video',
                        'rating': 0.0,
                        'view_count': 0,
                        'publish_ts': datetime.now()
                    }
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
    
    def extract_features(self, interaction: Dict[str, Any], is_positive: bool) -> Optional[List[float]]:
        """Extract features for ML model"""
        try:
            user_id = interaction['user_id']
            item_id = interaction['item_id']
            item_info = interaction.get('item_info', {})
            
            # User features
            user_profile = self.user_profiles.get(user_id, {})
            user_interaction_count = user_profile.get('interaction_count', 0)
            user_avg_dwell = user_profile.get('avg_dwell_time', 0)
            user_avg_rating = user_profile.get('avg_rating', 0)
            
            # Item features
            item_rating = item_info.get('rating', 0.0)
            view_count = item_info.get('view_count', 0)
            
            # Time features
            publish_ts = item_info.get('publish_ts', datetime.now())
            if isinstance(publish_ts, str):
                publish_ts = datetime.fromisoformat(publish_ts.replace('Z', '+00:00'))
            hours_since_publish = (datetime.now() - publish_ts).total_seconds() / 3600
            
            # Content type encoding
            content_type_features = [0] * len(ContentType)
            try:
                content_type_idx = list(ContentType).index(ContentType(item_info.get('content_type', 'video')))
                content_type_features[content_type_idx] = 1
            except:
                pass
            
            # Category popularity for user
            user_categories = user_profile.get('categories', Counter())
            category_affinity = user_categories.get(item_info.get('category', ''), 0) / max(user_interaction_count, 1)
            
            # Combine all features
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
    
    async def train_model(self):
        """Train the XGBoost ranking model"""
        try:
            await self.generate_item_embeddings()
            await self.build_user_profiles()
            
            X, y = await self.prepare_training_data()
            
            if len(X) == 0:
                logger.warning("No training data available, using dummy model")
                return
            
            # Scale features
            X_scaled = self.scaler.fit_transform(X)
            
            # Train XGBoost model
            self.xgb_model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42
            )
            
            self.xgb_model.fit(X_scaled, y)
            self.is_trained = True
            
            logger.info("XGBoost model trained successfully")
            
        except Exception as e:
            logger.error(f"Error training model: {e}")
    
    async def generate_candidates(self, user_id: str, content_type: Optional[str] = None, n_candidates: int = 500) -> List[str]:
        """Generate candidate items using multiple strategies"""
        candidates = set()
        
        try:
            # Strategy 1: Co-visitation graph
            user_interactions = await self.get_user_recent_items(user_id, limit=10)
            for item_id in user_interactions:
                similar_items = self.co_visitation_graph.get(item_id, {})
                candidates.update(list(similar_items.keys())[:20])
            
            # Strategy 2: Popular items
            db = await get_db_manager()
            popular_items = await db.get_popular_items(content_type, limit=100)
            candidates.update([item['item_id'] for item in popular_items])
            
            # Strategy 3: Content similarity
            if user_interactions and self.item_embeddings:
                for item_id in user_interactions[:3]:  # Use top 3 recent items
                    similar_items = await self.get_content_similar_items(item_id, limit=50)
                    candidates.update(similar_items)
            
            # Remove already interacted items
            seen_items = set(user_interactions)
            candidates = candidates - seen_items
            
            # Convert to list and limit
            candidate_list = list(candidates)[:n_candidates]
            
            # Fill with random popular items if not enough candidates
            if len(candidate_list) < n_candidates:
                additional_items = await db.get_popular_items(content_type, limit=n_candidates - len(candidate_list))
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
        """Get content-similar items using embeddings"""
        try:
            if item_id not in self.item_embeddings:
                return []
            
            item_embedding = self.item_embeddings[item_id]
            similarities = []
            
            for other_item_id, other_embedding in self.item_embeddings.items():
                if other_item_id != item_id:
                    similarity = cosine_similarity([item_embedding], [other_embedding])[0][0]
                    similarities.append((other_item_id, similarity))
            
            # Sort by similarity and return top items
            similarities.sort(key=lambda x: x[1], reverse=True)
            return [item_id for item_id, _ in similarities[:limit]]
            
        except Exception as e:
            logger.error(f"Error getting content similar items: {e}")
            return []
    
    async def rank_candidates(self, user_id: str, candidate_ids: List[str]) -> List[Dict[str, Any]]:
        """Rank candidate items using ML model"""
        try:
            if not self.is_trained or not candidate_ids:
                # Fallback to popularity ranking
                return await self.popularity_ranking(candidate_ids)
            
            db = await get_db_manager()
            
            # Get candidate item details
            cursor = db.db.items.find({"item_id": {"$in": candidate_ids}})
            items = await cursor.to_list(length=len(candidate_ids))
            
            scored_items = []
            
            for item in items:
                # Create interaction for feature extraction
                fake_interaction = {
                    'user_id': user_id,
                    'item_id': item['item_id'],
                    'interaction_type': 'view',
                    'dwell_seconds': 0,
                    'rating': None,
                    'item_info': item
                }
                
                # Extract features and predict score
                features = self.extract_features(fake_interaction, False)
                if features:
                    features_scaled = self.scaler.transform([features])
                    score = self.xgb_model.predict(features_scaled)[0]
                    
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
            
            # Sort by ML score
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
                cursor = db.db.items.find({"item_id": {"$in": candidate_ids}}).sort([("view_count", -1), ("rating", -1)])
            else:
                cursor = db.db.items.find({}).sort([("view_count", -1), ("rating", -1)]).limit(50)
            
            items = await cursor.to_list(length=50)
            
            result = []
            for item in items:
                result.append({
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
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error in popularity ranking: {e}")
            return []
    
    async def get_recommendations(self, user_id: str, n: int = 10, content_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Main recommendation function"""
        try:
            # Generate candidates
            candidates = await self.generate_candidates(user_id, content_type, n_candidates=500)
            
            # Rank candidates
            ranked_items = await self.rank_candidates(user_id, candidates)
            
            # Apply content type filter if specified
            if content_type:
                ranked_items = [item for item in ranked_items if item['content_type'] == content_type]
            
            # Return top N recommendations
            return ranked_items[:n]
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []

# Global recommendation engine instance
recommendation_engine = None

async def get_recommendation_engine():
    global recommendation_engine
    if recommendation_engine is None:
        recommendation_engine = RecommendationEngine()
        await recommendation_engine.initialize()
    return recommendation_engine