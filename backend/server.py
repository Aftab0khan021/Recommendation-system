from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
import logging
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
load_dotenv()
SEED_ON_STARTUP = os.getenv("SEED_ON_STARTUP", "false").lower() == "true"
# Switch between synthetic (Faker) data and real data from APIs + MovieLens
USE_REAL_DATA = os.getenv("USE_REAL_DATA", "false").lower() == "true"
# How many NEW real-user interactions trigger a background model retrain
RETRAIN_THRESHOLD = int(os.getenv("RETRAIN_THRESHOLD", "100"))
_raw_admin_key = os.getenv("ADMIN_API_KEY", "")
if not _raw_admin_key:
    import warnings
    warnings.warn(
        "ADMIN_API_KEY is not set. The /admin/retrain endpoint is disabled until you set it.",
        RuntimeWarning,
        stacklevel=1,
    )
ADMIN_API_KEY = _raw_admin_key or None

# Read allowed origins from environment (BUG-2 fix)
_cors_origins_raw = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000"
)
CORS_ORIGINS: list[str] = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]

# Import our modules
from models import (
    EventData, RecommendationRequest, RecommendationResponse, 
    ABTestArm, ContentType, InteractionType, Interaction,
    SearchRequest, SearchResponse
)
from database import get_db_manager
from recommendation_engine import get_recommendation_engine
from ab_testing import ab_test_manager
from data_seeder import DataSeeder
from search_engine import search_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state tracking
app_state = {
    'db_initialized': False,
    'recommendation_engine_ready': False,
    'data_seeded': False,
    # Auto-retrain: incremented on every real user interaction
    'interaction_counter': 0,
    # True while a background retrain is in progress
    'retraining': False,
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    logger.info("Starting Real-Time Recommendation System...")
    
    try:
        # Initialize database
        db = await get_db_manager()
        app_state['db_initialized'] = True
        logger.info("✅ Database initialized")
        
        # Check if we need to seed data
        user_count = await db.db.users.count_documents({})
        item_count = await db.db.items.count_documents({})
        if SEED_ON_STARTUP and (user_count == 0 or item_count == 0):
            if USE_REAL_DATA:
                logger.info("Seeding with REAL data (USE_REAL_DATA=true)...")
                seeder = DataSeeder()
                await seeder.seed_real_data()
            else:
                logger.info("Seeding with synthetic data (SEED_ON_STARTUP=true)...")
                seeder = DataSeeder()
                await seeder.seed_all_data(users_count=500, items_count=2000, interactions_count=20000)
            app_state['data_seeded'] = True
            logger.info("✅ Database seeded")
        else:
            if user_count == 0 or item_count == 0:
                logger.info("⏭️ Skipping seeding (SEED_ON_STARTUP=false). DB is empty; seed manually for prod.")
            else:
                logger.info(f"✅ Database already contains {user_count} users and {item_count} items")
            app_state['data_seeded'] = True

        # Initialize recommendation engine
        rec_engine = await get_recommendation_engine()
        app_state['recommendation_engine_ready'] = True
        logger.info("✅ Recommendation engine initialized")
        
        logger.info("🚀 Real-Time Recommendation System is ready!")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize application: {e}")
        raise
    
    yield
    
    # Cleanup
    logger.info("Shutting down Real-Time Recommendation System...")
    try:
        db = await get_db_manager()
        await db.cleanup()
        logger.info("✅ Database connections closed")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

# Create FastAPI app
app = FastAPI(
    title="Real-Time Recommendation System",
    description="A scalable recommendation system with ML ranking, real-time events, A/B testing, and intelligent search",
    version="1.0.0",
    lifespan=lifespan
)

# Create API router
api_router = APIRouter(prefix="/api")

# CORS middleware (BUG-2 fix: origins read from CORS_ORIGINS env var)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@api_router.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc),
        "services": {
            "database": app_state['db_initialized'],
            "recommendation_engine": app_state['recommendation_engine_ready'],
            "data_available": app_state['data_seeded']
        }
    }

# Event logging endpoint
@api_router.post("/event")
async def log_event(event: EventData, background_tasks: BackgroundTasks):
    """Log user interaction events — counts interactions and triggers auto-retrain."""
    try:
        # Create interaction object
        interaction = Interaction(
            user_id=event.user_id,
            item_id=event.item_id,
            interaction_type=event.type,
            timestamp=event.ts,
            dwell_seconds=event.dwell_seconds,
            rating=event.rating,
            context=event.context
        )
        
        # Log to database
        db = await get_db_manager()
        success = await db.log_interaction(interaction)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to log interaction")
        
        # Log A/B test event
        ab_test_manager.log_experiment_event(
            user_id=event.user_id,
            experiment_id='recommendation_algorithm_v1',
            event_type=f'interaction_{event.type}',
            event_data={
                'item_id': event.item_id,
                'dwell_seconds': event.dwell_seconds,
                'context': event.context
            }
        )

        # ── Auto-retrain: count real interactions and trigger retrain ─────────
        app_state['interaction_counter'] += 1
        if (app_state['interaction_counter'] >= RETRAIN_THRESHOLD
                and not app_state['retraining']):
            app_state['interaction_counter'] = 0
            background_tasks.add_task(_retrain_background)
            logger.info(
                f"Auto-retrain triggered after {RETRAIN_THRESHOLD} new interactions"
            )
        
        return {
            "status": "success",
            "interaction_id": interaction.interaction_id,
            "timestamp": interaction.timestamp
        }
        
    except Exception as e:
        logger.error(f"Error logging event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _retrain_background():
    """Background task: retrain the recommendation model non-blocking."""
    if app_state['retraining']:
        logger.info("Retrain already in progress — skipping.")
        return
    app_state['retraining'] = True
    try:
        logger.info("[Auto-Retrain] Starting background model retrain...")
        rec_engine = await get_recommendation_engine()
        await rec_engine.train_model()
        logger.info("[Auto-Retrain] Model retrained and swapped successfully.")
    except Exception as exc:
        logger.error(f"[Auto-Retrain] Failed: {exc}")
    finally:
        app_state['retraining'] = False


# Recommendations endpoint
@api_router.get("/recommend", response_model=RecommendationResponse)
async def get_recommendations(
    user_id: str,
    n: int = 10,
    content_type: Optional[str] = None,
    exclude_seen: bool = True,
    exclude_ids: Optional[str] = None,   # comma-separated item_ids already shown this session
):
    """
    Get personalized recommendations for a user.

    exclude_ids: comma-separated list of item_ids the frontend has already shown
                 in the current session (e.g. "tmdb_movie_155,rawg_game_12").
                 The endpoint will NEVER return any of these items, even via
                 Load More pagination.
    """
    try:
        # Validate parameters (BUG-10 fix: clamp both min and max)
        n = max(1, min(n, 100))

        # Validate content type
        if content_type and content_type not in [ct.value for ct in ContentType]:
            raise HTTPException(status_code=400, detail="Invalid content type")

        # Parse exclude_ids
        excluded_set: List[str] = (
            [eid.strip() for eid in exclude_ids.split(",") if eid.strip()]
            if exclude_ids else []
        )

        # Get A/B test assignment
        should_use_ml = ab_test_manager.should_use_xgboost(user_id)
        algorithm = "xgboost_ml" if should_use_ml else "popularity_based"

        # Get recommendations
        rec_engine = await get_recommendation_engine()

        if should_use_ml and rec_engine.is_trained:
            recommendations = await rec_engine.get_recommendations(
                user_id, n, content_type, exclude_ids=excluded_set
            )
        else:
            # Fallback: popularity-based, filtered + title-deduped
            db = await get_db_manager()
            # Fetch more than n so we have headroom after filtering
            popular_items = await db.get_popular_items(content_type, limit=n * 3)
            recommendations = []
            seen_titles: set = set()
            excluded_set_fast: set = set(excluded_set)

            for item in popular_items:
                if item['item_id'] in excluded_set_fast:
                    continue                                  # skip already-shown
                # Normalised title dedup
                import re
                norm = re.sub(r'\s+', ' ',
                    re.sub(r'[^\w\s]', ' ',
                    re.sub(r'^(the|a|an)\s+', '',
                    item['title'].lower().strip()))).strip()
                if norm in seen_titles:
                    continue                                  # skip same-title
                seen_titles.add(norm)
                recommendations.append({
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
                if len(recommendations) >= n:
                    break

        # Log recommendation request
        ab_test_manager.log_experiment_event(
            user_id=user_id,
            experiment_id='recommendation_algorithm_v1',
            event_type='recommendation_request',
            event_data={
                'algorithm': algorithm,
                'n_requested': n,
                'content_type': content_type,
                'n_returned': len(recommendations),
                'excluded_count': len(excluded_set),
                'recommended_item_ids': [r['item_id'] for r in recommendations]
            }
        )

        return RecommendationResponse(
            recommendations=recommendations,
            algorithm=algorithm
        )

    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Search endpoint - Simple text search
@api_router.get("/search")
async def search_items(
    q: str = Query(..., description="Search query"),
    user_id: Optional[str] = Query(None, description="User ID for personalization"),
    content_type: Optional[str] = Query(None, description="Filter by content type"),
    limit: int = Query(20, description="Maximum number of results"),
    search_type: str = Query("simple", description="Search type: simple or ai")
):
    """Search for items using text query"""
    try:
        # Validate content type
        if content_type and content_type not in [ct.value for ct in ContentType]:
            raise HTTPException(status_code=400, detail="Invalid content type")
        
        # Create search request
        search_request = SearchRequest(
            query=q,
            user_id=user_id,
            content_type=ContentType(content_type) if content_type else None,
            limit=min(limit, 100),  # Cap at 100 results
            search_type=search_type
        )
        
        # Execute search
        search_response = await search_engine.search(search_request)
        
        return search_response
        
    except Exception as e:
        logger.error(f"Error in search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Advanced AI search endpoint
@api_router.post("/search/ai", response_model=SearchResponse)
async def ai_search(request: SearchRequest):
    """Advanced AI-powered natural language search"""
    try:
        # Force AI search type
        request.search_type = "ai"
        
        # Execute AI search
        response = await search_engine.search(request)
        
        return response
        
    except Exception as e:
        logger.error(f"Error in AI search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# A/B test endpoint
@api_router.get("/ab/arm")
async def get_ab_test_arm(user_id: str, experiment_id: str = "recommendation_algorithm_v1"):
    """Get A/B test bucket assignment for user"""
    try:
        ab_info = ab_test_manager.get_ab_test_info(user_id, experiment_id)
        
        return ABTestArm(
            user_id=user_id,
            arm=ab_info['arm'],
            experiment_id=experiment_id,
            timestamp=ab_info['timestamp']
        )
        
    except Exception as e:
        logger.error(f"Error getting A/B test arm: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Get user profile endpoint
@api_router.get("/user/{user_id}/profile")
async def get_user_profile(user_id: str):
    """Get user profile and interaction history"""
    try:
        db = await get_db_manager()
        
        # Get user profile
        user_profile = await db.get_user_profile(user_id)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get recent interactions (slice for display)
        interactions = await db.get_user_interactions(user_id, limit=20)
        
        # Get real total count from DB
        total_interactions = await db.db.interactions.count_documents({"user_id": user_id})
        
        return {
            "user_id": user_id,
            "profile": user_profile,
            "recent_interactions": interactions,
            "total_interactions": total_interactions
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Get item details endpoint
@api_router.get("/item/{item_id}")
async def get_item_details(item_id: str):
    """Get detailed information about an item"""
    try:
        db = await get_db_manager()
        
        item_info = await db.db.items.find_one({"item_id": item_id})
        if not item_info:
            raise HTTPException(status_code=404, detail="Item not found")
        
        # Remove non-serializable _id field
        item_info = {k: v for k, v in item_info.items() if k != '_id'}
        
        # Get similar items
        similar_items = await db.get_similar_items(item_id, limit=10)
        
        return {
            "item": item_info,
            "similar_items": similar_items
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting item details: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Popular items endpoint
@api_router.get("/popular")
async def get_popular_items(content_type: Optional[str] = None, limit: int = 20):
    """Get popular/trending items"""
    try:
        # Cap limit to prevent large queries (Bug #13 fix)
        limit = min(limit, 200)
        
        db = await get_db_manager()
        popular_items = await db.get_popular_items(content_type, limit)
        
        return {
            "items": popular_items,
            "content_type": content_type,
            "count": len(popular_items)
        }
        
    except Exception as e:
        logger.error(f"Error getting popular items: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Categories endpoint
@api_router.get("/categories")
async def get_categories():
    """Get available categories by content type"""
    try:
        db = await get_db_manager()
        
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "content_type": "$content_type",
                        "category": "$category"
                    },
                    "item_count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.content_type",
                    "categories": {
                        "$push": {
                            "category": "$_id.category",
                            "item_count": "$item_count"
                        }
                    }
                }
            },
            {
                "$project": {
                    "content_type": "$_id",
                    "categories": {
                        "$slice": [
                            {"$sortArray": {"input": "$categories", "sortBy": {"item_count": -1}}},
                            10
                        ]
                    }
                }
            }
        ]
        
        cursor = db.db.items.aggregate(pipeline)
        results = await cursor.to_list(length=None)
        
        categories = {}
        for result in results:
            categories[result["content_type"]] = result["categories"]
        
        return categories
        
    except Exception as e:
        logger.error(f"Error getting categories: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Statistics endpoint
@api_router.get("/stats")
async def get_system_stats():
    """Get system statistics"""
    try:
        db = await get_db_manager()
        stats = await db.get_system_stats()

        # BUG-15 fix: expose friendly status flags, not raw internal dict
        return {
            "statistics": stats,
            "timestamp": datetime.now(timezone.utc),
            "system_status": {
                "database": app_state["db_initialized"],
                "ml_model": app_state["recommendation_engine_ready"],
                "data_ready": app_state["data_seeded"],
            },
        }
        
    except Exception as e:
        logger.error(f"Error getting system stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# A/B test experiments endpoint
@api_router.get("/experiments")
async def get_experiments():
    """Get all A/B test experiments"""
    return ab_test_manager.get_all_experiments()

# Retrain model endpoint — admin only (Bug #12 fix)
@api_router.post("/admin/retrain")
async def retrain_model(
    background_tasks: BackgroundTasks,
    x_admin_key: Optional[str] = Header(None)
):
    """Retrain the recommendation model (admin endpoint)"""
    # BUG-11 fix: if no key configured, endpoint is entirely disabled
    if ADMIN_API_KEY is None:
        raise HTTPException(status_code=503, detail="Admin endpoint is disabled: ADMIN_API_KEY not configured")
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden: invalid admin API key")
    
    try:
        async def retrain_task():
            rec_engine = await get_recommendation_engine()
            await rec_engine.train_model()
            logger.info("Model retrained successfully")
        
        background_tasks.add_task(retrain_task)
        
        return {
            "status": "success",
            "message": "Model retraining started in background",
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error(f"Error starting model retrain: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── Retrain status endpoint ───────────────────────────────────────────────────
@api_router.get("/retrain/status")
async def retrain_status():
    """Check whether a background model retrain is currently in progress."""
    return {
        "retraining":            app_state['retraining'],
        "interactions_since_last_retrain": app_state['interaction_counter'],
        "retrain_threshold":     RETRAIN_THRESHOLD,
        "progress_pct": round(
            min(app_state['interaction_counter'] / RETRAIN_THRESHOLD * 100, 100), 1
        ),
    }


# ── Manual seed endpoint (triggers real or synthetic seeding) ─────────────────
@api_router.post("/seed")
async def seed_database(
    background_tasks: BackgroundTasks,
    real: bool = False,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    """
    Manually trigger database seeding.
    - real=false → synthetic Faker data (fast, ~5 sec)
    - real=true  → real APIs + MovieLens dataset (slower, ~2-5 min)
    Requires X-Admin-Key header.
    """
    if not ADMIN_API_KEY or x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing admin key")

    async def _seed():
        seeder = DataSeeder()
        if real:
            logger.info("[Seed] Starting real data seeding...")
            await seeder.seed_real_data()
        else:
            logger.info("[Seed] Starting synthetic data seeding...")
            await seeder.seed_all_data()

    background_tasks.add_task(_seed)
    return {
        "status": "started",
        "mode": "real" if real else "synthetic",
        "message": (
            "Real data seeding started — check logs for progress (~2-5 min)"
            if real else
            "Synthetic seeding started — should complete in ~5 seconds"
        ),
    }

app.include_router(api_router)

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Real-Time Recommendation System API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/api/health",
            "recommendations": "/api/recommend",
            "search": "/api/search",
            "ai_search": "/api/search/ai",
            "events": "/api/event",
            "ab_testing": "/api/ab/arm",
            "popular": "/api/popular",
            "stats": "/api/stats"
        }
    }

if __name__ == "__main__":
    import uvicorn, os
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
