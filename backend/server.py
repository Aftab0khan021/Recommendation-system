from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
load_dotenv()
SEED_ON_STARTUP = os.getenv("SEED_ON_STARTUP", "false").lower() == "true"
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
    'data_seeded': False
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
            logger.info("Seeding database (SEED_ON_STARTUP=true)...")
            seeder = DataSeeder()
            await seeder.seed_all_data(users_count=500, items_count=2000, interactions_count=20000)
            app_state['data_seeded'] = True
            logger.info("✅ Database seeded with sample data")
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

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@api_router.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "services": {
            "database": app_state['db_initialized'],
            "recommendation_engine": app_state['recommendation_engine_ready'],
            "data_available": app_state['data_seeded']
        }
    }

# Event logging endpoint
@api_router.post("/event")
async def log_event(event: EventData):
    """Log user interaction events"""
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
        
        return {
            "status": "success",
            "interaction_id": interaction.interaction_id,
            "timestamp": interaction.timestamp
        }
        
    except Exception as e:
        logger.error(f"Error logging event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Recommendations endpoint
@api_router.get("/recommend", response_model=RecommendationResponse)
async def get_recommendations(
    user_id: str,
    n: int = 10,
    content_type: Optional[str] = None,
    exclude_seen: bool = True
):
    """Get personalized recommendations for user"""
    try:
        # Validate parameters
        if n > 100:
            n = 100  # Limit maximum recommendations
        
        # Validate content type
        if content_type and content_type not in [ct.value for ct in ContentType]:
            raise HTTPException(status_code=400, detail="Invalid content type")
        
        # Get A/B test assignment
        should_use_ml = ab_test_manager.should_use_xgboost(user_id)
        algorithm = "xgboost_ml" if should_use_ml else "popularity_based"
        
        # Get recommendations
        rec_engine = await get_recommendation_engine()
        
        if should_use_ml and rec_engine.is_trained:
            recommendations = await rec_engine.get_recommendations(user_id, n, content_type)
        else:
            # Fallback to popularity-based
            db = await get_db_manager()
            popular_items = await db.get_popular_items(content_type, limit=n)
            recommendations = []
            
            for item in popular_items:
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
        # Validate content type
        from models import ContentType  # ensure imported
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
        
        # Get recent interactions
        interactions = await db.get_user_interactions(user_id, limit=20)
        
        return {
            "user_id": user_id,
            "profile": user_profile,
            "recent_interactions": interactions,
            "total_interactions": len(interactions)
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
        
        return {
            "statistics": stats,
            "timestamp": datetime.utcnow(),
            "system_status": app_state
        }
        
    except Exception as e:
        logger.error(f"Error getting system stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# A/B test experiments endpoint
@api_router.get("/experiments")
async def get_experiments():
    """Get all A/B test experiments"""
    return ab_test_manager.get_all_experiments()

# Retrain model endpoint (for admin use)
@api_router.post("/admin/retrain")
async def retrain_model(background_tasks: BackgroundTasks):
    """Retrain the recommendation model (admin endpoint)"""
    try:
        async def retrain_task():
            rec_engine = await get_recommendation_engine()
            await rec_engine.train_model()
            logger.info("Model retrained successfully")
        
        background_tasks.add_task(retrain_task)
        
        return {
            "status": "success",
            "message": "Model retraining started in background",
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error starting model retrain: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Include the API router
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
