from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone
from enum import Enum
import uuid


def _utcnow() -> datetime:
    """Return a timezone-aware UTC datetime (replaces deprecated datetime.utcnow)."""
    return datetime.now(timezone.utc)


class ContentType(str, Enum):
    VIDEO = "video"
    MOVIE = "movie"
    ARTICLE = "article"
    PRODUCT = "product"
    MUSIC = "music"
    PODCAST = "podcast"
    COURSE = "course"
    GAME = "game"

class InteractionType(str, Enum):
    VIEW = "view"
    LIKE = "like"
    DISLIKE = "dislike"
    SHARE = "share"
    CLICK = "click"
    PURCHASE = "purchase"
    BOOKMARK = "bookmark"
    COMMENT = "comment"

class User(BaseModel):
    user_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    # Bug #21 fix: timezone-aware utcnow
    signup_ts: datetime = Field(default_factory=_utcnow)
    country: str = "US"
    device: str = "web"
    age_group: str = "25-34"
    # Bug #16 fix: mutable default replaced with default_factory
    preferences: List[str] = Field(default_factory=list)
    total_interactions: int = 0

class Item(BaseModel):
    item_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str
    content_type: ContentType
    category: str
    # Bug #16 fix: mutable defaults replaced with default_factory
    tags: List[str] = Field(default_factory=list)
    description: str = ""
    thumbnail_url: str = ""
    publish_ts: datetime = Field(default_factory=_utcnow)
    duration_seconds: Optional[int] = None
    rating: float = 0.0
    view_count: int = 0
    embedding: List[float] = Field(default_factory=list)

class Interaction(BaseModel):
    interaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    item_id: str
    interaction_type: InteractionType
    timestamp: datetime = Field(default_factory=_utcnow)
    dwell_seconds: int = 0
    rating: Optional[float] = None
    # Bug #16 fix: mutable default replaced with default_factory
    context: Dict[str, Any] = Field(default_factory=dict)

class EventData(BaseModel):
    user_id: str
    item_id: str
    type: InteractionType
    ts: datetime = Field(default_factory=_utcnow)
    dwell_seconds: int = 0
    rating: Optional[float] = None
    # Bug #16 fix: mutable default replaced with default_factory
    context: Dict[str, Any] = Field(default_factory=dict)

class RecommendationRequest(BaseModel):
    user_id: str
    n: int = 10
    content_type: Optional[ContentType] = None
    exclude_seen: bool = True

class RecommendationResponse(BaseModel):
    recommendations: List[Dict[str, Any]]
    algorithm: str
    timestamp: datetime = Field(default_factory=_utcnow)

class ABTestArm(BaseModel):
    user_id: str
    arm: str
    experiment_id: str = "recommendation_algorithm_v1"
    timestamp: datetime = Field(default_factory=_utcnow)

class SearchRequest(BaseModel):
    query: str
    user_id: Optional[str] = None
    content_type: Optional[ContentType] = None
    limit: int = 20
    search_type: str = "simple"  # "simple" or "ai"

class SearchResponse(BaseModel):
    results: List[Dict[str, Any]]
    query: str
    search_type: str
    total_results: int
    timestamp: datetime = Field(default_factory=_utcnow)