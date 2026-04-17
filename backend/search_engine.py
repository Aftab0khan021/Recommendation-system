import logging
import re
from typing import List, Dict, Any, Optional
from models import ContentType, SearchRequest, SearchResponse
from database import get_db_manager

logger = logging.getLogger(__name__)

class SearchEngine:
    def __init__(self):
        self.search_keywords = {
            'video': ['video', 'watch', 'clip', 'movie', 'film', 'show'],
            'movie': ['movie', 'film', 'cinema', 'blockbuster', 'hollywood'],
            'article': ['article', 'read', 'blog', 'news', 'story', 'post'],
            'product': ['product', 'buy', 'purchase', 'shop', 'item', 'goods'],
            'music': ['music', 'song', 'track', 'album', 'artist', 'band'],
            'podcast': ['podcast', 'listen', 'episode', 'show', 'audio'],
            'course': ['course', 'learn', 'tutorial', 'lesson', 'class', 'education'],
            'game': ['game', 'play', 'gaming', 'entertainment', 'fun']
        }

        self.intent_patterns = {
            'similar_to': [
                r'similar to (.+)',
                r'like (.+)',
                r'related to (.+)',
                r'comparable to (.+)'
            ],
            'category_filter': [
                r'(.+) (video|movie|article|product|music|podcast|course|game)s?',
                r'(video|movie|article|product|music|podcast|course|game)s? about (.+)',
                r'show me (.+) content'
            ],
            'recommendation_request': [
                r'recommend (.+)',
                r'suggest (.+)',
                r'find me (.+)',
                r'i want (.+)',
                r'looking for (.+)'
            ],
            'educational': [
                r'learn (.+)',
                r'how to (.+)',
                r'tutorial (.+)',
                r'guide (.+)'
            ],
            'trending': [
                r'trending (.+)',
                r'popular (.+)',
                r'hot (.+)',
                r'viral (.+)'
            ]
        }

    async def search(self, request: SearchRequest) -> SearchResponse:
        """Main search function that handles both simple and AI search"""
        try:
            if request.search_type == "ai":
                return await self.ai_search(request)
            else:
                return await self.simple_search(request)
        except Exception as e:
            logger.error(f"Error in search: {e}")
            return SearchResponse(
                results=[],
                query=request.query,
                search_type=request.search_type,
                total_results=0
            )

    async def simple_search(self, request: SearchRequest) -> SearchResponse:
        """Simple text-based search"""
        try:
            db = await get_db_manager()

            results = await db.search_items(
                query=request.query,
                content_type=request.content_type.value if request.content_type else None,
                limit=request.limit
            )

            formatted_results = []
            for item in results:
                formatted_results.append({
                    'item_id': item['item_id'],
                    'title': item['title'],
                    'content_type': item['content_type'],
                    'category': item['category'],
                    'description': item['description'],
                    'thumbnail_url': item['thumbnail_url'],
                    'rating': item['rating'],
                    'view_count': item['view_count'],
                    'tags': item.get('tags', []),
                    'ml_score': 0.0,
                    'search_score': item.get('score', 1.0)
                })

            return SearchResponse(
                results=formatted_results,
                query=request.query,
                search_type="simple",
                total_results=len(formatted_results)
            )

        except Exception as e:
            logger.error(f"Error in simple search: {e}")
            return SearchResponse(
                results=[],
                query=request.query,
                search_type="simple",
                total_results=0
            )

    async def ai_search(self, request: SearchRequest) -> SearchResponse:
        """AI-powered natural language search"""
        try:
            query = request.query.lower().strip()

            intent = self.parse_intent(query)

            if intent['type'] == 'similar_to':
                results = await self.find_similar_content(intent['target'], request)
            elif intent['type'] == 'category_filter':
                category = intent.get('category') or intent.get('target') or ""
                results = await self.search_by_category(category, intent.get('content_type'), request)
            elif intent['type'] == 'recommendation_request':
                results = await self.get_personalized_recommendations(intent['target'], request)
            elif intent['type'] == 'educational':
                results = await self.search_educational_content(intent['target'], request)
            elif intent['type'] == 'trending':
                results = await self.get_trending_content(intent['target'], request)
            else:
                results = await self.enhanced_simple_search(query, request)

            return SearchResponse(
                results=results,
                query=request.query,
                search_type="ai",
                total_results=len(results)
            )

        except Exception as e:
            logger.error(f"Error in AI search: {e}")
            return SearchResponse(
                results=[],
                query=request.query,
                search_type="ai",
                total_results=0
            )

    def parse_intent(self, query: str) -> Dict[str, Any]:
        """Parse user intent from natural language query (Bug #17 fix)."""
        intent: Dict[str, Any] = {'type': 'general', 'confidence': 0.0, 'target': query}

        # Check each intent pattern — return immediately on first match so
        # the target set inside the loop is NOT overwritten (Bug #17 fix).
        for intent_type, patterns in self.intent_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    intent['type'] = intent_type
                    intent['confidence'] = 0.8
                    intent['target'] = match.group(1).strip()
                    if intent_type == 'category_filter':
                        intent['category'] = match.group(1).strip()
                    # Extract content type hint if present in query
                    for content_type, keywords in self.search_keywords.items():
                        if any(keyword in query for keyword in keywords):
                            intent['content_type'] = content_type
                            break
                    return intent   # ← early return prevents unconditional overwrite

        # No specific pattern matched — try to extract a content type hint
        for content_type, keywords in self.search_keywords.items():
            if any(keyword in query for keyword in keywords):
                intent['content_type'] = content_type
                break

        # intent['target'] already set to `query` in the initialiser above
        return intent

    async def find_similar_content(self, target: str, request: SearchRequest) -> List[Dict[str, Any]]:
        """Find content similar to the specified target"""
        try:
            db = await get_db_manager()

            target_items = await db.search_items(target, limit=5)

            if not target_items:
                return []

            similar_items = []
            for target_item in target_items:
                similar = await db.get_similar_items(target_item['item_id'], limit=10)
                similar_items.extend(similar)

            seen_ids: set = set()
            results = []

            for item in similar_items:
                if item['item_id'] not in seen_ids:
                    seen_ids.add(item['item_id'])
                    results.append(self.format_search_result(item, score=0.8))

            return results[:request.limit]

        except Exception as e:
            logger.error(f"Error finding similar content: {e}")
            return []

    async def search_by_category(self, category: str, content_type: Optional[str], request: SearchRequest) -> List[Dict[str, Any]]:
        """Search by specific category and content type"""
        try:
            db = await get_db_manager()

            match_filter: Dict[str, Any] = {}

            if content_type:
                match_filter['content_type'] = content_type
            elif request.content_type:
                match_filter['content_type'] = request.content_type.value

            category_items = await db.db.items.find({
                **match_filter,
                'category': {'$regex': category, '$options': 'i'}
            }).sort([('view_count', -1), ('rating', -1)]).limit(request.limit).to_list(length=request.limit)

            if not category_items:
                category_items = await db.search_items(category, content_type, request.limit)

            results = []
            for item in category_items:
                clean = {k: v for k, v in item.items() if k != '_id'}
                results.append(self.format_search_result(clean, score=0.9))

            return results

        except Exception as e:
            logger.error(f"Error searching by category: {e}")
            return []

    async def get_personalized_recommendations(self, target: str, request: SearchRequest) -> List[Dict[str, Any]]:
        """Get personalized recommendations based on target"""
        try:
            if not request.user_id:
                return await self.get_trending_content(target, request)

            from recommendation_engine import get_recommendation_engine

            rec_engine = await get_recommendation_engine()

            recommendations = await rec_engine.get_recommendations(
                user_id=request.user_id,
                n=request.limit * 2,
                content_type=request.content_type.value if request.content_type else None
            )

            target_keywords = target.lower().split()
            filtered_results = []

            for rec in recommendations:
                text_to_search = f"{rec['title']} {rec['description']} {rec['category']} {' '.join(rec.get('tags', []))}".lower()
                if any(keyword in text_to_search for keyword in target_keywords):
                    filtered_results.append(rec)

            return filtered_results[:request.limit]

        except Exception as e:
            logger.error(f"Error getting personalized recommendations: {e}")
            return []

    async def search_educational_content(self, target: str, request: SearchRequest) -> List[Dict[str, Any]]:
        """Search for educational content"""
        try:
            db = await get_db_manager()

            educational_types = ['course', 'article', 'video', 'podcast']

            match_filter: Dict[str, Any] = {
                'content_type': {'$in': educational_types}
            }

            results = await db.db.items.find({
                **match_filter,
                '$or': [
                    {'title': {'$regex': target, '$options': 'i'}},
                    {'description': {'$regex': target, '$options': 'i'}},
                    {'category': {'$regex': target, '$options': 'i'}},
                    # Bug #11 fix: use $elemMatch for tag regex
                    {'tags': {'$elemMatch': {'$regex': target, '$options': 'i'}}}
                ]
            }).sort([('rating', -1), ('view_count', -1)]).limit(request.limit).to_list(length=request.limit)

            formatted_results = []
            for item in results:
                clean = {k: v for k, v in item.items() if k != '_id'}
                formatted_results.append(self.format_search_result(clean, score=0.85))

            return formatted_results

        except Exception as e:
            logger.error(f"Error searching educational content: {e}")
            return []

    async def get_trending_content(self, target: str, request: SearchRequest) -> List[Dict[str, Any]]:
        """Get trending/popular content related to target"""
        try:
            db = await get_db_manager()

            match_filter: Dict[str, Any] = {}
            if request.content_type:
                match_filter['content_type'] = request.content_type.value

            if target and target.strip():
                match_filter['$or'] = [
                    {'title': {'$regex': target, '$options': 'i'}},
                    {'description': {'$regex': target, '$options': 'i'}},
                    {'category': {'$regex': target, '$options': 'i'}},
                    # Bug #11 fix: use $elemMatch for tag regex
                    {'tags': {'$elemMatch': {'$regex': target, '$options': 'i'}}}
                ]

            results = await db.db.items.find(match_filter).sort([
                ('view_count', -1),
                ('rating', -1)
            ]).limit(request.limit).to_list(length=request.limit)

            formatted_results = []
            for item in results:
                clean = {k: v for k, v in item.items() if k != '_id'}
                formatted_results.append(self.format_search_result(clean, score=0.9))

            return formatted_results

        except Exception as e:
            logger.error(f"Error getting trending content: {e}")
            return []

    async def enhanced_simple_search(self, query: str, request: SearchRequest) -> List[Dict[str, Any]]:
        """Enhanced simple search with better ranking"""
        try:
            db = await get_db_manager()

            results = await db.search_items(
                query=query,
                content_type=request.content_type.value if request.content_type else None,
                limit=request.limit
            )

            enhanced_results = []
            query_words = query.lower().split()

            for item in results:
                score = item.get('score', 1.0)

                title_matches = sum(1 for word in query_words if word in item['title'].lower())
                score += title_matches * 0.2

                if any(word in item['category'].lower() for word in query_words):
                    score += 0.3

                item_tags = item.get('tags', [])
                tag_matches = sum(1 for word in query_words for tag in item_tags if word in tag.lower())
                score += tag_matches * 0.1

                enhanced_results.append(self.format_search_result(item, score=score))

            enhanced_results.sort(key=lambda x: x['search_score'], reverse=True)

            return enhanced_results

        except Exception as e:
            logger.error(f"Error in enhanced simple search: {e}")
            return []

    def format_search_result(self, item: Dict[str, Any], score: float = 1.0) -> Dict[str, Any]:
        """Format item as search result"""
        return {
            'item_id': item['item_id'],
            'title': item['title'],
            'content_type': item['content_type'],
            'category': item['category'],
            'description': item['description'],
            'thumbnail_url': item['thumbnail_url'],
            'rating': item['rating'],
            'view_count': item['view_count'],
            'tags': item.get('tags', []),
            'ml_score': 0.0,
            'search_score': score
        }


# Global search engine instance
search_engine = SearchEngine()