"""
Phase 2: In-memory TTL LRU cache for recommendations, search, and stats.
No external dependencies — pure Python. Thread-safe via asyncio.Lock.

Cache TTLs:
  recommendations : 60s  per user+filter combo
  search results  : 30s  per query+type+filter combo
  popular items   : 300s per content_type
  system stats    : 30s  global

Cache is invalidated for a user on every new interaction logged via /api/event.
"""
import time
import asyncio
import logging
from collections import OrderedDict
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TTLCache:
    """Async-safe LRU cache with per-entry TTL expiration."""

    def __init__(self, maxsize: int = 512, default_ttl: int = 60):
        self._cache: OrderedDict = OrderedDict()
        self._maxsize = maxsize
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            value, expires_at = self._cache[key]
            if time.monotonic() > expires_at:
                del self._cache[key]
                self._misses += 1
                return None
            self._cache.move_to_end(key)   # LRU: mark as recently used
            self._hits += 1
            return value

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        async with self._lock:
            ttl_val = ttl if ttl is not None else self._default_ttl
            expires_at = time.monotonic() + ttl_val
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = (value, expires_at)
            while len(self._cache) > self._maxsize:
                self._cache.popitem(last=False)   # evict oldest

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._cache.pop(key, None)

    async def delete_pattern(self, prefix: str) -> int:
        """Delete all keys starting with prefix. Returns count deleted."""
        async with self._lock:
            keys = [k for k in self._cache if k.startswith(prefix)]
            for k in keys:
                del self._cache[k]
            return len(keys)

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()

    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "maxsize": self._maxsize,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate_pct": round(self._hits / total * 100, 1) if total else 0.0,
        }


# ── Global singleton cache instances ─────────────────────────────────────────
recommendation_cache = TTLCache(maxsize=256, default_ttl=60)     # 60s per user
search_cache         = TTLCache(maxsize=256, default_ttl=30)     # 30s per query
popular_cache        = TTLCache(maxsize=32,  default_ttl=300)    # 5 min
stats_cache          = TTLCache(maxsize=8,   default_ttl=30)     # 30s global


def make_rec_key(user_id: str, content_type: Optional[str], n: int) -> str:
    return f"rec:{user_id}:{content_type or 'all'}:{n}"


def make_search_key(query: str, search_type: str, content_type: Optional[str],
                    limit: int, user_id: Optional[str]) -> str:
    return f"search:{query}:{search_type}:{content_type or 'all'}:{limit}:{user_id or 'anon'}"


def make_popular_key(content_type: Optional[str], limit: int) -> str:
    return f"popular:{content_type or 'all'}:{limit}"
