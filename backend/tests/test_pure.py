"""
test_pure.py — Pure-Python tests with NO external ML dependencies.
These run instantly even without xgboost/sklearn installed.

Covers:
  - Statistical significance (math only)
  - Input sanitization (re only)
  - TTL Cache (asyncio only)
"""
import re
import time
import asyncio
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# 1. Statistical Significance Tests
# ─────────────────────────────────────────────────────────────────────────────

from ab_testing import ABTestManager


class TestStatisticalSignificance:
    def _sig(self, n1, c1, n2, c2):
        return ABTestManager._calculate_significance(n1, c1, n2, c2)

    def test_insufficient_data_returns_not_significant(self):
        result = self._sig(n1=3, c1=1, n2=2, c2=0)
        assert result['is_significant'] is False
        assert result['z_score'] is None

    def test_identical_rates_not_significant(self):
        result = self._sig(n1=100, c1=30, n2=100, c2=30)
        assert result['is_significant'] is False
        assert abs(result['z_score']) < 0.01

    def test_large_difference_is_significant(self):
        result = self._sig(n1=500, c1=50, n2=500, c2=125)
        assert result['is_significant'] is True
        assert result['p_value'] < 0.05
        assert result['lift_pct'] == pytest.approx(150.0, rel=0.05)

    def test_positive_lift(self):
        result = self._sig(n1=100, c1=10, n2=100, c2=20)
        assert result['lift_pct'] == pytest.approx(100.0, rel=0.05)

    def test_p_value_in_range(self):
        result = self._sig(n1=200, c1=40, n2=200, c2=60)
        assert 0 <= result['p_value'] <= 1

    def test_z_score_sign(self):
        result = self._sig(n1=100, c1=10, n2=100, c2=20)
        assert result['z_score'] > 0

    def test_control_ctr_field(self):
        result = self._sig(n1=100, c1=25, n2=100, c2=25)
        assert result['control_ctr'] == pytest.approx(25.0, rel=0.01)

    def test_zero_conversions_control(self):
        result = self._sig(n1=100, c1=0, n2=100, c2=10)
        assert result['lift_pct'] is None   # p1 == 0 → undefined lift


# ─────────────────────────────────────────────────────────────────────────────
# 2. Input Sanitization Tests
# ─────────────────────────────────────────────────────────────────────────────

# _sanitize_regex is a standalone function in search_engine, not a method
from search_engine import _sanitize_regex


class TestInputSanitization:
    def test_regular_input_unchanged_meaning(self):
        raw = "machine learning"
        sanitised = re.escape(raw)
        assert re.search(sanitised, "machine learning") is not None

    def test_dots_are_escaped(self):
        raw = "a.b.c"
        sanitised = re.escape(raw)
        assert re.search(sanitised, "axbxc") is None

    def test_stars_are_escaped(self):
        raw = ".*.*.*"
        sanitised = re.escape(raw)
        assert '\\' in sanitised

    def test_max_length_enforced(self):
        long_query = "a" * 600
        result = _sanitize_regex(long_query)
        assert len(result) <= 512

    def test_empty_string_handled(self):
        result = _sanitize_regex("")
        assert result == ""

    def test_none_returns_empty(self):
        result = _sanitize_regex(None)
        assert result == ""

    def test_special_regex_chars_escaped(self):
        raw = "test[query](special)"
        sanitised = _sanitize_regex(raw)
        re.compile(sanitised)   # must not raise

    def test_injection_attempt_neutralised(self):
        raw = "') OR '1'='1"
        sanitised = _sanitize_regex(raw)
        assert re.search(sanitised, raw) is not None
        assert re.search(sanitised, "OTHER") is None


# ─────────────────────────────────────────────────────────────────────────────
# 3. TTL Cache Tests  (Python 3.10+ asyncio.run per test)
# ─────────────────────────────────────────────────────────────────────────────

from cache import TTLCache


class TestTTLCache:
    def test_basic_get_set(self):
        async def _test():
            cache = TTLCache(maxsize=10, default_ttl=60)
            await cache.set("k1", "v1")
            assert await cache.get("k1") == "v1"
        asyncio.run(_test())

    def test_miss_returns_none(self):
        async def _test():
            cache = TTLCache(maxsize=10, default_ttl=60)
            assert await cache.get("nonexistent") is None
        asyncio.run(_test())

    def test_expired_entry_returns_none(self):
        async def _test():
            cache = TTLCache(maxsize=10, default_ttl=1)
            await cache.set("k", "v", ttl=0.01)
            await asyncio.sleep(0.05)
            assert await cache.get("k") is None
        asyncio.run(_test())

    def test_eviction_at_capacity(self):
        async def _test():
            cache = TTLCache(maxsize=3, default_ttl=60)
            for i in range(5):
                await cache.set(f"k{i}", f"v{i}")
            assert cache.stats()['size'] == 3
        asyncio.run(_test())

    def test_delete_removes_entry(self):
        async def _test():
            cache = TTLCache(maxsize=10, default_ttl=60)
            await cache.set("k", "v")
            await cache.delete("k")
            assert await cache.get("k") is None
        asyncio.run(_test())

    def test_delete_pattern(self):
        async def _test():
            cache = TTLCache(maxsize=10, default_ttl=60)
            await cache.set("rec:user1:all", "v1")
            await cache.set("rec:user1:video", "v2")
            await cache.set("search:query1", "v3")
            deleted = await cache.delete_pattern("rec:user1:")
            assert deleted == 2
            assert cache.stats()['size'] == 1
        asyncio.run(_test())

    def test_hit_rate_tracking(self):
        async def _test():
            cache = TTLCache(maxsize=10, default_ttl=60)
            await cache.set("k", "v")
            await cache.get("k")   # hit
            await cache.get("k")   # hit
            await cache.get("miss")  # miss
            stats = cache.stats()
            assert stats['hits'] == 2
            assert stats['misses'] == 1
            assert stats['hit_rate_pct'] == pytest.approx(66.7, rel=0.05)
        asyncio.run(_test())
