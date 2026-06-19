"""
Phase 2: Pytest test suite for the Recommendation System.
Tests: statistical significance, input sanitization, batch ranking, TTL cache.
"""
import math
import re
import time
import asyncio
import numpy as np
import pytest


# ────────────────────────────────────────────────────────────────────────────
# 1. Statistical Significance Tests
# ────────────────────────────────────────────────────────────────────────────

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
        # control: 10% CTR, treatment: 25% CTR, 500 samples each
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
        # treatment > control → positive z
        result = self._sig(n1=100, c1=10, n2=100, c2=20)
        assert result['z_score'] > 0

    def test_control_ctr_field(self):
        result = self._sig(n1=100, c1=25, n2=100, c2=25)
        assert result['control_ctr'] == pytest.approx(25.0, rel=0.01)

    def test_zero_conversions_control(self):
        result = self._sig(n1=100, c1=0, n2=100, c2=10)
        assert result['lift_pct'] is None  # p1 == 0 → undefined lift


# ────────────────────────────────────────────────────────────────────────────
# 2. Input Sanitization Tests (ReDoS prevention)
# ────────────────────────────────────────────────────────────────────────────

from search_engine import SearchEngine

_se = SearchEngine.__new__(SearchEngine)


class TestInputSanitization:
    def test_regular_input_unchanged_meaning(self):
        """re.escape should preserve literal characters."""
        raw = "machine learning"
        sanitised = re.escape(raw)
        assert re.search(sanitised, "machine learning") is not None

    def test_dots_are_escaped(self):
        raw = "a.b.c"
        sanitised = re.escape(raw)
        # Should NOT match "axbxc" after escaping
        assert re.search(sanitised, "axbxc") is None

    def test_stars_are_escaped(self):
        raw = ".*.*.*"
        sanitised = re.escape(raw)
        # The escaped pattern should be a literal string
        assert '\\' in sanitised

    def test_max_length_enforced(self):
        long_query = "a" * 600
        # _sanitize_regex should truncate to 512
        result = _se._sanitize_regex(long_query)
        assert len(result) <= 512

    def test_empty_string_handled(self):
        result = _se._sanitize_regex("")
        assert result == ""

    def test_none_returns_empty(self):
        result = _se._sanitize_regex(None)
        assert result == ""

    def test_special_regex_chars_escaped(self):
        raw = "test[query](special)"
        sanitised = _se._sanitize_regex(raw)
        # Should not raise when used as regex pattern
        re.compile(sanitised)

    def test_injection_attempt_neutralised(self):
        raw = "') OR '1'='1"
        sanitised = _se._sanitize_regex(raw)
        # The result, when used as regex, should be a literal match
        assert re.search(sanitised, raw) is not None  # matches literal string
        assert re.search(sanitised, "OTHER") is None  # does not match anything else


# ────────────────────────────────────────────────────────────────────────────
# 3. Feature Extraction Tests (Phase 2 features)
# ────────────────────────────────────────────────────────────────────────────

from recommendation_engine import RecommendationEngine
from datetime import datetime, timezone


class TestFeatureExtraction:
    def setup_method(self):
        self.engine = RecommendationEngine.__new__(RecommendationEngine)
        # Minimal user profile
        self.engine.user_profiles = {
            'user1': {
                'interaction_count': 10,
                'avg_dwell_time': 45.0,
                'avg_rating': 3.5,
                'categories': {'science': 5, 'tech': 3},
                'content_types': {'video': 7, 'article': 2},
                'tags': {'python': 4, 'ml': 3},
                'demographics': {'age_group': '25-34', 'device': 'web'},
            }
        }

    def _make_interaction(self, **kwargs):
        defaults = {
            'user_id': 'user1',
            'dwell_seconds': 30,
            'item_info': {
                'item_id': 'item1',
                'title': 'Test Item',
                'content_type': 'video',
                'category': 'science',
                'tags': ['python', 'ml'],
                'rating': 4.2,
                'view_count': 1000,
                'publish_ts': datetime.now(timezone.utc),
            }
        }
        defaults.update(kwargs)
        return defaults

    def test_returns_list(self):
        interaction = self._make_interaction()
        features = self.engine.extract_features(interaction, True)
        assert isinstance(features, list)

    def test_feature_count(self):
        """Should return 32 features: 8 original + 7 new + 8 content_types + 5 age + 4 device"""
        interaction = self._make_interaction()
        features = self.engine.extract_features(interaction, True)
        assert len(features) == 32

    def test_freshness_recent_item(self):
        """Fresh item should have freshness close to 1."""
        interaction = self._make_interaction()
        features = self.engine.extract_features(interaction, True)
        freshness = features[8]  # index 8 = item_freshness
        assert 0.9 < freshness <= 1.0

    def test_freshness_old_item(self):
        """Old item (1 year old) should have low freshness."""
        from datetime import timedelta
        old_ts = datetime.now(timezone.utc) - timedelta(days=365)
        interaction = self._make_interaction()
        interaction['item_info']['publish_ts'] = old_ts
        features = self.engine.extract_features(interaction, True)
        freshness = features[8]
        assert freshness < 0.05

    def test_category_affinity_non_zero(self):
        """User has interactions in 'science' category."""
        interaction = self._make_interaction()
        features = self.engine.extract_features(interaction, True)
        category_affinity = features[6]
        assert category_affinity > 0

    def test_age_group_encoding(self):
        """25-34 age group → second position in age one-hot = 1."""
        interaction = self._make_interaction()
        features = self.engine.extract_features(interaction, True)
        # Age one-hot starts at index 15 + 8 = 23
        # ['18-24', '25-34', '35-44', '45-54', '55+'] → '25-34' at index 1
        age_start = 8 + 7 + 8  # 23
        assert features[age_start] == 0.0   # 18-24
        assert features[age_start + 1] == 1.0  # 25-34

    def test_no_exception_missing_profile(self):
        """Missing user profile should not raise — returns features with zeros."""
        interaction = self._make_interaction(user_id='unknown_user')
        features = self.engine.extract_features(interaction, True)
        assert features is not None
        assert len(features) == 32


# ────────────────────────────────────────────────────────────────────────────
# 4. TTL Cache Tests
# ────────────────────────────────────────────────────────────────────────────

from cache import TTLCache


class TestTTLCache:
    def test_basic_get_set(self):
        cache = TTLCache(maxsize=10, default_ttl=60)
        asyncio.get_event_loop().run_until_complete(cache.set("k1", "v1"))
        val = asyncio.get_event_loop().run_until_complete(cache.get("k1"))
        assert val == "v1"

    def test_miss_returns_none(self):
        cache = TTLCache(maxsize=10, default_ttl=60)
        val = asyncio.get_event_loop().run_until_complete(cache.get("nonexistent"))
        assert val is None

    def test_expired_entry_returns_none(self):
        cache = TTLCache(maxsize=10, default_ttl=1)
        asyncio.get_event_loop().run_until_complete(cache.set("k", "v", ttl=0.01))
        time.sleep(0.05)
        val = asyncio.get_event_loop().run_until_complete(cache.get("k"))
        assert val is None

    def test_eviction_at_capacity(self):
        cache = TTLCache(maxsize=3, default_ttl=60)
        loop = asyncio.get_event_loop()
        for i in range(5):
            loop.run_until_complete(cache.set(f"k{i}", f"v{i}"))
        # Only 3 entries should remain
        assert cache.stats()['size'] == 3

    def test_delete_removes_entry(self):
        cache = TTLCache(maxsize=10, default_ttl=60)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cache.set("k", "v"))
        loop.run_until_complete(cache.delete("k"))
        val = loop.run_until_complete(cache.get("k"))
        assert val is None

    def test_delete_pattern(self):
        cache = TTLCache(maxsize=10, default_ttl=60)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cache.set("rec:user1:all", "v1"))
        loop.run_until_complete(cache.set("rec:user1:video", "v2"))
        loop.run_until_complete(cache.set("search:query1", "v3"))
        deleted = loop.run_until_complete(cache.delete_pattern("rec:user1:"))
        assert deleted == 2
        assert cache.stats()['size'] == 1

    def test_hit_rate_tracking(self):
        cache = TTLCache(maxsize=10, default_ttl=60)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cache.set("k", "v"))
        loop.run_until_complete(cache.get("k"))
        loop.run_until_complete(cache.get("k"))
        loop.run_until_complete(cache.get("miss"))
        stats = cache.stats()
        assert stats['hits'] == 2
        assert stats['misses'] == 1
        assert stats['hit_rate_pct'] == pytest.approx(66.7, rel=0.05)


# ────────────────────────────────────────────────────────────────────────────
# 5. Vector Index Tests
# ────────────────────────────────────────────────────────────────────────────

from vector_index import VectorIndex


class TestVectorIndex:
    def setup_method(self):
        from sklearn.feature_extraction.text import TfidfVectorizer
        docs = [
            "machine learning recommendation system",
            "cooking recipes food",
            "python programming tutorial",
            "movie film cinema action",
        ]
        self.vectorizer = TfidfVectorizer()
        self.matrix = self.vectorizer.fit_transform(docs).toarray()
        self.item_ids = ["item_ml", "item_cook", "item_python", "item_movie"]

    def test_rebuild_sets_ready(self):
        vi = VectorIndex()
        asyncio.get_event_loop().run_until_complete(
            vi.rebuild(self.matrix, self.item_ids, self.vectorizer)
        )
        assert vi.is_ready is True

    def test_query_returns_relevant_item(self):
        vi = VectorIndex()
        asyncio.get_event_loop().run_until_complete(
            vi.rebuild(self.matrix, self.item_ids, self.vectorizer)
        )
        results = vi.query("machine learning recommendation", top_k=4)
        top_ids = [r[0] for r in results]
        assert "item_ml" in top_ids[:1]

    def test_query_returns_at_most_top_k(self):
        vi = VectorIndex()
        asyncio.get_event_loop().run_until_complete(
            vi.rebuild(self.matrix, self.item_ids, self.vectorizer)
        )
        results = vi.query("movie film", top_k=2)
        assert len(results) <= 2

    def test_query_not_ready_returns_empty(self):
        vi = VectorIndex()
        results = vi.query("anything")
        assert results == []
