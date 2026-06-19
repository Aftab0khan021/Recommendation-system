"""
Phase 3: Prometheus metrics for the recommendation system.

Exposes the following metrics at GET /metrics:
  - recommendation_requests_total      (counter, labels: algorithm, content_type)
  - recommendation_latency_seconds     (histogram)
  - search_requests_total              (counter, labels: search_type)
  - search_latency_seconds             (histogram)
  - cache_hits_total                   (counter, labels: cache_name)
  - cache_misses_total                 (counter, labels: cache_name)
  - model_retrain_duration_seconds     (histogram)
  - ab_test_assignments_total          (counter, labels: experiment, bucket)
  - active_users_gauge                 (gauge)

Usage:
    from metrics import REC_REQUESTS, REC_LATENCY, track_latency
    with track_latency(REC_LATENCY):
        result = await do_work()
    REC_REQUESTS.labels(algorithm="xgboost_ml", content_type="video").inc()
"""
import time
import logging
from contextlib import contextmanager
from typing import Generator

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest
    PROMETHEUS_AVAILABLE = True
    REGISTRY = CollectorRegistry(auto_describe=True)
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.warning(
        "prometheus_client not installed — metrics endpoint will return empty. "
        "Install with: pip install prometheus_client"
    )


def _make_counter(name, doc, labels=()):
    if PROMETHEUS_AVAILABLE:
        return Counter(name, doc, labels)
    return _DummyMetric()


def _make_histogram(name, doc, labels=(), buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10)):
    if PROMETHEUS_AVAILABLE:
        return Histogram(name, doc, labels, buckets=buckets)
    return _DummyMetric()


def _make_gauge(name, doc, labels=()):
    if PROMETHEUS_AVAILABLE:
        return Gauge(name, doc, labels)
    return _DummyMetric()


class _DummyMetric:
    """No-op metric used when prometheus_client is not installed."""
    def labels(self, **_): return self
    def inc(self, *_, **__): pass
    def set(self, *_, **__): pass
    def observe(self, *_, **__): pass
    def time(self): return _DummyCtx()


class _DummyCtx:
    def __enter__(self): return self
    def __exit__(self, *_): pass


# ── Metrics definitions ────────────────────────────────────────────────────────
REC_REQUESTS = _make_counter(
    "recommendation_requests_total",
    "Total number of recommendation requests",
    ["algorithm", "content_type"]
)

REC_LATENCY = _make_histogram(
    "recommendation_latency_seconds",
    "Recommendation request latency in seconds"
)

SEARCH_REQUESTS = _make_counter(
    "search_requests_total",
    "Total number of search requests",
    ["search_type"]
)

SEARCH_LATENCY = _make_histogram(
    "search_latency_seconds",
    "Search request latency in seconds"
)

CACHE_HITS = _make_counter(
    "cache_hits_total",
    "Total cache hits",
    ["cache_name"]
)

CACHE_MISSES = _make_counter(
    "cache_misses_total",
    "Total cache misses",
    ["cache_name"]
)

RETRAIN_DURATION = _make_histogram(
    "model_retrain_duration_seconds",
    "Model retrain duration in seconds",
    buckets=(1, 5, 10, 30, 60, 120, 300)
)

AB_ASSIGNMENTS = _make_counter(
    "ab_test_assignments_total",
    "Total A/B test arm assignments",
    ["experiment", "bucket"]
)

ACTIVE_USERS = _make_gauge(
    "active_users_current",
    "Number of distinct users active in the last 24h"
)

EVENT_LOGS = _make_counter(
    "event_logs_total",
    "Total interaction events logged",
    ["interaction_type"]
)


@contextmanager
def track_latency(histogram) -> Generator:
    """Context manager to track function latency."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        try:
            histogram.observe(elapsed)
        except Exception:
            pass


def get_metrics_output() -> str:
    """Generate Prometheus text format output."""
    if not PROMETHEUS_AVAILABLE:
        return "# prometheus_client not installed\n"
    try:
        return generate_latest().decode("utf-8")
    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        return f"# Error: {e}\n"
