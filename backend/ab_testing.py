import hashlib
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)

# Module-level lock so singleton initialisation is safe under concurrency
_db_lock = asyncio.Lock()
_db_ready = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ABTestManager:
    def __init__(self):
        self.experiments = {
            'recommendation_algorithm_v1': {
                'name': 'ML vs Popularity Algorithm Test',
                'description': 'Comparing XGBoost ML recommendations vs popularity-based recommendations',
                'traffic_split': 0.5,   # 50% traffic to treatment (ML)
                'control_arm': 'popularity_based',
                'treatment_arm': 'xgboost_ml',
                'start_date': _utcnow(),
                'status': 'active'
            }
        }
        # In-memory cache for fast lookups; backed by MongoDB for persistence
        self._assignment_cache: Dict[str, Dict[str, str]] = {}
        # In-memory event buffer per experiment (capped at 1 000 per experiment)
        self._event_buffer: Dict[str, list] = defaultdict(list)
        # MongoDB collection references — populated by _ensure_db()
        self._assignments_col = None
        self._events_col = None

    # ------------------------------------------------------------------
    # Lazy DB initialisation (called on first write to avoid import loops)
    # ------------------------------------------------------------------
    async def _ensure_db(self):
        """Initialise MongoDB collection references once."""
        global _db_ready
        if _db_ready:
            return
        async with _db_lock:
            if _db_ready:
                return
            try:
                from database import get_db_manager
                db_mgr = await get_db_manager()
                self._assignments_col = db_mgr.db.ab_assignments
                self._events_col = db_mgr.db.ab_events
                # Ensure indexes
                await self._assignments_col.create_index(
                    [("user_id", 1), ("experiment_id", 1)], unique=True
                )
                await self._events_col.create_index([("experiment_id", 1), ("timestamp", -1)])
                _db_ready = True
                logger.info("A/B test DB storage initialised")
            except Exception as e:
                logger.error(f"Failed to initialise A/B test DB storage: {e}")

    # ------------------------------------------------------------------
    # Bucket assignment — deterministic + persisted
    # ------------------------------------------------------------------
    def _compute_bucket(self, user_id: str, experiment_id: str) -> str:
        """Pure computation — hash-based deterministic bucket (Bug #22 fix: sha256 not md5)."""
        hash_input = f"{user_id}_{experiment_id}".encode('utf-8')
        # Bug #22 fix: use sha256 instead of MD5
        hash_value = int(hashlib.sha256(hash_input).hexdigest(), 16)
        experiment = self.experiments.get(experiment_id)
        if not experiment:
            return 'control'
        return 'treatment' if (hash_value % 100) < (experiment['traffic_split'] * 100) else 'control'

    def get_user_bucket(self, user_id: str, experiment_id: str) -> str:
        """
        Return bucket for user — checks in-memory cache first, then falls back to
        deterministic computation.  Persistence to MongoDB happens asynchronously
        via log_experiment_event so we never block a synchronous caller.
        """
        # In-memory cache hit
        if user_id in self._assignment_cache and experiment_id in self._assignment_cache[user_id]:
            return self._assignment_cache[user_id][experiment_id]

        bucket = self._compute_bucket(user_id, experiment_id)

        # Store in local cache
        if user_id not in self._assignment_cache:
            self._assignment_cache[user_id] = {}
        self._assignment_cache[user_id][experiment_id] = bucket

        return bucket

    async def get_user_bucket_async(self, user_id: str, experiment_id: str) -> str:
        """
        Async version: checks DB for existing assignment so server restarts don't
        re-assign users to different buckets (Bug #9 fix).
        """
        await self._ensure_db()

        # Check in-memory cache first
        if user_id in self._assignment_cache and experiment_id in self._assignment_cache[user_id]:
            return self._assignment_cache[user_id][experiment_id]

        # Check MongoDB for a previously persisted assignment
        if self._assignments_col is not None:
            try:
                doc = await self._assignments_col.find_one(
                    {"user_id": user_id, "experiment_id": experiment_id}
                )
                if doc:
                    bucket = doc["bucket"]
                    if user_id not in self._assignment_cache:
                        self._assignment_cache[user_id] = {}
                    self._assignment_cache[user_id][experiment_id] = bucket
                    return bucket
            except Exception as e:
                logger.warning(f"Could not read AB assignment from DB: {e}")

        # New user — compute deterministically and persist
        bucket = self._compute_bucket(user_id, experiment_id)
        if user_id not in self._assignment_cache:
            self._assignment_cache[user_id] = {}
        self._assignment_cache[user_id][experiment_id] = bucket

        if self._assignments_col is not None:
            try:
                await self._assignments_col.update_one(
                    {"user_id": user_id, "experiment_id": experiment_id},
                    {"$setOnInsert": {
                        "user_id": user_id,
                        "experiment_id": experiment_id,
                        "bucket": bucket,
                        "assigned_at": _utcnow()
                    }},
                    upsert=True
                )
            except Exception as e:
                logger.warning(f"Could not persist AB assignment to DB: {e}")

        return bucket

    def should_use_xgboost(self, user_id: str) -> bool:
        """Determine if user should get XGBoost recommendations."""
        bucket = self.get_user_bucket(user_id, 'recommendation_algorithm_v1')
        return bucket == 'treatment'

    def get_ab_test_info(self, user_id: str, experiment_id: str) -> Dict[str, Any]:
        """Get A/B test information for user."""
        bucket = self.get_user_bucket(user_id, experiment_id)
        experiment = self.experiments.get(experiment_id, {})

        arm = experiment.get('treatment_arm', 'treatment') if bucket == 'treatment' \
            else experiment.get('control_arm', 'control')

        return {
            'user_id': user_id,
            'experiment_id': experiment_id,
            'bucket': bucket,
            'arm': arm,
            'experiment_name': experiment.get('name', 'Unknown Experiment'),
            'timestamp': _utcnow()
        }

    # ------------------------------------------------------------------
    # Event logging — persisted to MongoDB (Bug #10 fix)
    # ------------------------------------------------------------------
    def log_experiment_event(self, user_id: str, experiment_id: str,
                             event_type: str, event_data: Optional[Dict[str, Any]] = None):
        """
        Log experiment event synchronously into in-memory buffer, then fire-and-forget
        to MongoDB so the HTTP response is not blocked.
        """
        try:
            bucket = self.get_user_bucket(user_id, experiment_id)

            event = {
                'timestamp': _utcnow(),
                'user_id': user_id,
                'experiment_id': experiment_id,
                'bucket': bucket,
                'event_type': event_type,
                'event_data': event_data or {}
            }

            # In-memory buffer (capped at 1 000 per experiment)
            buf = self._event_buffer[experiment_id]
            buf.append(event)
            if len(buf) > 1000:
                self._event_buffer[experiment_id] = buf[-1000:]

            # Persist to MongoDB asynchronously without blocking the caller
            # HIGH-1 fix: get_event_loop() is deprecated in 3.10+, raises in 3.12+
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._persist_event(event))
            except RuntimeError:
                pass  # No running event loop — skip async persist

        except Exception as e:
            logger.error(f"Error logging experiment event: {e}")

    async def _persist_event(self, event: Dict[str, Any]):
        """Write a single event document to MongoDB."""
        await self._ensure_db()
        if self._events_col is None:
            return
        try:
            await self._events_col.insert_one(event)
        except Exception as e:
            logger.warning(f"Could not persist AB event to DB: {e}")

    # ------------------------------------------------------------------
    # Metrics — reads from in-memory buffer (fast) or DB (on restart)
    # ------------------------------------------------------------------
    def get_experiment_metrics(self, experiment_id: str) -> Dict[str, Any]:
        """Get experiment performance metrics from in-memory buffer."""
        try:
            events = self._event_buffer.get(experiment_id, [])

            if not events:
                return {"message": "No in-memory events yet — metrics refresh after activity"}

            control_events = [e for e in events if e['bucket'] == 'control']
            treatment_events = [e for e in events if e['bucket'] == 'treatment']

            metrics = {
                'experiment_id': experiment_id,
                'total_events': len(events),
                'control_events': len(control_events),
                'treatment_events': len(treatment_events),
                'control_users': len({e['user_id'] for e in control_events}),
                'treatment_users': len({e['user_id'] for e in treatment_events}),
            }

            control_interactions = sum(
                1 for e in control_events if e['event_type'].startswith('interaction_'))
            treatment_interactions = sum(
                1 for e in treatment_events if e['event_type'].startswith('interaction_'))

            control_requests = sum(
                1 for e in control_events if e['event_type'] == 'recommendation_request')
            treatment_requests = sum(
                1 for e in treatment_events if e['event_type'] == 'recommendation_request')

            metrics['control_interaction_rate'] = (
                control_interactions / control_requests if control_requests > 0 else 0
            )
            metrics['treatment_interaction_rate'] = (
                treatment_interactions / treatment_requests if treatment_requests > 0 else 0
            )

            return metrics

        except Exception as e:
            logger.error(f"Error calculating experiment metrics: {e}")
            return {"error": str(e)}

    def get_all_experiments(self) -> Dict[str, Any]:
        """Get all experiment configurations and metrics."""
        result = {}
        for exp_id, exp_config in self.experiments.items():
            result[exp_id] = {
                'config': exp_config,
                'metrics': self.get_experiment_metrics(exp_id)
            }
        return result


# Global A/B test manager instance
ab_test_manager = ABTestManager()