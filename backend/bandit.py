"""
Phase 3: Multi-Armed Bandit with Thompson Sampling for adaptive A/B testing.

Thompson Sampling maintains a Beta(α, β) posterior for each arm.
On each assignment it samples from each arm's posterior and picks the highest.
This naturally shifts traffic toward better-performing arms over time.

Usage:
    bandit = get_bandit("recommendation_algorithm_v1")
    arm = bandit.select_arm(user_id)   # returns "control" or "treatment"
    bandit.record_outcome(arm, reward=1.0)  # call after observing outcome
"""
import hashlib
import logging
import asyncio
from typing import Dict, Optional
import numpy as np

logger = logging.getLogger(__name__)


class ThompsonSamplingBandit:
    """
    Beta-Bernoulli Thompson Sampling bandit.

    Each arm has a Beta(α, β) prior, initially Beta(1, 1) (uniform).
    On select_arm():
        - Sample θₖ ~ Beta(αₖ, βₖ) for each arm k
        - Return arm with highest θₖ
    On record_outcome(arm, reward):
        - If reward > 0: αₖ += reward
        - Else:          βₖ += 1
    """

    def __init__(self, experiment_id: str, arms: list[str], mode: str = "bandit"):
        self.experiment_id = experiment_id
        self.arms = arms
        self.mode = mode   # "bandit" or "ab"  (ab = fixed 50/50)
        # Alpha and beta counters per arm (start at 1,1 — uniform prior)
        self._alpha: Dict[str, float] = {a: 1.0 for a in arms}
        self._beta:  Dict[str, float] = {a: 1.0 for a in arms}
        self._lock = asyncio.Lock()

    async def select_arm(self, user_id: str) -> str:
        """
        Select an arm for user_id.
        In 'bandit' mode: Thompson Sampling.
        In 'ab' mode: deterministic SHA-256 hash split (classic A/B).
        """
        if self.mode == "ab":
            return self._deterministic_arm(user_id)

        async with self._lock:
            samples = {
                arm: np.random.beta(self._alpha[arm], self._beta[arm])
                for arm in self.arms
            }
        return max(samples, key=lambda a: samples[a])

    def _deterministic_arm(self, user_id: str) -> str:
        h = int(hashlib.sha256(f"{user_id}_{self.experiment_id}".encode()).hexdigest(), 16)
        return self.arms[h % len(self.arms)]

    async def record_outcome(self, arm: str, reward: float) -> None:
        """
        Update posterior.
        reward > 0  → positive outcome (click/interaction)
        reward == 0 → negative outcome (no interaction)
        """
        if arm not in self.arms:
            return
        async with self._lock:
            if reward > 0:
                self._alpha[arm] += reward
            else:
                self._beta[arm] += 1.0

    async def get_stats(self) -> Dict:
        async with self._lock:
            stats = {}
            for arm in self.arms:
                a, b = self._alpha[arm], self._beta[arm]
                n = a + b - 2   # total observations (subtract prior)
                mean = a / (a + b)
                stats[arm] = {
                    "alpha": round(a, 2),
                    "beta": round(b, 2),
                    "estimated_ctr": round(mean, 4),
                    "observations": max(0, int(n)),
                    "traffic_pct": round(mean * 100, 1),
                }
        # Normalize traffic_pct to sum to 100
        total = sum(s["traffic_pct"] for s in stats.values()) or 1
        for s in stats.values():
            s["traffic_pct"] = round(s["traffic_pct"] / total * 100, 1)
        return {"experiment_id": self.experiment_id, "mode": self.mode, "arms": stats}


# ── Registry ───────────────────────────────────────────────────────────────────
_bandits: Dict[str, ThompsonSamplingBandit] = {}
_registry_lock = asyncio.Lock()


async def get_bandit(experiment_id: str, arms: Optional[list] = None,
                     mode: str = "bandit") -> ThompsonSamplingBandit:
    """Get or create a bandit for the given experiment."""
    global _bandits
    if experiment_id in _bandits:
        return _bandits[experiment_id]
    async with _registry_lock:
        if experiment_id not in _bandits:
            _bandits[experiment_id] = ThompsonSamplingBandit(
                experiment_id=experiment_id,
                arms=arms or ["control", "treatment"],
                mode=mode
            )
    return _bandits[experiment_id]
