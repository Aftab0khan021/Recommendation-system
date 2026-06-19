"""
Phase 3: Collaborative Filtering via Alternating Least Squares (ALS).

Uses scipy's sparse matrix operations — no additional ML library needed.
Generates 64-dimensional latent user and item vectors from the interaction matrix.

Score blending (in recommendation_engine.py):
    final = 0.4·xgb_score + 0.4·als_score + 0.2·popularity_score
"""
import logging
import asyncio
import numpy as np
from typing import Dict, List, Optional, Any
from scipy.sparse import csr_matrix
from scipy.sparse.linalg import svds

logger = logging.getLogger(__name__)

_cf_lock = asyncio.Lock()


class CollaborativeFilter:
    """
    Implicit ALS-style matrix factorization using truncated SVD.

    We treat every interaction as a positive signal with confidence
    weighted by interaction type:
        purchase / like / bookmark → weight 4
        view (dwell > 30s)        → weight 2
        view (dwell <= 30s)       → weight 1
        click                     → weight 1

    SVD decomposes the confidence matrix C into U·Σ·Vᵀ.
    User vectors = U·√Σ  (n_users × n_factors)
    Item vectors = Vᵀ·√Σ  (n_items × n_factors)
    Score(u, i)  = user_vec[u] · item_vec[i]
    """

    N_FACTORS = 64

    def __init__(self):
        self.user_vectors: Optional[np.ndarray] = None   # (n_users, N_FACTORS)
        self.item_vectors: Optional[np.ndarray] = None   # (n_items, N_FACTORS)
        self.user_index: Dict[str, int] = {}             # user_id → row index
        self.item_index: Dict[str, int] = {}             # item_id → col index
        self.item_ids: List[str] = []                    # col index → item_id
        self.is_trained = False

    async def train(self) -> None:
        """Build interaction matrix and compute SVD factorization."""
        try:
            from database import get_db_manager
            db = await get_db_manager()

            # Fetch all interactions with type + dwell
            cursor = db.db.interactions.find(
                {},
                {"user_id": 1, "item_id": 1, "interaction_type": 1, "dwell_seconds": 1}
            )
            interactions = await cursor.to_list(length=None)

            if not interactions:
                logger.warning("[ALS] No interactions found — skipping CF training")
                return

            # Build user/item index maps
            users = sorted({i["user_id"] for i in interactions})
            items = sorted({i["item_id"] for i in interactions})
            user_idx = {u: i for i, u in enumerate(users)}
            item_idx = {it: i for i, it in enumerate(items)}

            # Build sparse confidence matrix
            WEIGHTS = {
                "purchase": 4.0, "like": 4.0, "bookmark": 4.0,
                "view": 2.0, "click": 1.0, "share": 1.0,
                "dislike": -1.0, "comment": 1.5,
            }

            rows, cols, data = [], [], []
            for inter in interactions:
                u = user_idx.get(inter["user_id"])
                it = item_idx.get(inter["item_id"])
                if u is None or it is None:
                    continue
                w = WEIGHTS.get(inter.get("interaction_type", "view"), 1.0)
                # Boost view weight for long dwell times
                if inter.get("interaction_type") == "view":
                    dwell = inter.get("dwell_seconds", 0) or 0
                    if dwell > 60:
                        w = 3.0
                    elif dwell > 30:
                        w = 2.0
                rows.append(u)
                cols.append(it)
                data.append(max(w, 0.0))

            n_users, n_items = len(users), len(items)
            if n_users < 2 or n_items < 2:
                logger.warning("[ALS] Too few users/items for CF — skipping")
                return

            C = csr_matrix((data, (rows, cols)), shape=(n_users, n_items), dtype=np.float32)

            # Truncated SVD (k = min(N_FACTORS, min_dim-1))
            k = min(self.N_FACTORS, min(n_users, n_items) - 1)
            if k < 1:
                return

            U, sigma, Vt = svds(C, k=k)
            sqrt_sigma = np.sqrt(np.abs(sigma))  # shape (k,)

            async with _cf_lock:
                self.user_vectors = U * sqrt_sigma          # (n_users, k)
                self.item_vectors = (Vt * sqrt_sigma[:, None]).T  # (n_items, k)
                self.user_index = user_idx
                self.item_index = item_idx
                self.item_ids = items
                self.is_trained = True

            logger.info(
                f"[ALS] Trained: {n_users} users × {n_items} items, {k} factors"
            )

        except Exception as e:
            logger.error(f"[ALS] Training failed: {e}")

    def score_items(self, user_id: str, candidate_ids: List[str]) -> Dict[str, float]:
        """
        Return ALS scores for candidate items for a given user.
        Returns empty dict if user is unknown or model not trained.
        Scores are normalised to [0, 1].
        """
        if not self.is_trained:
            return {}

        u_idx = self.user_index.get(user_id)
        if u_idx is None:
            return {}

        u_vec = self.user_vectors[u_idx]  # (k,)

        scores: Dict[str, float] = {}
        item_indices = []
        found_ids = []

        for item_id in candidate_ids:
            i_idx = self.item_index.get(item_id)
            if i_idx is not None:
                item_indices.append(i_idx)
                found_ids.append(item_id)

        if not item_indices:
            return {}

        # Batch dot product: (n_found, k) × (k,) → (n_found,)
        item_vecs = self.item_vectors[item_indices]   # (n_found, k)
        raw_scores = item_vecs @ u_vec                # (n_found,)

        # Normalize to [0, 1]
        s_min, s_max = raw_scores.min(), raw_scores.max()
        if s_max > s_min:
            norm = (raw_scores - s_min) / (s_max - s_min)
        else:
            norm = np.zeros_like(raw_scores)

        for item_id, score in zip(found_ids, norm):
            scores[item_id] = float(score)

        return scores


# ── Singleton ──────────────────────────────────────────────────────────────────
_cf_instance: Optional[CollaborativeFilter] = None
_cf_init_lock = asyncio.Lock()


async def get_collaborative_filter() -> CollaborativeFilter:
    global _cf_instance
    if _cf_instance is not None:
        return _cf_instance
    async with _cf_init_lock:
        if _cf_instance is None:
            _cf_instance = CollaborativeFilter()
            await _cf_instance.train()
    return _cf_instance
