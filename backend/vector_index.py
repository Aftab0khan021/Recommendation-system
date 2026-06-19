"""
Phase 3: TF-IDF-based vector index for semantic search.

Uses the existing TF-IDF item embeddings from RecommendationEngine to enable
true vector similarity search — no external model downloads needed.

How it works:
    1. After the recommendation engine builds item embeddings, call rebuild().
    2. For a query string, vectorize it with the same fitted TF-IDF vectorizer.
    3. Compute cosine similarity between query vector and all item vectors (one
       batched matrix multiplication — O(n) not O(n²)).
    4. Return the top-k most similar item_ids with their similarity scores.

Blending with text score in search_engine.py:
    final_score = 0.65 · vector_score + 0.35 · text_score
"""
import logging
import asyncio
import numpy as np
from typing import List, Tuple, Optional
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

_vi_lock = asyncio.Lock()


class VectorIndex:
    """
    In-memory vector index backed by the recommendation engine's TF-IDF embeddings.
    """

    def __init__(self):
        self._matrix: Optional[np.ndarray] = None   # (n_items, n_features)
        self._item_ids: List[str] = []
        self._vectorizer = None                       # fitted TfidfVectorizer
        self.is_ready = False

    async def rebuild(self, embedding_matrix: np.ndarray, item_ids: List[str],
                      vectorizer) -> None:
        """Copy the embedding matrix and fitted vectorizer from the rec engine."""
        async with _vi_lock:
            self._matrix = embedding_matrix.copy()
            self._item_ids = list(item_ids)
            self._vectorizer = vectorizer
            self.is_ready = True
        logger.info(f"[VectorIndex] Rebuilt: {len(item_ids)} items, "
                    f"{embedding_matrix.shape[1]} dims")

    def query(self, text: str, top_k: int = 50) -> List[Tuple[str, float]]:
        """
        Return top_k (item_id, similarity_score) pairs for a query string.
        Scores are in [0, 1].  Returns empty list if index not ready.
        """
        if not self.is_ready or self._vectorizer is None or self._matrix is None:
            return []

        try:
            q_vec = self._vectorizer.transform([text]).toarray()  # (1, n_features)
            sims = cosine_similarity(q_vec, self._matrix)[0]      # (n_items,)

            # Top-k indices (unsorted first, then sorted)
            if top_k >= len(sims):
                top_indices = np.argsort(sims)[::-1]
            else:
                top_indices = np.argpartition(sims, -top_k)[-top_k:]
                top_indices = top_indices[np.argsort(sims[top_indices])[::-1]]

            return [
                (self._item_ids[i], float(sims[i]))
                for i in top_indices
                if sims[i] > 0.01   # skip near-zero matches
            ]

        except Exception as e:
            logger.error(f"[VectorIndex] query error: {e}")
            return []


# ── Singleton ──────────────────────────────────────────────────────────────────
vector_index = VectorIndex()
