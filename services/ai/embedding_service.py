# services/embedding_service.py
"""
Embedding Service — Ollama mxbai-embed-large (local, unlimited, 1024d).
"""

import asyncio
import aiohttp
import numpy as np
from typing import Dict, Optional, List
from loguru import logger


class EmbeddingService:
    """
    Generates embeddings using Ollama mxbai-embed-large.
    1024 dimensions — compatible with pgvector Vector(1024).
    """

    OLLAMA_URL = "http://localhost:11434/api/embeddings"
    MODEL = "mxbai-embed-large"

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: Dict[str, List[float]] = {}
        self._embedding_dim = 1024
        logger.info(f"🔢 Embedding: Ollama {self.MODEL} ({self._embedding_dim}d)")

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def generate_embedding(self, text: str) -> Optional[List[float]]:
        """Generate embedding via Ollama mxbai."""
        if not text:
            return None

        text = text[:1000] if len(text) > 1000 else text

        cache_key = text.strip()
        if cache_key in self._cache:
            return self._cache[cache_key]

        session = await self._get_session()
        payload = {"model": self.MODEL, "prompt": text}

        try:
            async with session.post(
                self.OLLAMA_URL, json=payload,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    embedding = data.get("embedding")
                    if embedding:
                        self._cache[cache_key] = embedding
                        return embedding
        except Exception as e:
            logger.error(f"Ollama embedding failed: {e}")

        return None

    async def generate_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Generate embeddings for multiple texts."""
        results = []
        for text in texts:
            emb = await self.generate_embedding(text)
            results.append(emb)
            await asyncio.sleep(0.05)  # Small delay between calls
        return results

    @property
    def embedding_dim(self) -> int:
        return self._embedding_dim

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()