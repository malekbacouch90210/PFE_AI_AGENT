#!/usr/bin/env python3
"""
run_embedding.py – Phase 2: Generate and store embeddings for normalized products.
"""
import asyncio, argparse, sys
from pathlib import Path

# Fix for Windows + psycopg async
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import httpx
from loguru import logger
from sqlalchemy import text

from config import config
from database import DatabaseManager

# --- Tiny embedding helper (replaces services/embedding_service) ---
async def generate_embedding(text: str) -> list | None:
    """Call Ollama mxbai-embed-large, return 1024-dim vector or None."""
    if not text:
        return None
    try:
        async with httpx.AsyncClient(timeout=config.OLLAMA_TIMEOUT) as client:
            resp = await client.post(
                f"{config.OLLAMA_URL}/api/embeddings",
                json={"model": config.OLLAMA_MODEL_EMBED, "prompt": text[:1000]},
            )
            resp.raise_for_status()
            vec = resp.json().get("embedding")
            if vec and len(vec) == 1024:
                return vec
    except Exception as e:
        logger.error(f"Embedding API error: {e}")
    return None
# ----------------------------------------------------------------

async def run_embedding(limit: int = 200, run_id: str = None):
    logger.info("=" * 60)
    logger.info("Phase Modeling: Embedding generation")
    logger.info(f"  Model: {config.OLLAMA_MODEL_EMBED}")
    logger.info("=" * 60)

    db = DatabaseManager()
    await db.create_tables()

    try:
        async with db.engine.connect() as conn:
            if run_id:
                query = text("""
                    SELECT id, embedding_text
                    FROM scraped_products_normalized
                    WHERE embedding IS NULL AND run_id = :run_id
                    ORDER BY id
                    LIMIT :limit
                """)
                result = await conn.execute(query, {"run_id": run_id, "limit": limit})
            else:
                query = text("""
                    SELECT id, embedding_text
                    FROM scraped_products_normalized
                    WHERE embedding IS NULL
                    ORDER BY id
                    LIMIT :limit
                """)
                result = await conn.execute(query, {"limit": limit})

            rows = result.fetchall()

        if not rows:
            logger.info("✅ No products need embedding")
            return

        logger.info(f"📦 Processing {len(rows)} products")

        for row in rows:
            norm_id, emb_text = row[0], row[1]
            if not emb_text:
                logger.warning(f"  ⚠️ Product {norm_id} has no embedding_text – skipping")
                continue

            vector = await generate_embedding(emb_text)
            if vector:
                async with db.engine.begin() as conn:
                    await conn.execute(
                        text("""
                            UPDATE scraped_products_normalized
                            SET embedding = :embedding
                            WHERE id = :norm_id
                        """),
                        {"embedding": vector, "norm_id": norm_id},
                    )
                logger.info(f"  ✅ {norm_id}: embedding stored ({len(vector)}d)")
            else:
                logger.error(f"  ❌ {norm_id}: embedding failed")

    except Exception as e:
        logger.exception("Fatal error in embedding phase")
    finally:
        pass

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Phase 2 – Generate embeddings for normalized products")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--run-id", type=str, default=None)
    args = p.parse_args()
    asyncio.run(run_embedding(args.limit, args.run_id))