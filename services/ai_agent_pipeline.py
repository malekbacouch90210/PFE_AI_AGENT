"""
services/ai_agent_pipeline.py
Connects all CRISP-DM phases after scraping completes.

EXACT paths (from run_normalization.py + run_matching.py):
  services/ingestion/normalization_pipeline.py → NormalizationPipeline
  services/matching/matching_pipeline.py       → MatchingPipeline
"""
import asyncio
import sys
import time
from datetime import datetime
from typing import Dict

from loguru import logger

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _run_async(coro):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def run_normalization(run_id: str, limit: int = 500) -> Dict:
    """
    Phase 2+3 — Data Preparation + Modeling (Embedding).
    Uses: services/ingestion/normalization_pipeline.py
    Same as: python run_normalization.py --run-id <run_id>
    """
    logger.info(f"[Pipeline] Phase 2+3 — Normalization+Embedding | run_id={run_id}")
    t0 = time.time()
    try:
        # CORRECT path confirmed from run_normalization.py (doc 28)
        from services.ingestion.normalization_pipeline import NormalizationPipeline
        from database import DatabaseManager
        from sqlalchemy import text

        async def _run():
            db = DatabaseManager()
            # Same setup as run_normalization.py main()
            await db.create_tables()
            # Ensure UNIQUE constraint
            async with db.engine.begin() as conn:
                r = await conn.execute(text("""
                    SELECT COUNT(*) FROM pg_constraint
                    WHERE conname='scraped_products_normalized_scraped_product_id_key'
                      AND conrelid='scraped_products_normalized'::regclass
                """))
                if (r.fetchone()[0] or 0) == 0:
                    await conn.execute(text("""
                        DELETE FROM scraped_products_normalized a
                        USING scraped_products_normalized b
                        WHERE a.id < b.id
                          AND a.scraped_product_id = b.scraped_product_id
                          AND a.scraped_product_id IS NOT NULL
                    """))
                    await conn.execute(text("""
                        ALTER TABLE scraped_products_normalized
                        ADD CONSTRAINT scraped_products_normalized_scraped_product_id_key
                        UNIQUE (scraped_product_id)
                    """))
            pipe  = NormalizationPipeline(db)
            stats = await pipe.run(limit=limit, run_id=run_id)
            return stats

        stats   = _run_async(_run())
        elapsed = round(time.time() - t0, 1)
        logger.info(f"[Pipeline] Phase 2+3 done {elapsed}s | ok={stats.get('ok',0)}")
        return {"phase": "normalization", "elapsed": elapsed, **stats}

    except Exception as e:
        logger.error(f"[Pipeline] Phase 2+3 error: {e}")
        return {"phase": "normalization", "error": str(e),
                "elapsed": round(time.time() - t0, 1)}


def run_matching(run_id: str, limit: int = 500) -> Dict:
    """
    Phase 4 — Evaluation: Matching.
    Uses: services/matching/matching_pipeline.py
    Same as: python run_matching.py --run_id <run_id>
    """
    logger.info(f"[Pipeline] Phase 4 — Matching | run_id={run_id}")
    t0 = time.time()
    try:
        # CORRECT path confirmed from run_matching.py
        from services.matching.matching_pipeline import MatchingPipeline
        from database import DatabaseManager
        from sqlalchemy import text

        async def _run():
            db = DatabaseManager()
            # Same setup as run_matching.py ensure_validation_columns()
            async with db.engine.begin() as conn:
                await conn.execute(text("""
                    ALTER TABLE produit_matches
                    ADD COLUMN IF NOT EXISTS validation_decision VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS validated_at TIMESTAMP
                """))
            pipe  = MatchingPipeline(db)
            stats = await pipe.run(limit=limit, run_id=run_id, groq_reasons=True)
            return stats

        stats   = _run_async(_run())
        elapsed = round(time.time() - t0, 1)
        new_p   = stats.get("accepted", 0)
        similar = stats.get("duplicate", 0)
        logger.info(
            f"[Pipeline] Phase 4 done {elapsed}s | "
            f"NEW_PRODUCT={new_p} SIMILAR={similar}"
        )
        return {"phase": "matching", "elapsed": elapsed,
                "new_products": new_p, **stats}

    except Exception as e:
        logger.error(f"[Pipeline] Phase 4 error: {e}")
        return {"phase": "matching", "error": str(e),
                "elapsed": round(time.time() - t0, 1), "new_products": 0}


def run_post_scraping_pipeline(run_id: str, limit: int = 500) -> Dict:
    """
    Called after Phase 1 scraping.
    Runs Phase 2+3 (normalization+embedding) then Phase 4 (matching).
    Returns new_products count for validation page redirect.
    """
    logger.info(f"[Pipeline] Starting post-scraping | run_id={run_id}")
    started = datetime.now()
    results = {"run_id": run_id, "started_at": started, "phases": {}}

    r2 = run_normalization(run_id, limit=limit)
    results["phases"]["normalization"] = r2
    if r2.get("error"):
        logger.warning(f"[Pipeline] Normalization error: {r2['error']}")

    r4 = run_matching(run_id, limit=limit)
    results["phases"]["matching"] = r4

    results["new_products"] = r4.get("new_products", 0)
    results["finished_at"]  = datetime.now()
    results["elapsed"]      = round(
        (results["finished_at"] - started).total_seconds(), 1
    )
    logger.info(
        f"[Pipeline] Done {results['elapsed']}s | "
        f"new_products={results['new_products']}"
    )
    return results