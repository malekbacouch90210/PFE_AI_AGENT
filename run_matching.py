"""
Run the matching pipeline from command line.

Usage:
  python run_matching.py                    # match all unmatched
  python run_matching.py --run_id abc123    # only a specific run
  python run_matching.py --limit 200        # limit products
  python run_matching.py --no-groq          # skip Groq reasons (faster)


  Human validation happens in the Streamlit validation page.
  Approved products are pushed to Odoo from there.
"""

import asyncio
import argparse
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from loguru import logger
from database import DatabaseManager
from services.matching.matching_pipeline import MatchingPipeline


async def ensure_validation_columns(db: DatabaseManager):
    """
    Add validation_decision and validated_at columns to produit_matches
    if they don't exist yet (safe — runs only if missing).
    """
    from sqlalchemy import text
    async with db.engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE produit_matches
            ADD COLUMN IF NOT EXISTS validation_decision VARCHAR(20),
            ADD COLUMN IF NOT EXISTS validated_at         TIMESTAMP
        """))
    logger.info("produit_matches validation columns ready")


async def main(limit: int, run_id: str, groq_reasons: bool):
    logger.info("=" * 60)
    logger.info("DEX AI Sourcing Agent — Matching Pipeline")
    logger.info(f"  limit      : {limit}")
    logger.info(f"  run_id     : {run_id or 'all'}")
    logger.info(f"  groq       : {groq_reasons}")
    logger.info(f"  NEW STATUSES: NEW_PRODUCT / SIMILAR")
    logger.info("=" * 60)

    db = DatabaseManager()

    # Ensure DB columns exist
    await ensure_validation_columns(db)

    # Run matching
    pipeline = MatchingPipeline(db)
    stats    = await pipeline.run(
        limit       = limit,
        run_id      = run_id,
        groq_reasons= groq_reasons,
    )

    logger.info("=" * 60)
    logger.info("MATCHING COMPLETE")
    logger.info(f"  NEW_PRODUCT (pending validation) : {stats.get('accepted', 0)}")
    logger.info(f"  SIMILAR (auto-excluded)          : {stats.get('duplicate', 0)}")
    logger.info(f"  Failed                           : {stats.get('failed', 0)}")
    logger.info(f"  Prices estimated                 : {stats.get('price_estimated', 0)}")
    logger.info("=" * 60)
    logger.info("Next step: open the Streamlit validation page to review products")
    logger.info("  streamlit run app.py  → ✅ Product Validation")
    logger.info("=" * 60)

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DEX Matching Pipeline")
    parser.add_argument("--limit",    type=int,  default=100,   help="Max products to match")
    parser.add_argument("--run_id",   type=str,  default=None,  help="Filter by run ID")
    parser.add_argument("--no-groq",  action="store_true",      help="Skip Groq AI reasons")
    args = parser.parse_args()

    asyncio.run(main(
        limit       = args.limit,
        run_id      = args.run_id,
        groq_reasons= not args.no_groq,
    ))