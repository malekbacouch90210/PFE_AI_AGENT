
import asyncio
import argparse
import sys
from pathlib import Path
from typing import Optional

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
from sqlalchemy import text

from config import config
from database import DatabaseManager
from services.odoo.odoo_client import odoo_client
from services.odoo.odoo_sync import OdooSync
from services.odoo.price_estimator import reestimate_null_prices


async def check_odoo(db: DatabaseManager):
    """Health check and statistics."""
    logger.info("🔍 Checking Odoo connection...")
    ok = await odoo_client.health_check()
    if not ok:
        return

    logger.info("\n📊 Sync readiness check:")
    async with db.engine.connect() as conn:
        # ACCEPTED ready
        r = await conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE pm.odoo_product_id IS NULL) AS pending,
                COUNT(*) FILTER (WHERE pm.odoo_product_id IS NOT NULL) AS synced,
                COUNT(*) FILTER (WHERE spn.estimated_price IS NULL
                                   AND pm.odoo_product_id IS NULL) AS null_price
            FROM produit_matches pm
            JOIN scraped_products_normalized spn
                ON spn.scraped_product_id = pm.scraped_product_id
            WHERE pm.match_status = 'ACCEPTED'
        """))
        row = r.fetchone()
        if row:
            logger.info(f"   ACCEPTED pending sync : {row[0]}")
            logger.info(f"   Already in Odoo       : {row[1]}")
            logger.info(f"   NULL price (need est.): {row[2]}")

        # Breakdown by type
        r2 = await conn.execute(text("""
            SELECT spn.canonical_activity_type, COUNT(*) AS cnt
            FROM produit_matches pm
            JOIN scraped_products_normalized spn
                ON spn.scraped_product_id = pm.scraped_product_id
            WHERE pm.match_status = 'ACCEPTED'
              AND pm.odoo_product_id IS NULL
            GROUP BY spn.canonical_activity_type
        """))
        logger.info("\n   By activity type:")
        for type_row in r2.fetchall():
            logger.info(f"     {type_row[0]:<15} {type_row[1]} products")

        # Breakdown by country
        r3 = await conn.execute(text("""
            SELECT spn.normalized_country, COUNT(*) AS cnt
            FROM produit_matches pm
            JOIN scraped_products_normalized spn
                ON spn.scraped_product_id = pm.scraped_product_id
            WHERE pm.match_status = 'ACCEPTED'
              AND pm.odoo_product_id IS NULL
            GROUP BY spn.normalized_country
            ORDER BY cnt DESC LIMIT 10
        """))
        logger.info("\n   Top countries pending sync:")
        for c_row in r3.fetchall():
            logger.info(f"     {str(c_row[0] or 'Unknown'):<25} {c_row[1]} products")


async def main(
    limit: int,
    run_id: Optional[str],
    check_only: bool,
    price_only: bool,
    sync_only: bool,
    no_suppliers: bool,
):
    logger.info("=" * 60)
    logger.info("PHase 5 deployment - odoo")
    logger.info(f"  Odoo URL : {config.ODOO_URL}")
    logger.info(f"  Odoo DB  : {config.ODOO_DB}")
    logger.info(f"  Limit    : {limit}")
    logger.info("=" * 60)

    db = DatabaseManager()
    await db.create_tables()

    if check_only:
        await check_odoo(db)
        return

    # Step 1: Re-estimate NULL prices for ACCEPTED products
    if not sync_only:
        price_stats = await reestimate_null_prices(db)
        logger.info(
            f"💶 Price estimation: "
            f"DEX-ref={price_stats['from_dex']} "
            f"Groq={price_stats['from_groq']} "
            f"failed={price_stats['failed']}"
        )

    if price_only:
        logger.info("✅ Price re-estimation complete (--price-only)")
        return

    # Step 2: Push to Odoo
    sync = OdooSync(db)
    await sync.run(
        limit          = limit,
        run_id         = run_id,
        sync_suppliers = not no_suppliers,
    )

    # Final check
    await check_odoo(db)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="phase 5 deployement — Odoo Sync")
    p.add_argument("--limit",         type=int, default=100)
    p.add_argument("--run-id",        type=str, default=None)
    p.add_argument("--check",         action="store_true")
    p.add_argument("--price-only",    action="store_true")
    p.add_argument("--sync-only",     action="store_true")
    p.add_argument("--no-suppliers",  action="store_true")
    args = p.parse_args()

    asyncio.run(main(
        limit         = args.limit,
        run_id        = args.run_id,
        check_only    = args.check,
        price_only    = args.price_only,
        sync_only     = args.sync_only,
        no_suppliers  = args.no_suppliers,
    ))
