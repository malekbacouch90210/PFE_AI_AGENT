"""

Allows Streamlit dashboard / admin UI to trigger Odoo sync via HTTP.

Endpoints:
  POST /api/odoo/sync          → trigger product sync
  POST /api/odoo/estimate      → re-estimate NULL prices
  GET  /api/odoo/status        → sync statistics
  GET  /api/odoo/health        → Odoo connection check
"""
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel

from database import DatabaseManager
from services.odoo.odoo_client import odoo_client
from services.odoo.odoo_sync import OdooSync
from services.odoo.price_estimator import reestimate_null_prices

router = APIRouter(prefix="/api/odoo", tags=["Odoo Integration"])


class SyncRequest(BaseModel):
    limit:          int           = 100
    run_id:         Optional[str] = None
    sync_suppliers: bool          = True
    price_first:    bool          = True   # re-estimate prices before sync


class SyncResponse(BaseModel):
    status:             str
    products_inserted:  int = 0
    products_skipped:   int = 0
    products_failed:    int = 0
    suppliers_inserted: int = 0
    prices_estimated:   int = 0
    message:            str = ""


@router.get("/health")
async def odoo_health():
    """Check Odoo connection."""
    ok = await odoo_client.health_check()
    return {
        "odoo_connected": ok,
        "odoo_url":       odoo_client.url,
        "odoo_db":        odoo_client.db,
    }


@router.get("/status")
async def odoo_status():
    """Get sync statistics from produit_matches + scraped_products_normalized."""
    db = DatabaseManager()
    from sqlalchemy import text
    async with db.engine.connect() as conn:
        r = await conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE pm.odoo_product_id IS NULL)     AS pending,
                COUNT(*) FILTER (WHERE pm.odoo_product_id IS NOT NULL)  AS synced,
                COUNT(*) FILTER (WHERE spn.estimated_price IS NULL
                                   AND pm.odoo_product_id IS NULL)      AS null_price
            FROM produit_matches pm
            JOIN scraped_products_normalized spn
                ON spn.scraped_product_id = pm.scraped_product_id
            WHERE pm.match_status = 'ACCEPTED'
        """))
        row = r.fetchone()
        pending    = row[0] if row else 0
        synced     = row[1] if row else 0
        null_price = row[2] if row else 0

        # Country breakdown
        r2 = await conn.execute(text("""
            SELECT spn.normalized_country, COUNT(*) AS cnt
            FROM produit_matches pm
            JOIN scraped_products_normalized spn
                ON spn.scraped_product_id = pm.scraped_product_id
            WHERE pm.match_status = 'ACCEPTED'
              AND pm.odoo_product_id IS NULL
            GROUP BY spn.normalized_country
            ORDER BY cnt DESC LIMIT 10
        """))
        by_country = [{"country": r[0] or "Unknown", "count": r[1]} for r in r2.fetchall()]

        # Type breakdown
        r3 = await conn.execute(text("""
            SELECT spn.canonical_activity_type, COUNT(*) AS cnt
            FROM produit_matches pm
            JOIN scraped_products_normalized spn
                ON spn.scraped_product_id = pm.scraped_product_id
            WHERE pm.match_status = 'ACCEPTED'
              AND pm.odoo_product_id IS NULL
            GROUP BY spn.canonical_activity_type
        """))
        by_type = {r[0]: r[1] for r in r3.fetchall()}

    return {
        "pending_sync":  pending,
        "synced":        synced,
        "null_price":    null_price,
        "by_country":    by_country,
        "by_type":       by_type,
    }


@router.post("/sync", response_model=SyncResponse)
async def trigger_sync(req: SyncRequest):
    """
    Trigger Odoo sync for ACCEPTED products.
    Re-estimates NULL prices first if price_first=True.
    """
    db = DatabaseManager()
    prices_estimated = 0

    # Re-estimate prices first
    if req.price_first:
        price_stats = await reestimate_null_prices(db)
        prices_estimated = price_stats.get("from_dex", 0) + price_stats.get("from_groq", 0)

    # Sync to Odoo
    sync  = OdooSync(db)
    stats = await sync.run(
        limit          = req.limit,
        run_id         = req.run_id,
        sync_suppliers = req.sync_suppliers,
    )

    return SyncResponse(
        status             = "done",
        products_inserted  = stats["products_inserted"],
        products_skipped   = stats["products_skipped"],
        products_failed    = stats["products_failed"],
        suppliers_inserted = stats["suppliers_inserted"],
        prices_estimated   = prices_estimated,
        message            = f"Synced {stats['products_inserted']} products to Odoo",
    )


@router.post("/estimate-prices")
async def trigger_price_estimation():
    """Re-estimate NULL prices for ACCEPTED products without syncing to Odoo."""
    db = DatabaseManager()
    stats = await reestimate_null_prices(db)
    return {
        "status":     "done",
        "from_dex":   stats["from_dex"],
        "from_groq":  stats["from_groq"],
        "failed":     stats["failed"],
        "total":      stats["total"],
    }
