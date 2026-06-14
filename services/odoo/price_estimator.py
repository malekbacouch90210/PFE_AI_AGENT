"""
services/odoo/price_estimator.py — Sprint 6 price re-estimation

CONTEXT:
  After matching, 278 ACCEPTED products still have NULL price.
  Before inserting into Odoo, we need a price for every product.

STRATEGY:
  1. Group NULL-price products by (country, activity_type, category)
  2. For each group: find median price of DEX products in same city/country/type
  3. If DEX median found → use it (Layer 3: PostgreSQL truth)
  4. If not → Groq estimates based on context (Layer 2: reasoning)
  5. Validate: 5€ ≤ price ≤ 5000€

GROUP FALLBACKS (if city has no DEX price):
  country median → activity_type median → global category median
"""
import asyncio
import re
from typing import Dict, List, Optional, Tuple

import httpx
from loguru import logger
from sqlalchemy import text

from config import config
from database import DatabaseManager

PRICE_MIN = 5.0
PRICE_MAX = 5000.0


async def get_dex_price_reference(
    city: Optional[str],
    country: Optional[str],
    activity_type: str,
    category: Optional[str],
    db: DatabaseManager,
) -> Optional[float]:
    """
    Layer 3: Get median price from DEX products for same location + type.
    Priority: city+type → country+type → type only.
    """
    try:
        async with db.engine.connect() as conn:
            # Level 1: same city + activity_type
            if city:
                r = await conn.execute(text("""
                    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median
                    FROM dex_products_normalized
                    WHERE LOWER(normalized_city) = LOWER(:city)
                      AND canonical_activity_type = :atype
                      AND price IS NOT NULL AND price > 0
                """), {"city": city, "atype": activity_type})
                row = r.fetchone()
                if row and row[0]:
                    return round(float(row[0]), 2)

            # Level 2: same country + activity_type
            if country:
                r = await conn.execute(text("""
                    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median
                    FROM dex_products_normalized
                    WHERE LOWER(normalized_country) = LOWER(:country)
                      AND canonical_activity_type = :atype
                      AND price IS NOT NULL AND price > 0
                """), {"country": country, "atype": activity_type})
                row = r.fetchone()
                if row and row[0]:
                    return round(float(row[0]), 2)

            # Level 3: same activity_type globally
            r = await conn.execute(text("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median
                FROM dex_products_normalized
                WHERE canonical_activity_type = :atype
                  AND price IS NOT NULL AND price > 0
            """), {"atype": activity_type})
            row = r.fetchone()
            if row and row[0]:
                return round(float(row[0]), 2)

    except Exception as e:
        logger.debug(f"  dex_price_reference error: {e}")

    return None


async def groq_estimate_price(
    title: str,
    city: Optional[str],
    country: Optional[str],
    activity_type: str,
    category: Optional[str],
    dex_reference: Optional[float],
) -> Optional[float]:
    """Layer 2: Groq estimates price when no DEX reference available."""
    if not config.groq_configured():
        return None

    reference_str = f"\nDEX reference price for similar products: {dex_reference}€" if dex_reference else ""

    prompt = (
        f"Estimate the realistic selling price in EUR for this tourism product.\n\n"
        f"Product: {title}\n"
        f"Location: {city or 'Unknown'}, {country or 'Unknown'}\n"
        f"Type: {activity_type}\n"
        f"Category: {category or 'Tourism'}\n"
        f"{reference_str}\n\n"
        f"Rules:\n"
        f"- Price must be realistic for this destination and activity\n"
        f"- Return ONLY a number, nothing else\n"
        f"- Min 5€, Max 5000€\n\n"
        f"Price (EUR):"
    )

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
            resp = await c.post(
                f"{config.GROQ_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {config.GROQ_API_KEYS[0]}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       config.GROQ_FALLBACK_MODEL,
                    "messages":    [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens":  20,
                },
            )
        if resp.status_code != 200:
            return None

        raw = resp.json()["choices"][0]["message"]["content"].strip()
        m   = re.search(r"\d+(?:[.,]\d+)?", raw)
        if m:
            price = float(m.group().replace(",", "."))
            if PRICE_MIN <= price <= PRICE_MAX:
                return round(price, 2)
    except Exception as e:
        logger.debug(f"  groq_estimate_price error: {e}")

    return None


async def reestimate_null_prices(db: DatabaseManager) -> Dict:
    """
    Main re-estimation loop.
    Load ACCEPTED products with NULL price → estimate → update.
    """
    stats = dict(total=0, from_dex=0, from_groq=0, failed=0)

    logger.info("💶 Re-estimating NULL prices for ACCEPTED products...")

    async with db.engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT
                spn.scraped_product_id,
                spn.clean_title,
                spn.normalized_city,
                spn.normalized_country,
                spn.canonical_activity_type,
                spn.normalized_category
            FROM scraped_products_normalized spn
            JOIN produit_matches pm ON pm.scraped_product_id = spn.scraped_product_id
            WHERE pm.match_status = 'ACCEPTED'
              AND (spn.estimated_price IS NULL OR spn.estimated_price = 0)
            ORDER BY spn.scraped_product_id
        """))
        rows = result.fetchall()

    stats["total"] = len(rows)
    logger.info(f"  📋 {len(rows)} products need price estimation")

    if not rows:
        return stats

    # Rate limit Groq
    GROQ_DELAY = 1.5

    for row in rows:
        pid      = row[0]
        title    = row[1] or ""
        city     = row[2]
        country  = row[3]
        atype    = row[4] or "EXCURSION"
        category = row[5]

        # Step 1: DEX reference price (free, no LLM)
        dex_ref = await get_dex_price_reference(city, country, atype, category, db)

        # Step 2: Groq estimate if no DEX reference
        if dex_ref:
            final_price = dex_ref
            stats["from_dex"] += 1
            source = "DEX median"
        else:
            await asyncio.sleep(GROQ_DELAY)
            final_price = await groq_estimate_price(title, city, country, atype, category, None)
            if final_price:
                stats["from_groq"] += 1
                source = "Groq estimate"
            else:
                stats["failed"] += 1
                logger.debug(f"  ❌ No price for id={pid}: {title[:40]}")
                continue

        # Update
        async with db.engine.begin() as conn:
            await conn.execute(text("""
                UPDATE scraped_products_normalized
                SET estimated_price    = :price,
                    price_is_estimated = TRUE
                WHERE scraped_product_id = :pid
            """), {"price": final_price, "pid": pid})

        logger.debug(f"  💶 {pid} | {title[:40]} | {final_price}€ ({source})")

    logger.info(
        f"  ✅ Re-estimation done: "
        f"DEX-ref={stats['from_dex']} "
        f"Groq={stats['from_groq']} "
        f"failed={stats['failed']}"
    )
    return stats
