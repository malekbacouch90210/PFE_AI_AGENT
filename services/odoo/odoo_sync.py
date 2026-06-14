"""
services/odoo/odoo_sync.py — Sprint 6

Pushes ACCEPTED products from scraped_products_normalized into Odoo
using the dex_sourcing addon fields (x_ville, x_activity_type, etc.)

FLOW:
  1. Load ACCEPTED products from produit_matches + scraped_products_normalized
  2. For each product: build Odoo vals dict matching dex_sourcing fields
  3. Dedup check: skip if already in Odoo (same name + run_id)
  4. Create product.template in Odoo via XML-RPC
  5. Also sync fournisseurs as res.partner in Odoo
  6. Update produit_matches.odoo_product_id after insert

FIELDS MAPPED (scraped_products_normalized → product.template):
  clean_title           → name
  clean_description     → description_sale
  estimated_price       → list_price
  normalized_city       → x_ville
  normalized_country    → x_country
  canonical_activity    → x_activity_type
  normalized_category   → x_category_dex
  duration_minutes      → x_duration_minutes
  price_is_estimated    → x_price_estimated
  fournisseur.domain    → x_supplier_domain + x_supplier_name
  run_id                → x_run_id
  similarity_score      → x_similarity_score
  source                → x_scraping_method
"""
import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from loguru import logger
from sqlalchemy import text

from config import config
from database import DatabaseManager
from services.odoo.odoo_client import odoo_client

# Scraping method mapping scraped source → Odoo selection values
SOURCE_METHOD_MAP = {
    "firecrawl_groq":     "playwright",   # Firecrawl = dynamic = playwright category
    "crawl4ai_groq":      "playwright",
    "crawl4ai_heuristic": "playwright",
    "bs4":                "scrapy",
    "jsonld":             "jsonld",
    "api":                "api",
    "marketplace_groq":   "playwright",
}

# Connection type mapping (DB → Odoo)
CONN_MAP = {
    "API":             "API",
    "CHANNEL":         "CHANNEL",
    "CHANNEL_MANAGER": "CHANNEL",
    "BOTH":            "BOTH",
    "MARKETPLACE":     "API",
    "NONE":            "NONE",
}

# DEX zone mapping by country (for supplier res.partner)
COUNTRY_ZONE_MAP = {
    # Zone 1: Italy
    "Italy": "1",
    # Zone 2: Europe
    "France": "2", "Spain": "2", "Germany": "2", "Portugal": "2",
    "Greece": "2", "Netherlands": "2", "Belgium": "2", "Switzerland": "2",
    "Austria": "2", "Croatia": "2", "Czech Republic": "2", "Hungary": "2",
    "Poland": "2", "Norway": "2", "Sweden": "2", "Denmark": "2",
    "Finland": "2", "Ireland": "2", "United Kingdom": "2",
    # Zone 3: Americas
    "United States": "3", "Canada": "3", "Mexico": "3",
    "Brazil": "3", "Argentina": "3", "Peru": "3", "Colombia": "3",
    # Zone 4: Africa
    "Morocco": "4", "Tunisia": "4", "Egypt": "4", "Algeria": "4",
    "Senegal": "4", "Kenya": "4", "South Africa": "4", "Tanzania": "4",
    "Ghana": "4", "Ethiopia": "4", "Jordan": "4",
    # Zone 5: Asia / Oceania
    "China": "5", "Japan": "5", "Thailand": "5", "India": "5",
    "Indonesia": "5", "Vietnam": "5", "South Korea": "5",
    "Singapore": "5", "Malaysia": "5", "Australia": "5",
    "New Zealand": "5", "Philippines": "5",
}


def build_product_vals(row: Dict) -> Dict:
    """
    Build the Odoo product.template vals dict from scraped_products_normalized row.
    Maps to the dex_sourcing addon fields exactly.
    """
    source_method = SOURCE_METHOD_MAP.get(row.get("source") or "", "playwright")

    # Clean description for Odoo (strip HTML if any)
    desc = row.get("clean_description") or ""
    desc = re.sub(r"<[^>]+>", " ", desc)
    desc = re.sub(r"\s+", " ", desc).strip()

    # Price — use estimated_price in EUR, fallback to 0 (Odoo needs a value)
    price = row.get("estimated_price")
    try:
        price = float(price) if price else 0.0
    except (TypeError, ValueError):
        price = 0.0

    # Activity type must match Odoo selection
    atype = row.get("canonical_activity_type") or "EXCURSION"
    if atype not in ("EXCURSION", "TICKET", "TRANSFER"):
        atype = "EXCURSION"

    vals = {
        # Core product fields
        "name":              row["clean_title"][:250],
        "type":              "service",          # tourism = service
        "list_price":        price,
        "description_sale":  desc[:2000] or False,

        # DEX custom fields (from dex_sourcing addon)
        "x_ville":              row.get("normalized_city")    or False,
        "x_country":            row.get("normalized_country") or False,
        "x_activity_type":      atype,
        "x_category_dex":       row.get("normalized_category") or False,
        "x_duration_minutes":   row.get("duration_minutes") or 0,
        "x_price_estimated":    bool(row.get("price_is_estimated")),
        "x_original_price":     price,
        "x_original_currency":  "EUR",
        "x_ai_agent_source":    True,
        "x_source_url":         row.get("url_source") or False,
        "x_booking_url":        row.get("url_source") or False,
        "x_scraping_method":    source_method,
        "x_run_id":             row.get("run_id") or False,
        "x_similarity_score":   float(row.get("similarity_score") or 0.0),
        "x_supplier_name":      row.get("supplier_name") or False,
        "x_supplier_domain":    row.get("supplier_domain") or False,
        "x_inserted_at":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),

        # Make it sellable
        "sale_ok":     True,
        "purchase_ok": True,
    }

    return vals


def build_supplier_vals(fournisseur: Dict) -> Dict:
    """Build res.partner vals from fournisseurs table row."""
    country = fournisseur.get("_detected_country") or ""
    zone    = COUNTRY_ZONE_MAP.get(country, "2")

    raw_conn = (fournisseur.get("type_connexion") or "NONE").upper().strip()
    conn_type = CONN_MAP.get(raw_conn, "NONE")

    return {
        "name":         fournisseur.get("nom") or "Unknown Supplier",
        "website":      f"https://{fournisseur['domain']}" if fournisseur.get("domain") else False,
        "is_company":   True,
        "supplier_rank": 1,

        # DEX custom fields
        "x_supplier_score":        fournisseur.get("score") or 0,
        "x_supplier_status":       "scraped",
        "x_has_api":               bool(fournisseur.get("has_api")),
        "x_api_name":              fournisseur.get("api_name") or False,
        "x_has_channel_manager":   bool(fournisseur.get("has_channel")),
        "x_channel_manager_name":  fournisseur.get("channel_name") or False,
        "x_connection_type":       conn_type,
        "x_supplier_ville":        fournisseur.get("ville") or False,
        "x_zone":                  zone,
        "x_run_id":                fournisseur.get("run_id") or False,
        "x_discovered_at":         datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "x_products_count_scraped":  fournisseur.get("products_scraped") or 0,
        "x_products_count_accepted": fournisseur.get("products_accepted") or 0,
    }


class OdooSync:
    """
    Sprint 6 — Push ACCEPTED products and suppliers to Odoo 19.

    Batch of 100 by default.
    Each product checked for duplicates before insert.
    Supplier stats updated after batch.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def run(
        self,
        limit: int = 100,
        run_id: Optional[str] = None,
        sync_suppliers: bool = True,
    ) -> Dict:
        stats = dict(
            products_total=0, products_inserted=0,
            products_skipped=0, products_failed=0,
            suppliers_inserted=0, suppliers_skipped=0,
        )

        logger.info("=" * 60)
        logger.info("🚀 Sprint 6 — Odoo Sync")
        logger.info(f"   Odoo URL  : {config.ODOO_URL}")
        logger.info(f"   Database  : {config.ODOO_DB}")
        logger.info(f"   Limit     : {limit}")
        logger.info("=" * 60)

        # Authenticate
        try:
            await odoo_client.authenticate()
        except ConnectionError as e:
            logger.error(str(e))
            return stats

        # Step 1: Sync suppliers first
        if sync_suppliers:
            await self._sync_suppliers(stats, run_id)

        # Step 2: Sync accepted products
        await self._sync_products(stats, limit, run_id)

        logger.info("=" * 60)
        logger.info(f"✅ Odoo Sync complete")
        logger.info(f"   Products inserted : {stats['products_inserted']}")
        logger.info(f"   Products skipped  : {stats['products_skipped']} (already in Odoo)")
        logger.info(f"   Products failed   : {stats['products_failed']}")
        logger.info(f"   Suppliers inserted: {stats['suppliers_inserted']}")
        logger.info("=" * 60)
        return stats

    async def _sync_products(self, stats: Dict, limit: int, run_id: Optional[str]):
        """Load ACCEPTED products and push to Odoo product.template."""
        async with self.db.engine.connect() as conn:
            where_run = "AND spn.run_id = :run_id" if run_id else ""
            params    = {"limit": limit}
            if run_id:
                params["run_id"] = run_id

            result = await conn.execute(text(f"""
                SELECT
                    spn.scraped_product_id,
                    spn.fournisseur_id,
                    spn.run_id,
                    spn.clean_title,
                    spn.clean_description,
                    spn.normalized_city,
                    spn.normalized_country,
                    spn.canonical_activity_type,
                    spn.normalized_category,
                    spn.estimated_price,
                    spn.price_is_estimated,
                    spn.duration_minutes,
                    spn.image_url,
                    pm.similarity_score,
                    pm.id AS match_id,
                    f.nom          AS supplier_name,
                    f.domain       AS supplier_domain,
                    p.url_source,
                    p.source
                FROM scraped_products_normalized spn
                JOIN produit_matches pm
                    ON pm.scraped_product_id = spn.scraped_product_id
                LEFT JOIN produits p
                    ON p.id = spn.scraped_product_id
                LEFT JOIN fournisseurs f
                    ON f.id = spn.fournisseur_id
                WHERE pm.match_status = 'ACCEPTED'
                  AND pm.odoo_product_id IS NULL
                  {where_run}
                ORDER BY spn.scraped_product_id
                LIMIT :limit
            """), params)
            rows = [dict(r._mapping) for r in result.fetchall()]

        stats["products_total"] = len(rows)
        logger.info(f"📦 {len(rows)} ACCEPTED products to sync to Odoo")

        if not rows:
            logger.info("  ✅ Nothing to sync")
            return

        for row in rows:
            pid   = row["scraped_product_id"]
            title = (row.get("clean_title") or "")[:40]

            try:
                vals = build_product_vals(row)

                # Dedup check
                existing_id = await odoo_client.search_product(
                    vals["name"], vals.get("x_run_id")
                )
                if existing_id:
                    stats["products_skipped"] += 1
                    logger.debug(f"  ⏭ Skip existing: {title}")
                    # Still save odoo_product_id
                    await self._update_match_odoo_id(row["match_id"], existing_id)
                    continue

                # Insert
                odoo_id = await odoo_client.create_product(vals)
                if odoo_id:
                    stats["products_inserted"] += 1
                    await self._update_match_odoo_id(row["match_id"], odoo_id)
                    logger.info(
                        f"  ✅ Inserted odoo_id={odoo_id} | "
                        f"{title} | "
                        f"{row.get('normalized_city','?')},{row.get('normalized_country','?')} | "
                        f"{row.get('estimated_price','?')}€"
                    )
                else:
                    stats["products_failed"] += 1

            except Exception as e:
                stats["products_failed"] += 1
                logger.error(f"  ❌ {pid} | {title}: {e}")

    async def _sync_suppliers(self, stats: Dict, run_id: Optional[str]):
        """Sync fournisseurs to Odoo res.partner."""
        async with self.db.engine.connect() as conn:
            where_run = "WHERE run_id = :run_id" if run_id else ""
            params    = {"run_id": run_id} if run_id else {}

            result = await conn.execute(text(f"""
                SELECT f.id, f.nom, f.domain, f.score, f.has_api,
                       f.api_name, f.has_channel, f.channel_name,
                       f.type_connexion, f.ville, f.run_id,
                       f.pays_id,
                       p.nom AS country_name
                FROM fournisseurs f
                LEFT JOIN pays p ON f.pays_id = p.id
                {where_run}
                LIMIT 200
            """), params)
            rows = [dict(r._mapping) for r in result.fetchall()]

        for row in rows:
            name   = row.get("nom") or ""
            domain = row.get("domain") or ""

            # Get product counts for this supplier
            async with self.db.engine.connect() as conn:
                pc = await conn.execute(text("""
                    SELECT
                        COUNT(*) FILTER (WHERE pm.match_status = 'ACCEPTED') AS accepted,
                        COUNT(*) AS total
                    FROM scraped_products_normalized spn
                    JOIN produit_matches pm ON pm.scraped_product_id = spn.scraped_product_id
                    WHERE spn.fournisseur_id = :fid
                """), {"fid": row["id"]})
                pc_row = pc.fetchone()
                row["products_accepted"] = pc_row[0] if pc_row else 0
                row["products_scraped"]  = pc_row[1] if pc_row else 0
                row["_detected_country"] = row.get("country_name") or ""

            # Dedup
            existing = await odoo_client.search_supplier(name, domain)
            if existing:
                stats["suppliers_skipped"] += 1
                continue

            vals = build_supplier_vals(row)
            sup_id = await odoo_client.create_supplier(vals)
            if sup_id:
                stats["suppliers_inserted"] += 1
                logger.debug(f"  🏢 Supplier: {name} → odoo_id={sup_id}")

    async def _update_match_odoo_id(self, match_id: int, odoo_product_id: int):
        """Save the Odoo product id back to produit_matches."""
        try:
            async with self.db.engine.begin() as conn:
                await conn.execute(text("""
                    UPDATE produit_matches
                    SET odoo_product_id = :oid
                    WHERE id = :mid
                """), {"oid": odoo_product_id, "mid": match_id})
        except Exception as e:
            logger.debug(f"  update_match_odoo_id error: {e}")