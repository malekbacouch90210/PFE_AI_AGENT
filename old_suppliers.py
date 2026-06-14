"""
test_scrape_existing_suppliers.py
──────────────────────────────────────────────────────────────
Scrape products from EXISTING fournisseurs in the DB.

Usage:
  python test_scrape_existing_suppliers.py --country France
  python test_scrape_existing_suppliers.py --country Belgium
  python test_scrape_existing_suppliers.py --country Tunisia
  python test_scrape_existing_suppliers.py --country "South Africa"
  python test_scrape_existing_suppliers.py  # prompts interactively

What it does:
  1. Look up the country in pays table by name
  2. Get all fournisseurs where pays_id = that country's id
     (only API / CHANNEL / BOTH / MARKETPLACE — no NONE)
  3. For each supplier → scrape products via Crawl4AI + Groq
  4. Save products to produits table
  5. Print summary at the end
"""

import argparse
import asyncio
import sys
import uuid
from datetime import datetime
from pathlib import Path

from loguru import logger
from sqlalchemy import select, text

# ── project root on sys.path ──────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from config import config
from database import DatabaseManager
from scrapers.product_scraper import ProductScraper


# ─────────────────────────────────────────────────────────────
# Config — edit these defaults if needed
# ─────────────────────────────────────────────────────────────

DEFAULT_MAX_PRODUCTS_PER_SUPPLIER = 20
DEFAULT_COUNTRY                   = ""    # empty = prompt user

# Fix psycopg async issue on Windows
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

async def get_pays_id(db: DatabaseManager, country_name: str) -> int | None:
    """
    Find pays.id by country name (case-insensitive).
    Returns None if country not found.
    """
    async with db.AsyncSessionLocal() as session:
        result = await session.execute(
            text("SELECT id, nom FROM pays WHERE LOWER(nom) = LOWER(:nom)"),
            {"nom": country_name.strip()},
        )
        row = result.fetchone()
        if row:
            return row[0], row[1]  # (id, canonical_name)
        return None, None


async def get_fournisseurs_for_country(
    db: DatabaseManager, pays_id: int
) -> list:
    """
    Get all fournisseurs for a country that have API or Channel Manager.
    Excludes NONE (no connection method — useless for product scraping).
    """
    async with db.engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT
                    f.id,
                    f.nom,
                    f.domain,
                    f.type_connexion,
                    f.has_api,
                    f.api_name,
                    f.has_channel,
                    f.channel_name,
                    f.is_marketplace,
                    f.marketplace_type,
                    f.score,
                    f.status,
                    p.nom as pays_nom
                FROM fournisseurs f
                JOIN pays p ON f.pays_id = p.id
                WHERE f.pays_id = :pays_id
                  AND f.type_connexion != 'NONE'
                  AND f.domain IS NOT NULL
                  AND f.domain != ''
                ORDER BY f.score DESC
            """),
            {"pays_id": pays_id},
        )
        rows = result.fetchall()

    suppliers = []
    for row in rows:
        suppliers.append({
            "id":               row[0],
            "nom":              row[1],
            "domain":           row[2],
            "type_connexion":   row[3],
            "has_api":          row[4],
            "api_name":         row[5],
            "has_channel":      row[6],
            "channel_name":     row[7],
            "is_marketplace":   row[8],
            "marketplace_type": row[9],
            "score":            row[10],
            "status":           row[11],
            "_detected_country": row[12],
            "_target_country":   row[12],
        })
    return suppliers


async def get_already_scraped_domains(db: DatabaseManager, pays_id: int) -> set:
    """
    Return domains that already have products in this country.
    Used to show info — we still scrape them (may have new products).
    """
    async with db.engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT DISTINCT f.domain
                FROM produits p
                JOIN fournisseurs f ON p.fournisseur_id = f.id
                WHERE f.pays_id = :pays_id
            """),
            {"pays_id": pays_id},
        )
        return {row[0] for row in result.fetchall()}


def pick_country_interactive(all_countries: list) -> str:
    """Let user pick a country interactively from the full list."""
    print("\n📋 Available countries in DB:")
    for i, c in enumerate(all_countries, 1):
        print(f"  {i:3d}. {c}")
    print()
    while True:
        choice = input("Enter country name or number: ").strip()
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(all_countries):
                return all_countries[idx]
            print("❌ Invalid number")
        elif choice:
            # Fuzzy match
            matches = [c for c in all_countries if choice.lower() in c.lower()]
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                print(f"Multiple matches: {matches}")
                print("Be more specific.")
            else:
                print(f"❌ '{choice}' not found. Try again.")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def main(country_name: str, max_products: int):
    logger.info("=" * 60)
    logger.info("🔄 DEX — Scrape products from existing suppliers")
    logger.info(f"   Country        : {country_name}")
    logger.info(f"   Max products   : {max_products} per supplier")
    logger.info(f"   Groq keys      : {config.groq_key_count()}")
    logger.info("=" * 60)

    db = DatabaseManager()
    await db.create_tables()

    # ── 1. Resolve country → pays_id ─────────────────────────
    pays_id, canonical_name = await get_pays_id(db, country_name)
    if not pays_id:
        logger.error(
            f"❌ Country '{country_name}' not found in pays table.\n"
            f"   Check spelling or run: SELECT nom FROM pays WHERE nom ILIKE '%{country_name}%'"
        )
        return

    logger.info(f"✅ Country resolved: '{canonical_name}' (pays_id={pays_id})")

    # ── 2. Get suppliers for this country ─────────────────────
    suppliers = await get_fournisseurs_for_country(db, pays_id)

    if not suppliers:
        logger.warning(
            f"⚠️  No API/CM suppliers found for '{canonical_name}'.\n"
            f"   Run fournisseur scraper first to discover suppliers."
        )
        return

    already_scraped = await get_already_scraped_domains(db, pays_id)

    logger.info(f"\n📋 Found {len(suppliers)} supplier(s) for {canonical_name}:")
    for i, s in enumerate(suppliers, 1):
        scraped_flag = "🔄 re-scrape" if s["domain"] in already_scraped else "🆕 new"
        logger.info(
            f"   {i:2d}. {s['nom'][:45]:<45} | "
            f"{s['type_connexion']:<11} | "
            f"score={s['score']:3d} | "
            f"{s['domain'][:40]} | {scraped_flag}"
        )

    # ── 3. Create run_id for this batch ──────────────────────
    run_id = f"rescrape_{canonical_name.lower().replace(' ','_')}_{str(uuid.uuid4())[:8]}"
    logger.info(f"\n🚀 Starting product scraping | run_id={run_id}")

    await db.save_scraping_run({
        "run_id":                    run_id,
        "countries":                 [canonical_name],
        "required_cities":           {},
        "activity_types":            ["excursion", "ticket", "transfer"],
        "min_suppliers":             len(suppliers),
        "max_suppliers":             len(suppliers),
        "max_products_per_supplier": max_products,
        "recall_old_suppliers":      True,
    })

    # ── 4. Scrape products ────────────────────────────────────
    product_scraper  = ProductScraper()
    total_extracted  = 0
    total_saved      = 0
    total_skipped    = 0
    total_rejected   = 0
    total_errors     = 0
    suppliers_done   = 0
    suppliers_empty  = 0

    for i, supplier in enumerate(suppliers, 1):
        domain = supplier.get("domain","?")
        logger.info(
            f"\n[{i}/{len(suppliers)}] 🕷 {domain} "
            f"| {supplier.get('type_connexion','')} "
            f"| {supplier.get('nom','')[:40]}"
        )

        try:
            products = await product_scraper.scrape_supplier(
                supplier     = supplier,
                max_products = max_products,
                run_id       = run_id,
            )
        except Exception as e:
            logger.error(f"  ❌ scrape_supplier crashed: {e}")
            suppliers_empty += 1
            continue

        if not products:
            logger.info(f"  ⚪ 0 products found")
            suppliers_empty += 1
            continue

        logger.info(f"  📦 {len(products)} products extracted → saving...")

        batch = await db.save_produits_batch(products)
        total_extracted += batch.get("extracted", 0)
        total_saved     += batch.get("saved",     0)
        total_skipped   += batch.get("skipped",   0)
        total_rejected  += batch.get("rejected",  0)
        total_errors    += batch.get("errors",    0)
        suppliers_done  += 1

    # ── 5. Update run stats ───────────────────────────────────
    await db.update_scraping_run(run_id, {
        "status":                 "COMPLETE",
        "finished_at":            datetime.utcnow(),
        "total_suppliers_found":  len(suppliers),
        "total_products_scraped": total_saved,
    })

    # ── 6. Final summary ──────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ DONE — run_id: {run_id}")
    logger.info(f"   Country          : {canonical_name}")
    logger.info(f"   Suppliers total  : {len(suppliers)}")
    logger.info(f"   Suppliers scraped: {suppliers_done}")
    logger.info(f"   Suppliers empty  : {suppliers_empty}")
    logger.info(f"   Products extracted: {total_extracted}")
    logger.info(f"   ✅ Products saved  : {total_saved}")
    logger.info(f"   ⏭ Products skipped : {total_skipped} (dedup)")
    logger.info(f"   🚫 Products rejected: {total_rejected} (garbage)")
    logger.info(f"   ❌ Products errors  : {total_errors}")
    logger.info("=" * 60)

    if total_saved > 0:
        logger.info(
            f"\n📊 Check results:\n"
            f"   SELECT nom_produit, ville_raw, pays_raw, prix, source\n"
            f"   FROM produits p\n"
            f"   JOIN fournisseurs f ON p.fournisseur_id = f.id\n"
            f"   JOIN pays py ON f.pays_id = py.id\n"
            f"   WHERE py.nom = '{canonical_name}'\n"
            f"   AND p.run_id = '{run_id}'\n"
            f"   ORDER BY p.id DESC;"
        )


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape products from existing fournisseurs by country"
    )
    parser.add_argument(
        "--country", "-c",
        type=str,
        default=DEFAULT_COUNTRY,
        help='Country name matching pays.nom (e.g. France, Belgium, "South Africa")',
    )
    parser.add_argument(
        "--max-products", "-m",
        type=int,
        default=DEFAULT_MAX_PRODUCTS_PER_SUPPLIER,
        help=f"Max products per supplier (default: {DEFAULT_MAX_PRODUCTS_PER_SUPPLIER})",
    )
    parser.add_argument(
        "--list-countries",
        action="store_true",
        help="List all countries that have suppliers in DB",
    )

    args = parser.parse_args()

    # ── list mode ────────────────────────────────────────────
    if args.list_countries:
        async def list_countries():
            db = DatabaseManager()
            async with db.engine.connect() as conn:
                result = await conn.execute(text("""
                    SELECT p.nom, COUNT(f.id) as supplier_count
                    FROM pays p
                    JOIN fournisseurs f ON f.pays_id = p.id
                    GROUP BY p.nom
                    ORDER BY supplier_count DESC
                """))
                rows = result.fetchall()
            print(f"\n{'Country':<30} {'Suppliers':>10}")
            print("-" * 42)
            for row in rows:
                print(f"{row[0]:<30} {row[1]:>10}")
        asyncio.run(list_countries())
        sys.exit(0)

    # ── resolve country ───────────────────────────────────────
    country = args.country.strip()

    if not country:
        # Interactive mode — show list and let user pick
        async def get_countries():
            db = DatabaseManager()
            async with db.engine.connect() as conn:
                result = await conn.execute(text("""
                    SELECT DISTINCT p.nom
                    FROM pays p
                    JOIN fournisseurs f ON f.pays_id = p.id
                    ORDER BY p.nom
                """))
                return [row[0] for row in result.fetchall()]

        all_countries = asyncio.run(get_countries())
        if not all_countries:
            print("❌ No suppliers with API/CM found in DB. Run fournisseur scraper first.")
            sys.exit(1)

        country = pick_country_interactive(all_countries)

    asyncio.run(main(
        country_name = country,
        max_products = args.max_products,
    ))