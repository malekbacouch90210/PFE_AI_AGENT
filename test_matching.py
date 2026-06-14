"""
test_matching.py — Sprint 5 test

Tests the matching pipeline with a small set of known products.
Shows whether the 5-dimension scoring + pgvector works correctly.

Expected results:
  "Transfer from Fiumicino Airport to Rome" → REJECTED_DUPLICATE (exists in DEX)
  "Tromsø Arctic Fox Safari" → ACCEPTED (new product not in DEX)
  "Marrakech Medina Walking Tour" → ACCEPTED or PENDING_REVIEW
  "Louvre Museum Skip-the-Line Ticket" → REJECTED_DUPLICATE (common DEX product)

Usage:
  python test_matching.py
  python test_matching.py --verbose
  python test_matching.py --product-id 4409
"""
import asyncio
import argparse
import json
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
from services.matching.matching_pipeline import (
    MatchingPipeline,
    compute_similarity,
    decide_match_status,
    estimate_price,
    get_dex_candidates,
    get_transfer_direction,
    score_category,
    score_city,
    score_country,
    score_description,
    score_price,
    score_title,
    THRESHOLD_DUPLICATE,
    THRESHOLD_REVIEW,
)


# ─────────────────────────────────────────────────────────────
# Unit tests — scoring functions
# ─────────────────────────────────────────────────────────────

def test_scoring_functions():
    logger.info("🧪 Unit tests — scoring functions")

    # Title scoring
    assert score_title("Transfer from Fiumicino Airport to Rome", "Transfer Fiumicino-Rome", "Transfer Fiumicino Airport to Rome hotel") > 0.6, "Title match failed"
    assert score_title("Arctic Fox Safari Tromsø", "Marrakech Medina Walk", "Marrakech Medina Walking Tour") < 0.3, "Title should be low"
    logger.info("  ✅ title_score")

    # City scoring
    assert score_city("Rome",   "Rome")       == 1.0, "Exact city match failed"
    assert score_city("Rome",   "Roma")       >= 0.8, "Rome/Roma variant failed"
    assert score_city("Fes",    "Fez")        >= 0.6, "Fes/Fez variant failed"
    assert score_city("Tromsø", "Tromso")     >= 0.8, "Accent variant failed"
    assert score_city("Paris",  "Paris CDG")  >= 0.7, "Substring match failed"
    assert score_city("Rome",   "Paris")      == 0.0, "Different cities should be 0"
    assert score_city("",       "Paris")      == 0.0, "Empty city should be 0"
    logger.info("  ✅ city_score")

    # Category scoring
    assert score_category("HISTORIQUE", "CULTUREL") > 0.5, "Related categories failed"
    assert score_category("NATURE", "AVENTURE") > 0.5, "Related categories failed"
    assert score_category("TRANSPORT", "HISTORIQUE") == 0.0, "Unrelated categories failed"
    assert score_category("CULTUREL", "CULTUREL") == 1.0, "Exact category failed"
    logger.info("  ✅ category_score")

    # Price scoring
    assert score_price(100, 100) == 1.0, "Same price failed"
    assert score_price(100, 120) == 1.0, "Within 30% failed"
    assert score_price(100, 300) < 0.5, "3x price failed"
    assert score_price(None, 100) == 0.5, "NULL price should be neutral"
    assert score_price(100, None) == 0.5, "NULL price should be neutral"
    logger.info("  ✅ price_score")

    # Country scoring
    assert score_country("Italy",         "Italy")        == 1.0, "Exact country match failed"
    assert score_country("France",        "France")       == 1.0, "Exact country match failed"
    assert score_country("Italy",         "France")       == 0.0, "Different countries should be 0"
    assert score_country("United States", "United States")== 1.0, "US exact match failed"
    assert score_country("",              "Italy")        == 0.5, "NULL country should be neutral"
    assert score_country("Italy",         "")             == 0.5, "NULL country should be neutral"
    logger.info("  ✅ country_score")

    # Decision thresholds
    assert decide_match_status(0.90)[0] == "REJECTED_DUPLICATE"
    assert decide_match_status(0.70)[0] == "PENDING_REVIEW"
    assert decide_match_status(0.50)[0] == "ACCEPTED"
    logger.info("  ✅ decide_match_status")

    # Transfer direction tests
    assert get_transfer_direction("Transfer from Fiumicino Airport to Rome") == "ARRIVAL",  "ARRIVAL detect failed"
    assert get_transfer_direction("Departure Transfer Rome to Airport") == "DEPARTURE",     "DEPARTURE detect failed"
    assert get_transfer_direction("Rome Hotels to Fiumicino Airport")   == "DEPARTURE",     "DEPARTURE detect failed"
    assert get_transfer_direction("Rome City Tour")                     == "UNKNOWN",       "UNKNOWN failed"

    # Transfer direction CONFLICT → score_title returns 0.1
    arrival_score   = score_title("Transfer from Fiumicino Airport to Rome",
                                  "Private transfer from Rome hotels to Fiumicino Airport", "",
                                  activity_type="TRANSFER")
    same_dir_score  = score_title("Transfer from Fiumicino Airport to Rome",
                                  "Private Arrival Transfer Fiumicino Airport to Hotel", "",
                                  activity_type="TRANSFER")
    assert arrival_score < 0.2,      f"Conflicting directions should score low, got {arrival_score}"
    assert same_dir_score > arrival_score, f"Same direction should score higher than conflict"
    logger.info("  ✅ transfer_direction")

    logger.info("✅ All unit tests passed!")
    return True


# ─────────────────────────────────────────────────────────────
# Integration test — single product
# ─────────────────────────────────────────────────────────────

async def test_single_product(product_id: int, db: DatabaseManager, verbose: bool = False):
    """Test matching for a single scraped product."""
    async with db.engine.connect() as conn:
        r = await conn.execute(text("""
            SELECT
                spn.id, spn.scraped_product_id, spn.fournisseur_id, spn.run_id,
                spn.clean_title, spn.clean_description,
                spn.normalized_city, spn.normalized_country,
                spn.canonical_activity_type, spn.normalized_category,
                spn.estimated_price, spn.duration_minutes,
                spn.embedding, spn.matching_signature
            FROM scraped_products_normalized spn
            WHERE spn.scraped_product_id = :pid
              AND spn.embedding IS NOT NULL
            LIMIT 1
        """), {"pid": product_id})
        row = r.fetchone()

    if not row:
        logger.error(f"  ❌ Product {product_id} not found in scraped_products_normalized or has no embedding")
        return

    scraped = dict(row._mapping)
    logger.info(f"\n🔍 Testing product id={product_id}")
    logger.info(f"   Title   : {scraped.get('clean_title','')}")
    logger.info(f"   City    : {scraped.get('normalized_city','')}")
    logger.info(f"   Country : {scraped.get('normalized_country','')}")
    logger.info(f"   Type    : {scraped.get('canonical_activity_type','')}")
    logger.info(f"   Category: {scraped.get('normalized_category','')}")
    logger.info(f"   Price   : {scraped.get('estimated_price','NULL')}€")

    # Get DEX candidates
    candidates = await get_dex_candidates(scraped, db)
    logger.info(f"\n   📋 DEX candidates found: {len(candidates)}")

    if not candidates:
        logger.info("   → ACCEPTED (no DEX candidates in same city/type)")
        return

    # Score all candidates
    best_score = -1.0
    best_cand  = None
    all_scores = []

    for cand in candidates:
        vector_sim = float(cand.get("cosine_sim") or 0.0)
        scores     = compute_similarity(scraped, cand, vector_sim)
        all_scores.append((scores["similarity_score"], cand, scores))
        if scores["similarity_score"] > best_score:
            best_score = scores["similarity_score"]
            best_cand  = (cand, scores)

    # Sort by score
    all_scores.sort(key=lambda x: x[0], reverse=True)

    if verbose:
        logger.info("\n   🏆 Top 5 candidates:")
        for sim, cand, sc in all_scores[:5]:
            logger.info(
                f"     sim={sim:.3f} | t={sc['title_score']:.2f} city={sc['city_score']:.2f} "
                f"cntry={sc.get('country_score',0):.2f} cat={sc['category_score']:.2f} "
                f"vec={sc['vector_sim']:.2f} | "
                f"{cand.get('clean_title_en') or cand.get('clean_title','')[:40]} "
                f"({cand.get('normalized_city','')},{cand.get('normalized_country','')})"
            )

    # Decision
    match_status, match_type = decide_match_status(best_score)
    cand, scores = best_cand

    icon = {"ACCEPTED": "✅", "REJECTED_DUPLICATE": "❌", "PENDING_REVIEW": "⏳"}[match_status]
    logger.info(f"\n   {icon} DECISION: {match_status} (sim={best_score:.3f})")
    logger.info(f"   Best match DEX: {cand.get('clean_title_en') or cand.get('clean_title','')}")
    logger.info(f"   Scores: title={scores['title_score']:.2f} city={scores['city_score']:.2f} "
                f"country={scores.get('country_score',0):.2f} desc={scores['description_score']:.2f} "
                f"cat={scores['category_score']:.2f} price={scores['price_score']:.2f} "
                f"vec={scores['vector_sim']:.2f}")
    logger.info(f"   Best DEX country: {cand.get('normalized_country','?')}")

    # Price estimation if ACCEPTED and no price
    if match_status == "ACCEPTED" and not scraped.get("estimated_price"):
        dex_prices = [float(c["price"]) for c in candidates if c.get("price") and float(c["price"]) > 0]
        estimated = await estimate_price(scraped, dex_prices)
        if estimated:
            logger.info(f"   💶 Estimated price: {estimated}€")
        else:
            logger.info(f"   💶 Price estimation: failed")


# ─────────────────────────────────────────────────────────────
# Full integration test — small batch
# ─────────────────────────────────────────────────────────────

async def test_full_batch(limit: int, db: DatabaseManager):
    """Test full matching pipeline on a small batch."""
    logger.info(f"\n🚀 Full batch test — {limit} products")

    pipeline = MatchingPipeline(db)
    stats = await pipeline.run(
        limit        = limit,
        run_id       = None,
        groq_reasons = config.groq_configured(),
    )

    logger.info("\n📊 Batch test results:")
    total = stats["total"]
    if total:
        logger.info(f"   Total processed  : {total}")
        logger.info(f"   ✅ ACCEPTED (new): {stats['accepted']} ({stats['accepted']/total*100:.0f}%)")
        logger.info(f"   ❌ DUPLICATE     : {stats['duplicate']} ({stats['duplicate']/total*100:.0f}%)")
        logger.info(f"   ⏳ PENDING       : {stats['pending']} ({stats['pending']/total*100:.0f}%)")
        logger.info(f"   💶 Prices est.   : {stats['price_estimated']}")

    return stats


# ─────────────────────────────────────────────────────────────
# Check prerequisites
# ─────────────────────────────────────────────────────────────

async def check_prerequisites(db: DatabaseManager) -> bool:
    """Verify DEX and scraped products have embeddings."""
    async with db.engine.connect() as conn:
        dex_total = (await conn.execute(
            text("SELECT COUNT(*) FROM dex_products_normalized")
        )).scalar() or 0
        dex_emb = (await conn.execute(
            text("SELECT COUNT(*) FROM dex_products_normalized WHERE embedding IS NOT NULL")
        )).scalar() or 0
        scr_total = (await conn.execute(
            text("SELECT COUNT(*) FROM scraped_products_normalized")
        )).scalar() or 0
        scr_emb = (await conn.execute(
            text("SELECT COUNT(*) FROM scraped_products_normalized WHERE embedding IS NOT NULL")
        )).scalar() or 0
        matched = (await conn.execute(
            text("SELECT COUNT(*) FROM produit_matches")
        )).scalar() or 0

    logger.info("📋 Prerequisites check:")
    logger.info(f"   DEX products total    : {dex_total:,}")
    logger.info(f"   DEX with embeddings   : {dex_emb:,}  {'✅' if dex_emb > 0 else '❌'}")
    logger.info(f"   Scraped total         : {scr_total:,}")
    logger.info(f"   Scraped with embeddings: {scr_emb:,}  {'✅' if scr_emb > 0 else '❌'}")
    logger.info(f"   Already matched       : {matched:,}")

    if dex_emb == 0:
        logger.error(
            "\n❌ DEX products have no embeddings!\n"
            "   Run: python rebuild_dex_normalized.py --only-vendable"
        )
        return False

    if scr_emb == 0:
        logger.error(
            "\n❌ Scraped products have no embeddings!\n"
            "   Run: python run_normalization.py --limit 100"
        )
        return False

    logger.info("\n✅ Prerequisites OK — ready to match")
    return True


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def main(product_id: Optional[int], limit: int, verbose: bool, unit_only: bool):
    logger.info("=" * 60)
    logger.info("🧪 Sprint 5 — Matching Test")
    logger.info("=" * 60)

    # Always run unit tests first
    test_scoring_functions()

    if unit_only:
        return

    db = DatabaseManager()
    await db.create_tables()

    # Check prerequisites
    ok = await check_prerequisites(db)
    if not ok:
        return

    if product_id:
        # Test a specific product
        await test_single_product(product_id, db, verbose=verbose)
    else:
        # Test a small batch
        await test_full_batch(limit, db)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Sprint 5 Matching Test")
    p.add_argument("--product-id", type=int,  default=None,
                   help="Test a specific scraped_product_id")
    p.add_argument("--limit",      type=int,  default=10,
                   help="Batch size for full test (default 10)")
    p.add_argument("--verbose",    action="store_true",
                   help="Show all candidates scores")
    p.add_argument("--unit-only",  action="store_true",
                   help="Run unit tests only (no DB)")
    args = p.parse_args()

    asyncio.run(main(
        product_id = args.product_id,
        limit      = args.limit,
        verbose    = args.verbose,
        unit_only  = args.unit_only,
    ))
