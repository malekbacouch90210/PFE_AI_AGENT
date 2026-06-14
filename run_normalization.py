#!/usr/bin/env python3

import asyncio
import argparse
import re
import sys
from pathlib import Path
from typing import Optional, Tuple

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import httpx
from loguru import logger
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent))

from config import config
from database import DatabaseManager
from services.ingestion.normalization_pipeline import NormalizationPipeline


# -------------------------------------------------------------------
# Database setup: UNIQUE constraint (no pg_trgm)
# -------------------------------------------------------------------
async def ensure_unique_constraint(db: DatabaseManager):
    """Add UNIQUE constraint on scraped_product_id (if missing)."""
    async with db.engine.begin() as conn:
        r = await conn.execute(text("""
            SELECT COUNT(*) FROM pg_constraint
            WHERE conname = 'scraped_products_normalized_scraped_product_id_key'
              AND conrelid = 'scraped_products_normalized'::regclass
        """))
        row = r.fetchone()
        exists = row[0] > 0 if row else False
        if not exists:
            logger.info("  🔧 Adding UNIQUE constraint on scraped_product_id...")
            # Remove duplicates (keep latest)
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
            logger.info("  ✅ UNIQUE constraint added")
        else:
            logger.info("  ✅ UNIQUE constraint already exists")


# -------------------------------------------------------------------
# Step 1a — SQL exact match corrections (DISABLED)
# -------------------------------------------------------------------
# This function is kept for reference but NOT called.
# It would update ville_raw / pays_raw in produits, which we do NOT want.
async def sql_prematch_disabled(db: DatabaseManager) -> int:
    logger.info("  ⏭️ Skipping SQL pre‑match (would modify ville_raw/pays_raw)")
    return 0


# -------------------------------------------------------------------
# Step 1b — Translation (qwen2.5:7b) — unchanged
# Does NOT touch ville_raw or pays_raw.
# -------------------------------------------------------------------
_ENGLISH_DETECTION_WORDS = {
    "the","a","an","and","or","for","to","in","of","is","are","with",
    "from","on","at","tour","trip","visit","day","half","full","private",
    "guided","city","walk","experience","adventure","discover","explore",
}

def _is_likely_english(text: str) -> bool:
    if not text or len(text) < 5:
        return True
    words = set(re.findall(r"\b[a-z]+\b", text.lower()))
    if not words:
        return False
    overlap = len(words & _ENGLISH_DETECTION_WORDS)
    return (overlap / len(words)) > 0.25

async def translate_to_english(text: str, model: str) -> str:
    if not text or _is_likely_english(text):
        return text
    prompt = (
        f"Translate this tourism product title/description to English.\n"
        f"Keep proper nouns (city names, attraction names) unchanged.\n"
        f"Return ONLY the English translation, nothing else.\n\n"
        f"Text: {text[:500]}"
    )
    try:
        async with httpx.AsyncClient(timeout=config.OLLAMA_TIMEOUT) as client:
            resp = await client.post(
                f"{config.OLLAMA_URL}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            result = resp.json().get("response", "").strip()
            result = re.sub(r"<think>.*?</think>", "", result, flags=re.DOTALL).strip()
            if result and len(result) > 3:
                return result
    except Exception as e:
        logger.debug(f"    Translation error: {e}")
    return text

async def translation_prematch(db: DatabaseManager) -> int:
    logger.info("🌐 Step 1b: Translation pre‑match...")
    async with db.engine.begin() as conn:
        await conn.execute(text("""
            ALTER TABLE produits
            ADD COLUMN IF NOT EXISTS nom_produit_en TEXT,
            ADD COLUMN IF NOT EXISTS description_en TEXT
        """))
    async with db.engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT id, nom_produit, description
            FROM produits
            WHERE nom_produit_en IS NULL
              AND (nom_produit IS NOT NULL OR description IS NOT NULL)
            LIMIT 500
        """))
        rows = result.fetchall()
    if not rows:
        logger.info("  ✅ No translations needed")
        return 0
    logger.info(f"  📋 {len(rows)} rows to check for translation")
    model = config.OLLAMA_MODEL_CLASSIFY
    translated = 0
    for row in rows:
        pid  = row[0]
        nom  = (row[1] or "").strip()
        desc = (row[2] or "").strip()
        nom_en  = await translate_to_english(nom,  model)
        desc_en = await translate_to_english(desc, model)
        async with db.engine.begin() as conn:
            await conn.execute(
                text("UPDATE produits SET nom_produit_en = :n, description_en = :d WHERE id = :pid"),
                {"n": nom_en[:500], "d": desc_en[:2000], "pid": pid},
            )
        if nom_en != nom or desc_en != desc:
            translated += 1
    logger.info(f"  ✅ {translated} rows translated")
    return translated


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
async def main(limit: int, run_id: Optional[str], check_only: bool, sql_only: bool):
    logger.info("=" * 60)
    logger.info("Phase 2 - Normalization Pipeline Phase 3 - embeddig pipeline")
    logger.info("  ⚠️  ville_raw / pays_raw in produits will NOT be updated")
    logger.info(f"  Ollama  : {config.OLLAMA_URL}")
    logger.info(f"  Model classify : {config.OLLAMA_MODEL_CLASSIFY}")
    logger.info(f"  Model embed    : {config.OLLAMA_MODEL_EMBED}")
    logger.info("=" * 60)

    # Health check
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{config.OLLAMA_URL}/api/tags")
            logger.info("✅ Ollama ready" if r.status_code == 200 else f"⚠️ Ollama {r.status_code}")
    except Exception as e:
        logger.error(f"❌ Ollama not reachable: {e}")
        if check_only:
            return

    if check_only:
        return

    db = DatabaseManager()
    await db.create_tables()
    await ensure_unique_constraint(db)

    # Step 1a — disabled
    # await sql_prematch(db)   # ← would modify ville_raw/pays_raw
    logger.info("⏭️  Step 1a: SQL pre‑match (exact) SKIPPED - no updates to ville_raw/pays_raw")

    # Step 1b
    await translation_prematch(db)

    if sql_only:
        logger.info("✅ Pre‑match steps complete (--sql-only)")
        return

    # Normalization pipeline (exact lookups only, reads from produits but does NOT write to it)
    pipeline = NormalizationPipeline(db)
    await pipeline.run(limit=limit, run_id=run_id)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Phase 2 Normalization and phase 3 embedding")
    p.add_argument("--limit",    type=int,  default=100, help="Number of products to process (default 100)")
    p.add_argument("--run-id",   type=str,  default=None, help="Filter by specific run_id")
    p.add_argument("--check",    action="store_true", help="Only check Ollama and exit")
    p.add_argument("--sql-only", action="store_true", help="Execute only translation, skip final normalization")
    args = p.parse_args()

    asyncio.run(main(args.limit, args.run_id, args.check, args.sql_only))