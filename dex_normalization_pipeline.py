#!/usr/bin/env python3
"""
DEX Normalization Pipeline — OPTIMIZED V2
==========================================
- Correct price source: prixappel (real price)
- Concurrent embedding generation (semaphore)
- JSONB passed as Python list (no json.dumps)
- Deterministic country map first, LLM fallback
- Batch size adjustable, progress logging
"""

import asyncio
import re
import json  # Add this with the other imports
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp
from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
import asyncio


try:
    from config import config
    DATABASE_URL = config.DATABASE_URL
except Exception:
    DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/Sourcing"

OLLAMA_URL = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "mxbai-embed-large"
OLLAMA_GENERATE_URL = "http://localhost:11434/api/generate"
LLM_MODEL = "qwen2.5:7b"

# ------------------------------------------------------------
# FAST DETERMINISTIC COUNTRY MAP (first priority)
# ------------------------------------------------------------
ISO_TO_COUNTRY = {
    "fr": "France", "it": "Italy", "es": "Spain", "de": "Germany",
    "gb": "United Kingdom", "us": "United States", "jp": "Japan",
    # add more as needed
}
STATIC_CITY_COUNTRY = {
    "paris": "France", "rome": "Italy", "berlin": "Germany",
    "tokyo": "Japan", "new york": "United States",
}

SERVICE_TYPE_MAP = {
    "TRANSFER": ["transfer", "airport", "shuttle", "taxi"],
    "TICKET": ["ticket", "billet", "entrance", "admission", "pass"],
    "TOUR": ["tour", "excursion", "guided", "sightseeing", "day trip"],
}
AIRPORT_KEYWORDS = ["airport", "aéroport", "aeroport"]

# ------------------------------------------------------------
# ASYNC ENGINE
# ------------------------------------------------------------
engine = create_async_engine(DATABASE_URL, echo=False, pool_size=20)

# ------------------------------------------------------------
# LLM CLIENT (with concurrency)
# ------------------------------------------------------------
class LLMClient:
    def __init__(self, max_concurrent=5):
        self._session = None
        self._city_cache = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def detect_country(self, city: str, context: str = "") -> str:
        if not city:
            return ""
        key = city.lower().strip()
        # 1. Static map
        if key in STATIC_CITY_COUNTRY:
            return STATIC_CITY_COUNTRY[key]
        # 2. Cache
        if key in self._city_cache:
            return self._city_cache[key]
        # 3. LLM (last resort)
        async with self._semaphore:
            prompt = f'What country is the city "{city}" in? Reply ONLY the country name.'
            try:
                session = await self._get_session()
                async with session.post(OLLAMA_GENERATE_URL,
                                        json={"model": LLM_MODEL, "prompt": prompt, "stream": False,
                                              "options": {"temperature": 0.0, "num_predict": 30}},
                                        timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        country = data.get("response", "").strip().strip('"\'')
                        if country and country.lower() not in ("unknown", ""):
                            self._city_cache[key] = country
                            return country
            except Exception:
                pass
        return ""

    async def embed(self, text: str) -> Optional[List[float]]:
        if not text:
            return None
        async with self._semaphore:
            try:
                session = await self._get_session()
                async with session.post(OLLAMA_URL,
                                        json={"model": EMBED_MODEL, "prompt": text[:1500]},
                                        timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        emb = data.get("embedding")
                        if emb and len(emb) > 0:
                            return emb
            except Exception as e:
                logger.debug(f"Embed failed: {e}")
            return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# Change this line at the bottom of the file (where llm is created):
llm = LLMClient(max_concurrent=10)  # Increased from 5 to 10

# ------------------------------------------------------------
# DATA LOADER (fixed price: use prixappel)
# ------------------------------------------------------------
async def load_dex_products(batch_size: int, offset: int) -> List[Dict]:
    sql = r"""
        SELECT
            p.id,
            COALESCE(p.nomen, p.nom, p.nominitial, '')           AS title_raw,
            COALESCE(p.nom, '')                                   AS title_fr,
            COALESCE(p.description, '')                           AS description_raw,
            p.typeprestation,
            p.producttype,
            p.productcategory,
            p.productsubcategory,
            p.productgroup,
            p.categorie,
            COALESCE(p.prixappel, 0)                              AS price,          -- ✅ FIX: real price
            COALESCE(p.prixappelachat, 0)                         AS cost_price,
            COALESCE(p.devise, 'EUR')                             AS devise,
            p.dureeheure,
            p.dureeminute,
            p.adresse,
            p.organisateur,
            COALESCE(loc.nomen, loc.nom, '')                     AS city_name,
            COALESCE(loc.codeiso, '')                             AS country_iso,
            CASE WHEN loc.longitude ~ '^-?[0-9]+\\.?[0-9]*$' THEN loc.longitude::float END AS geo_lng,
            CASE WHEN loc.latitude  ~ '^-?[0-9]+\\.?[0-9]*$' THEN loc.latitude::float  END AS geo_lat,
            COALESCE(MAX(CASE WHEN d.idlangue = 2 THEN d.texte END),
                     MAX(CASE WHEN d.idlangue = 1 THEN d.texte END), MAX(d.texte), '') AS detail_text,
            COALESCE(MAX(CASE WHEN d.idlangue = 2 THEN d.titre END),
                     MAX(CASE WHEN d.idlangue = 1 THEN d.titre END), MAX(d.titre), '') AS detail_title,
            STRING_AGG(DISTINCT COALESCE(t.nomen, t.nom), ' | ') AS themes
        FROM dex_produit p
        LEFT JOIN dex_localite loc ON loc.id = p.idville
        LEFT JOIN dex_detail   d   ON d.idobjet = p.id
        LEFT JOIN dex_produit_theme pt ON pt.idproduit = p.id
        LEFT JOIN dex_theme    t   ON t.id = pt.theme
        GROUP BY p.id, p.nomen, p.nom, p.nominitial, p.description, p.typeprestation,
                 p.producttype, p.productcategory, p.productsubcategory, p.productgroup,
                 p.categorie, p.prixappel, p.prixappelachat, p.devise,
                 p.dureeheure, p.dureeminute, p.adresse, p.organisateur,
                 loc.nomen, loc.nom, loc.codeiso, loc.longitude, loc.latitude
        ORDER BY p.id
        LIMIT :limit OFFSET :offset
    """
    async with engine.connect() as conn:
        result = await conn.execute(text(sql), {"limit": batch_size, "offset": offset})
        rows = result.fetchall()
        keys = result.keys()
        return [dict(zip(keys, row)) for row in rows]

async def count_dex_products() -> int:
    async with engine.connect() as conn:
        r = await conn.execute(text("SELECT COUNT(*) FROM dex_produit"))
        return r.scalar() or 0

async def get_already_normalized_ids() -> set:
    async with engine.connect() as conn:
        r = await conn.execute(text("SELECT dex_product_id FROM dex_products_normalized"))
        return {row[0] for row in r.fetchall()}

# ------------------------------------------------------------
# NORMALIZATION (clean, fast)
# ------------------------------------------------------------
def _clean_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:2000]

def _detect_service_type(text: str) -> str:
    t = text.lower()
    for st, kw in SERVICE_TYPE_MAP.items():
        if any(k in t for k in kw):
            return st
    return "TOUR"

def _extract_route(text: str) -> Tuple[str, str]:
    combined = text.lower()
    # simple pattern for airport transfers
    for kw in AIRPORT_KEYWORDS:
        if kw in combined:
            return kw.upper(), ""
    return "", ""

def _detect_flags(text: str) -> Dict[str, bool]:
    t = text.lower()
    return {
        "is_private": "private" in t or "privé" in t,
        "is_shared": "shared" in t,
        "is_skip_the_line": "skip the line" in t or "fast track" in t,
        "is_guided": "guided" in t or "guide" in t,
        "is_group": "group" in t,
        "is_airport": any(k in t for k in AIRPORT_KEYWORDS),
    }

def _extract_tags(text: str) -> List[str]:
    tags = set()
    for kw in ["airport", "private", "group", "guided", "museum", "food", "walking"]:
        if kw in text.lower():
            tags.add(kw)
    return sorted(tags)

def _build_embedding_text(rec: Dict) -> str:
    parts = []
    if rec.get("service_type"):      parts.append(f"service:{rec['service_type']}")
    if rec.get("experience_family"): parts.append(f"theme:{rec['experience_family']}")
    if rec.get("origin"):            parts.append(f"from:{rec['origin']}")
    if rec.get("destination"):       parts.append(f"to:{rec['destination']}")
    if rec.get("normalized_city"):   parts.append(f"city:{rec['normalized_city']}")
    if rec.get("normalized_country"):parts.append(f"country:{rec['normalized_country']}")
    flags = [k.replace("is_", "") for k in ["is_private","is_guided","is_skip_the_line","is_airport"] if rec.get(k)]
    if flags: parts.append(f"flags:{','.join(flags)}")
    if rec.get("clean_title"):       parts.append(rec["clean_title"])
    if rec.get("clean_description"): parts.append(rec["clean_description"][:150])
    return " | ".join(filter(None, parts))[:2000]


async def normalize_one(row: Dict) -> Dict:
    """
    Normalize a single dex_produit row into DexProductNormalized fields.
    """
    dex_id = row.get("id")
    if dex_id is None:
        raise ValueError("Row has no id")

    # ── TITLE with fallbacks ────────────────────────────────────────────────
    raw_title = row.get("detail_title") or row.get("title_raw") or row.get("title_fr") or ""
    clean_title = _clean_html(raw_title)[:500] if raw_title else f"DEX Product #{dex_id}"

    # ── DESCRIPTION ──────────────────────────────────────────────────────────
    desc = row.get("detail_text") or row.get("description_raw") or ""
    clean_desc = _clean_html(desc)[:1000] if desc else ""

    # ── CITY / COUNTRY with null checks ───────────────────────────────────────
    city = row.get("city_name") or ""
    city = city.strip() if city else ""

    country_iso = row.get("country_iso") or ""
    country_iso = country_iso.strip().lower() if country_iso else ""
    country = ISO_TO_COUNTRY.get(country_iso, "")

    # Try to parse adresse field (safe)
    adresse = row.get("adresse") or ""
    adresse = adresse.strip() if adresse else ""

    if adresse and (not city or not country):
        if " - " in adresse:
            parts = adresse.split(" - ", 1)
            if not country and len(parts) > 0:
                country = parts[0].strip()
            if not city and len(parts) > 1:
                city = parts[1].strip()
        elif "," in adresse:
            parts = adresse.split(",", 1)
            if not city and len(parts) > 0:
                city = parts[0].strip()
            if not country and len(parts) > 1:
                country = parts[-1].strip()

    # LLM fallback for country (only if city exists)
    if city and not country:
        country = await llm.detect_country(city, clean_title)
    if country and not city and adresse:
        city = adresse.split(",")[0].strip() if "," in adresse else adresse

    # ── SERVICE TYPE ─────────────────────────────────────────────────────────
    themes = row.get("themes") or ""
    combined = f"{clean_title} {clean_desc} {themes}"
    service_type = _detect_service_type(combined)

    # typeprestation override (safe)
    tp = row.get("typeprestation") or ""
    tp = str(tp).lower() if tp else ""
    if "449" in tp or "ticket" in tp or "billet" in tp:
        service_type = "TICKET"
    elif tp in ("3", "4") or "transfer" in tp or "transport" in tp:
        service_type = "TRANSFER"

    # ── EXPERIENCE FAMILY ────────────────────────────────────────────────────
    experience_family = "TOURS_SIGHTSEEING"
    combined_lower = combined.lower()
    if "food" in combined_lower or "wine" in combined_lower:
        experience_family = "FOOD_DRINK"
    elif "museum" in combined_lower or "culture" in combined_lower:
        experience_family = "CULTURE"

    # ── ROUTE ────────────────────────────────────────────────────────────────
    origin, destination = _extract_route(combined)
    route_signature = ""
    if origin and destination:
        route_signature = f"{origin.upper().replace(' ', '_')}→{destination.upper().replace(' ', '_')}"
    elif origin:
        route_signature = origin.upper().replace(" ", "_")

    # ── FLAGS ────────────────────────────────────────────────────────────────
    flags = _detect_flags(combined)

    # ── PRICE (safe) ─────────────────────────────────────────────────────────
    price_val = row.get("price")
    if price_val is None or price_val == 0:
        # Fallback to prixappel from row
        price_val = row.get("prixappel") or 0.0
    try:
        price = float(price_val)
    except (TypeError, ValueError):
        price = 0.0

    # ── CURRENCY ─────────────────────────────────────────────────────────────
    currency = row.get("devise") or "EUR"
    if isinstance(currency, int):
        # Convert numeric currency codes (403 = EUR)
        currency_map = {403: "EUR", 404: "USD", 405: "GBP", 406: "CHF", 407: "CHF"}
        currency = currency_map.get(currency, "EUR")
    currency = str(currency)[:10]

    # ── TAGS ─────────────────────────────────────────────────────────────────
    activity_tags = _extract_tags(combined)

    # ── NORMALIZED CATEGORY (safe) ───────────────────────────────────────────
    productcategory = row.get("productcategory") or ""
    producttype = row.get("producttype") or ""
    categorie = row.get("categorie") or ""

    normalized_category = (
                                  productcategory or
                                  producttype or
                                  categorie or
                                  experience_family
                          ) or "Tours & Sightseeing"

    normalized_category = str(normalized_category)[:150]

    # ── SERVICE SUBTYPE (safe) ───────────────────────────────────────────────
    service_subtype = row.get("service_subtype") or ""
    service_subtype = str(service_subtype)[:100] if service_subtype else ""

    # ── SUPPLIER (safe) ─────────────────────────────────────────────────────
    supplier_name = row.get("organisateur") or ""
    supplier_name = str(supplier_name)[:255] if supplier_name else ""

    # ── SUB CATEGORY, PRODUCT TYPE, GROUP (safe) ─────────────────────────────
    subcategory = row.get("productsubcategory") or ""
    subcategory = str(subcategory)[:150] if subcategory else ""

    product_type = row.get("producttype") or ""
    product_type = str(product_type)[:100] if product_type else ""

    product_group = row.get("productgroup") or ""
    product_group = str(product_group)[:150] if product_group else ""

    # ── ACTIVE / VENDABLE (safe) ─────────────────────────────────────────────
    is_active = row.get("actif", 1) == 1
    is_vendable = row.get("vendable", 1) == 1

    # ── GEO (safe) ──────────────────────────────────────────────────────────
    geo_lat = row.get("geo_lat")
    geo_lng = row.get("geo_lng")
    if geo_lat is not None:
        try:
            geo_lat = float(geo_lat)
        except (TypeError, ValueError):
            geo_lat = None
    if geo_lng is not None:
        try:
            geo_lng = float(geo_lng)
        except (TypeError, ValueError):
            geo_lng = None

    # ── CONFIDENCE ───────────────────────────────────────────────────────────
    confidence = 0.0
    if clean_title:     confidence += 0.20
    if clean_desc:      confidence += 0.10
    if city:            confidence += 0.20
    if country:         confidence += 0.15
    if service_type:    confidence += 0.10
    if price > 0:       confidence += 0.10
    if supplier_name:   confidence += 0.10
    confidence = min(round(confidence, 2), 1.0)

    # ── BUILD EMBEDDING TEXT ─────────────────────────────────────────────────
    embedding_text = _build_embedding_text({
        "service_type": service_type,
        "experience_family": experience_family,
        "origin": origin,
        "destination": destination,
        "normalized_city": city,
        "normalized_country": country,
        "is_private": flags["is_private"],
        "is_guided": flags["is_guided"],
        "is_skip_the_line": flags["is_skip_the_line"],
        "is_airport": flags["is_airport"],
        "clean_title": clean_title,
        "clean_description": clean_desc,
    })

    # ── BUILD RECORD ─────────────────────────────────────────────────────────
    rec = {
        "dex_product_id": dex_id,
        "clean_title": clean_title,
        "clean_description": clean_desc,
        "chapeau": None,
        "normalized_city": city[:150] if city else "",
        "normalized_country": country[:100] if country else "",
        "country_iso": country_iso[:10] if country_iso else "",
        "price": price,
        "currency": currency,
        "is_promo": False,
        "category": normalized_category,
        "subcategory": subcategory,
        "product_type": product_type,
        "product_group": product_group,
        "service_type": service_type,
        "service_subtype": service_subtype,
        "experience_family": experience_family[:100],
        "origin": origin[:255] if origin else "",
        "destination": destination[:255] if destination else "",
        "route_signature": route_signature[:300] if route_signature else "",
        "is_private": flags["is_private"],
        "is_shared": flags["is_shared"],
        "is_skip_the_line": flags["is_skip_the_line"],
        "is_guided": flags["is_guided"],
        "is_group": flags["is_group"],
        "is_airport": flags["is_airport"],
        "is_active": is_active,
        "is_vendable": is_vendable,
        "has_pickup": False,
        "has_guide": False,
        "has_meal": False,
        "has_transport": False,
        "activity_tags": activity_tags,
        "available_languages": None,
        "supplier_name": supplier_name,
        "supplier_operator": supplier_name,
        "supplier_id": row.get("idfournisseur"),
        "geo_lat": geo_lat,
        "geo_lng": geo_lng,
        "source_language": "fr",
        "normalization_confidence": confidence,
        "embedding_text": embedding_text,
        "source": "DEX_NATIVE",
    }

    return rec
# ------------------------------------------------------------
# DB UPSERT (uses real price)
# ------------------------------------------------------------
UPSERT_SQL = """
INSERT INTO dex_products_normalized (
    dex_product_id, clean_title, clean_description, chapeau,
    normalized_city, normalized_country, country_iso,
    price, currency, is_promo,
    category, subcategory, product_type, product_group,
    service_type, service_subtype, experience_family,
    origin, destination, route_signature,
    is_private, is_shared, is_skip_the_line,
    is_guided, is_group, is_airport,
    is_active, is_vendable,
    has_pickup, has_guide, has_meal, has_transport,
    activity_tags, available_languages,
    supplier_name, supplier_operator, supplier_id,
    geo_lat, geo_lng,
    source_language, normalization_confidence,
    embedding_text, embedding,
    source, created_at, updated_at
) VALUES (
    :dex_product_id, :clean_title, :clean_description, :chapeau,
    :normalized_city, :normalized_country, :country_iso,
    :price, :currency, :is_promo,
    :category, :subcategory, :product_type, :product_group,
    :service_type, :service_subtype, :experience_family,
    :origin, :destination, :route_signature,
    :is_private, :is_shared, :is_skip_the_line,
    :is_guided, :is_group, :is_airport,
    :is_active, :is_vendable,
    :has_pickup, :has_guide, :has_meal, :has_transport,
    :activity_tags, :available_languages,
    :supplier_name, :supplier_operator, :supplier_id,
    :geo_lat, :geo_lng,
    :source_language, :normalization_confidence,
    :embedding_text, :embedding,
    :source, NOW(), NOW()
)
ON CONFLICT (dex_product_id) DO UPDATE SET
    clean_title = EXCLUDED.clean_title,
    clean_description = EXCLUDED.clean_description,
    chapeau = EXCLUDED.chapeau,
    normalized_city = EXCLUDED.normalized_city,
    normalized_country = EXCLUDED.normalized_country,
    country_iso = EXCLUDED.country_iso,
    price = EXCLUDED.price,
    currency = EXCLUDED.currency,
    is_promo = EXCLUDED.is_promo,
    category = EXCLUDED.category,
    subcategory = EXCLUDED.subcategory,
    product_type = EXCLUDED.product_type,
    product_group = EXCLUDED.product_group,
    service_type = EXCLUDED.service_type,
    service_subtype = EXCLUDED.service_subtype,
    experience_family = EXCLUDED.experience_family,
    origin = EXCLUDED.origin,
    destination = EXCLUDED.destination,
    route_signature = EXCLUDED.route_signature,
    is_private = EXCLUDED.is_private,
    is_shared = EXCLUDED.is_shared,
    is_skip_the_line = EXCLUDED.is_skip_the_line,
    is_guided = EXCLUDED.is_guided,
    is_group = EXCLUDED.is_group,
    is_airport = EXCLUDED.is_airport,
    is_active = EXCLUDED.is_active,
    is_vendable = EXCLUDED.is_vendable,
    has_pickup = EXCLUDED.has_pickup,
    has_guide = EXCLUDED.has_guide,
    has_meal = EXCLUDED.has_meal,
    has_transport = EXCLUDED.has_transport,
    activity_tags = EXCLUDED.activity_tags,
    available_languages = EXCLUDED.available_languages,
    supplier_name = EXCLUDED.supplier_name,
    supplier_operator = EXCLUDED.supplier_operator,
    supplier_id = EXCLUDED.supplier_id,
    geo_lat = EXCLUDED.geo_lat,
    geo_lng = EXCLUDED.geo_lng,
    source_language = EXCLUDED.source_language,
    normalization_confidence = EXCLUDED.normalization_confidence,
    embedding_text = EXCLUDED.embedding_text,
    embedding = EXCLUDED.embedding,
    updated_at = NOW()
"""

ADD_UNIQUE_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_dex_products_normalized_dex_product_id') THEN
        ALTER TABLE dex_products_normalized ADD CONSTRAINT uq_dex_products_normalized_dex_product_id UNIQUE (dex_product_id);
    END IF;
END$$;
"""


async def save_batch(records: List[Tuple[Dict, Optional[List[float]]]]):
    """Upsert a batch of (normalized_record, embedding) into dex_products_normalized."""
    async with engine.begin() as conn:
        for rec, embedding in records:
            # Convert Python lists to JSON strings for JSONB columns
            activity_tags_value = rec.get("activity_tags", [])
            if isinstance(activity_tags_value, list):
                activity_tags_value = json.dumps(activity_tags_value)

            available_languages_value = rec.get("available_languages", [])
            if isinstance(available_languages_value, list):
                available_languages_value = json.dumps(available_languages_value) if available_languages_value else "[]"

            # Convert embedding to PostgreSQL vector string
            emb_str = None
            if embedding:
                emb_str = "[" + ",".join(str(v) for v in embedding) + "]"

            params = {
                **rec,
                "embedding": emb_str,
                "activity_tags": activity_tags_value,
                "available_languages": available_languages_value,
            }

            await conn.execute(text(UPSERT_SQL), params)


# ------------------------------------------------------------
# MAIN PIPELINE (CONCURRENT EMBEDDING - FIXED)
# ------------------------------------------------------------
async def run_pipeline(batch_size: int = 100, max_products: int = 0,
                       resume: bool = True, test_mode: bool = False,
                       max_concurrent: int = 10):  # Add this parameter
    logger.info("=" * 65)
    logger.info("🚀 DEX Normalization Pipeline v2 (OPTIMIZED - CONCURRENT)")
    logger.info(f"   batch={batch_size}  resume={resume}  test={test_mode}  concurrent={max_concurrent}")

    if not test_mode:
        async with engine.begin() as conn:
            await conn.execute(text(ADD_UNIQUE_SQL))

    total = await count_dex_products()
    logger.info(f"📊 Total dex_produit rows: {total:,}")

    already_done = set()
    if resume and not test_mode:
        already_done = await get_already_normalized_ids()
        logger.info(f"⏩ Already normalized: {len(already_done):,} — will skip")

    limit = min(max_products, total) if max_products else total
    offset = 0
    processed = saved = errors = embed_ok = embed_fail = 0
    start = datetime.now()

    # Semaphore to control concurrency
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_one(row):
        async with semaphore:
            try:
                rec = await normalize_one(row)
                embedding = await llm.embed(rec["embedding_text"])
                return rec, embedding
            except Exception as e:
                logger.error(f"❌ Failed row {row['id']}: {e}")
                return None, None

    while offset < limit:
        fetch_limit = min(batch_size, limit - offset)
        rows = await load_dex_products(fetch_limit, offset)
        if not rows:
            break

        if resume and not test_mode:
            rows = [r for r in rows if r["id"] not in already_done]

        if not rows:
            offset += fetch_limit
            continue

        # Process ALL rows in this batch CONCURRENTLY
        tasks = [process_one(row) for row in rows]
        results = await asyncio.gather(*tasks)

        # Collect results
        batch_to_save = []
        for rec, embedding in results:
            if rec is not None:
                if embedding:
                    embed_ok += 1
                else:
                    embed_fail += 1
                batch_to_save.append((rec, embedding))
                processed += 1
            else:
                errors += 1

        # Save batch
        if not test_mode and batch_to_save:
            try:
                await save_batch(batch_to_save)
                saved += len(batch_to_save)
            except Exception as e:
                logger.error(f"❌ save_batch failed: {e}")
                errors += len(batch_to_save)

        offset += fetch_limit
        elapsed = (datetime.now() - start).total_seconds()
        rate = processed / max(elapsed / 60, 0.01)
        eta = (limit - offset) / max(rate, 0.01)
        logger.info(
            f"📦 {offset:,}/{limit:,} | saved={saved} embed✅={embed_ok} embed❌={embed_fail} errors={errors} | {rate:.0f}/min | ETA {eta:.1f}min")

        if test_mode and processed >= 5:
            break

    await llm.close()
    elapsed = (datetime.now() - start).total_seconds()
    logger.info(
        f"✅ DONE — processed={processed} saved={saved} embedOK={embed_ok} embedFAIL={embed_fail} errors={errors} — {elapsed:.1f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--max", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()

    asyncio.run(run_pipeline(
        batch_size=args.batch,
        max_products=args.max,
        resume=args.resume,
        test_mode=args.test,
        max_concurrent=15,  # Add this parameter
    ))