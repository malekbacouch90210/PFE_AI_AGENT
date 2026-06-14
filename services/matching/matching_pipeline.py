import asyncio
import json
import math
import re
import sys
from typing import Dict, List, Optional, Tuple

import httpx
from loguru import logger
from sqlalchemy import text

from config import config
from database import DatabaseManager

# ─────────────────────────────────────────────────────────────
# Thresholds (Layer 5)
# ─────────────────────────────────────────────────────────────

THRESHOLD_DUPLICATE     = 0.6


# Max DEX candidates to compare per scraped product
MAX_CANDIDATES = 30

# Price estimation: max price difference ratio to score as "similar"
PRICE_RATIO_MAX = 3.0   # prices more than 3× apart → score 0


# ─────────────────────────────────────────────────────────────
# Layer 3: PostgreSQL candidate retrieval
# ─────────────────────────────────────────────────────────────

async def get_dex_candidates(
    scraped: Dict,
    db: DatabaseManager,
) -> List[Dict]:
    """
    Layer 3: Fast SQL pre-filter + pgvector cosine search.

    Step 1: Pre-filter by city + activity_type (SQL)
            Reduces 24k DEX products to ~50-200 candidates
    Step 2: Sort by vector distance (<=> cosine)
            Returns top MAX_CANDIDATES most similar

    Uses dex_products_normalized.embedding (mxbai-embed-large 1024-dim)
    """
    city    = scraped.get("normalized_city") or ""
    atype   = scraped.get("canonical_activity_type") or ""
    country = scraped.get("normalized_country") or ""
    emb     = scraped.get("embedding")

    if not emb:
        return []

    try:
        async with db.engine.connect() as conn:
            # Strategy 1: city + activity_type + vector distance
            if city and atype:
                r = await conn.execute(text("""
                    SELECT
                        d.id                    AS dex_norm_id,
                        d.dex_product_id,
                        d.clean_title,
                        d.clean_title_en,
                        d.clean_description,
                        d.normalized_city,
                        d.normalized_country,
                        d.canonical_activity_type,
                        d.category,
                        d.price,
                        d.duration_minutes,
                        d.embedding,
                        d.matching_signature,
                        d.odoo_id,
                        1 - (d.embedding <=> CAST(:emb AS vector(1024))) AS cosine_sim
                    FROM dex_products_normalized d
                    WHERE d.embedding IS NOT NULL
                      AND LOWER(d.normalized_city)    = LOWER(:city)
                      AND d.canonical_activity_type   = :atype
                    ORDER BY d.embedding <=> CAST(:emb AS vector(1024))
                    LIMIT :limit
                """), {
                    "emb":   str(emb),
                    "city":  city,
                    "atype": atype,
                    "limit": MAX_CANDIDATES,
                })
                rows = r.fetchall()

                if rows:
                    return [dict(r._mapping) for r in rows]

            # Strategy 2: country + activity_type (wider net)
            if country and atype:
                r = await conn.execute(text("""
                    SELECT
                        d.id AS dex_norm_id,
                        d.dex_product_id,
                        d.clean_title,
                        d.clean_title_en,
                        d.clean_description,
                        d.normalized_city,
                        d.normalized_country,
                        d.canonical_activity_type,
                        d.category,
                        d.price,
                        d.duration_minutes,
                        d.embedding,
                        d.matching_signature,
                        d.odoo_id,
                        1 - (d.embedding <=> CAST(:emb AS vector(1024))) AS cosine_sim
                    FROM dex_products_normalized d
                    WHERE d.embedding IS NOT NULL
                      AND LOWER(d.normalized_country) = LOWER(:country)
                      AND d.canonical_activity_type   = :atype
                    ORDER BY d.embedding <=> CAST(:emb AS vector(1024))
                    LIMIT :limit
                """), {
                    "emb":     str(emb),
                    "country": country,
                    "atype":   atype,
                    "limit":   MAX_CANDIDATES,
                })
                rows = r.fetchall()
                if rows:
                    return [dict(r._mapping) for r in rows]

            # Strategy 3: activity_type only (global search, last resort)
            if atype:
                r = await conn.execute(text("""
                    SELECT
                        d.id AS dex_norm_id,
                        d.dex_product_id,
                        d.clean_title,
                        d.clean_title_en,
                        d.clean_description,
                        d.normalized_city,
                        d.normalized_country,
                        d.canonical_activity_type,
                        d.category,
                        d.price,
                        d.duration_minutes,
                        d.embedding,
                        d.matching_signature,
                        d.odoo_id,
                        1 - (d.embedding <=> CAST(:emb AS vector(1024))) AS cosine_sim
                    FROM dex_products_normalized d
                    WHERE d.embedding IS NOT NULL
                      AND d.canonical_activity_type = :atype
                    ORDER BY d.embedding <=> CAST(:emb AS vector(1024))
                    LIMIT :limit
                """), {
                    "emb":   str(emb),
                    "atype": atype,
                    "limit": MAX_CANDIDATES // 2,
                })
                rows = r.fetchall()
                return [dict(r._mapping) for r in rows]

    except Exception as e:
        logger.error(f"  get_dex_candidates error: {e}")

    return []


# ─────────────────────────────────────────────────────────────
# 5-dimension scoring (Layer 5 — deterministic)
# ─────────────────────────────────────────────────────────────

def get_transfer_direction(title: str) -> str:
    """
    Detect transfer direction: ARRIVAL (airport→city) or DEPARTURE (city→airport).

    FIX: "to" and "from" carry the ENTIRE MEANING for transfers.
    Without this, "Airport to Rome" matches "Rome to Airport" with high score.

    Returns: "ARRIVAL" | "DEPARTURE" | "UNKNOWN"

    Examples:
      "Transfer from Fiumicino Airport to Rome"  → ARRIVAL
      "Departure Transfer Rome Hotel to Airport" → DEPARTURE
      "Private transfer Orly Airport to Paris"   → ARRIVAL
      "Transfer from Paris to CDG Airport"       → DEPARTURE
    """
    t = title.lower()

    # Pattern 1: airport then "to" (→ going TO city = ARRIVAL)
    if re.search(r"airport.*?\bto\b", t):
        return "ARRIVAL"

    # Pattern 2: "to" then airport (→ going TO airport = DEPARTURE)
    if re.search(r"\bto\b.*?airport", t):
        return "DEPARTURE"

    # Pattern 3: explicit keywords
    if any(kw in t for kw in ["arrival transfer", "arriving", "from airport", "airport pickup"]):
        return "ARRIVAL"
    if any(kw in t for kw in ["departure transfer", "departing", "to airport", "airport dropoff", "drop-off"]):
        return "DEPARTURE"

    return "UNKNOWN"


def score_title(scraped_title: str, dex_title: str, dex_title_en: str,
                activity_type: str = "") -> float:
    """
    Title similarity — keyword Jaccard + transfer direction awareness.
    35% weight in final score (reduced from 40% to make room for country_score).

    FIX: For TRANSFER products, "to" and "from" are NOT stop words.
    They define the direction (arrival vs departure) and must be kept.
    A hard 0.0 is returned if transfer directions are opposite.
    """
    # ── TRANSFER DIRECTION CHECK (before any scoring) ─────────
    if activity_type == "TRANSFER":
        dir_scraped = get_transfer_direction(scraped_title)
        dir_dex_fr  = get_transfer_direction(dex_title or "")
        dir_dex_en  = get_transfer_direction(dex_title_en or "")
        dir_dex     = dir_dex_fr if dir_dex_fr != "UNKNOWN" else dir_dex_en

        # Both directions known and they CONFLICT → hard penalty
        if dir_scraped != "UNKNOWN" and dir_dex != "UNKNOWN":
            if dir_scraped != dir_dex:
                return 0.1   # 0.1 not 0.0 — still some shared keywords (airport, city name)

    # ── KEYWORD SCORING ───────────────────────────────────────
    def keywords(text: str, is_transfer: bool = False) -> set:
        if not text:
            return set()
        # For transfers: keep "to" and "from" — they carry directional meaning
        # For non-transfers: remove them as generic stop words
        STOP_BASE = {"the","a","an","and","or","of","in","for","with","by","on","at",
                     "tour","tours","trip","visit","private","guided"}
        STOP_DIRECTION = {"to","from"}  # only remove for non-transfers
        stop = STOP_BASE if is_transfer else STOP_BASE | STOP_DIRECTION

        return {w.lower() for w in re.findall(r"\b[a-z]{3,}\b", text.lower())
                if w.lower() not in stop}

    is_t  = activity_type == "TRANSFER"
    s_kw  = keywords(scraped_title, is_t)
    d_kw  = keywords(dex_title, is_t)
    de_kw = keywords(dex_title_en, is_t)
    dex_kw = d_kw | de_kw

    if not s_kw or not dex_kw:
        return 0.0

    intersection = s_kw & dex_kw
    union        = s_kw | dex_kw
    jaccard      = len(intersection) / len(union)

    # Substring bonus
    bonus    = 0.0
    s_lower  = scraped_title.lower()
    d_lower  = (dex_title or "").lower()
    de_lower = (dex_title_en or "").lower()
    if d_lower  and d_lower  in s_lower: bonus = 0.1
    if de_lower and de_lower in s_lower: bonus = 0.1

    return min(1.0, jaccard + bonus)


def score_city(scraped_city: str, dex_city: str) -> float:
    """
    City similarity — character-level using SequenceMatcher + unicode normalization.
    20% weight.

    Handles: Rome/Roma, Fes/Fez, Tromso/Tromsø, Marrakech/Marrakesh, Seoul/Seoul
    Also handles: "Paris" in "Paris Charles de Gaulle Airport"

    Strategy:
      1. Exact match after normalization → 1.0
      2. SequenceMatcher ratio on ASCII-normalized strings
      3. Substring check for airport/region variants
    """
    import unicodedata
    from difflib import SequenceMatcher

    if not scraped_city or not dex_city:
        return 0.0

    # Normalize: strip accents, lowercase (Tromsø→troms, but Tromso→tromso)
    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s.strip())
        s = s.encode("ascii", "ignore").decode("ascii").lower()
        return s

    s_raw = scraped_city.lower().strip()
    d_raw = dex_city.lower().strip()

    # Exact match (raw)
    if s_raw == d_raw:
        return 1.0

    # Exact match after accent stripping
    s = _norm(scraped_city)
    d = _norm(dex_city)
    if s == d:
        return 1.0

    # Substring check (e.g. "Paris" in "Paris Charles de Gaulle")
    if s and d and (s in d or d in s):
        return 0.85

    # SequenceMatcher on ASCII-normalized strings
    ratio = SequenceMatcher(None, s, d).ratio()
    if ratio >= 0.80:   return 1.0    # very close: Roma/Rome, Tromso/Tromsø
    if ratio >= 0.65:   return 0.85   # close: Fes/Fez (ratio=0.667)
    if ratio >= 0.50:   return 0.60   # partial

    return 0.0


def score_country(scraped_country: str, dex_country: str) -> float:
    """
    Country match — exact after normalization.
    10% weight. Added as 6th dimension per user request.

    Exact match → 1.0
    No match    → 0.0
    NULL either side → 0.5 (neutral — don't penalise missing data)
    """
    import unicodedata

    if not scraped_country or not dex_country:
        return 0.5   # neutral — missing data

    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s.strip())
        return s.encode("ascii", "ignore").decode("ascii").lower()

    if _norm(scraped_country) == _norm(dex_country):
        return 1.0
    return 0.0


def score_description(scraped_desc: str, dex_desc: str) -> float:
    """
    Description keyword overlap.
    15% weight.
    """
    def keywords(text: str) -> set:
        if not text:
            return set()
        return {w.lower() for w in re.findall(r"\b[a-z]{4,}\b", text.lower())}

    s_kw = keywords(scraped_desc)
    d_kw = keywords(dex_desc)
    if not s_kw or not d_kw:
        return 0.0
    intersection = s_kw & d_kw
    union        = s_kw | d_kw
    return len(intersection) / len(union)


def score_category(scraped_cat: str, dex_cat: str) -> float:
    """
    Category match.
    15% weight.

    FIX: DEX uses French category names (from dex_products_normalized.category
    which comes from dex_produit.ProductCategory). Must handle:
      scraped TRANSPORT vs DEX "Transferts", "Transfer", "transfert"
      scraped CULTUREL  vs DEX "Tours culturels", "Visite culturelle"
    Also: NULL on either side → neutral 0.3
    """
    if not scraped_cat or not dex_cat:
        return 0.3   # neutral

    s = scraped_cat.upper().strip()
    d = dex_cat.upper().strip()

    if s == d:
        return 1.0

    # Canonical equivalences — scraped (EN) ↔ DEX (FR/EN mixed)
    EQUIVALENCES = {
        "TRANSPORT":   {"TRANSFERT","TRANSFER","TRANSFERTS","PRIVATE TRANSFER","TRANSPORT"},
        "CULTUREL":    {"CULTUREL","CULTURAL","CULTURE","VISITE CULTURELLE","TOUR CULTUREL",
                        "CITY TOUR","CITY TOURS","WALKING TOUR"},
        "HISTORIQUE":  {"HISTORIQUE","HISTORICAL","HISTOIRE","PATRIMOINE","HERITAGE",
                        "ARCHAEOLOGICAL"},
        "NATURE":      {"NATURE","NATURAL","OUTDOOR","OUTDOOR ACTIVITIES"},
        "AVENTURE":    {"AVENTURE","ADVENTURE","SPORTS","OUTDOOR SPORTS"},
        "NAUTIQUE":    {"NAUTIQUE","NAUTICAL","WATER SPORTS","SEA","BOAT TOUR","CRUISE"},
        "GASTRONOMIE": {"GASTRONOMIE","GASTRONOMY","FOOD","FOOD & WINE","WINE TOUR"},
        "BIEN_ETRE":   {"BIEN_ETRE","BIEN-ÊTRE","WELLNESS","SPA"},
        "FAMILLE":     {"FAMILLE","FAMILY","FAMILY ACTIVITIES"},
    }

    # Check if both map to the same canonical group
    for canonical, synonyms in EQUIVALENCES.items():
        if s in synonyms or s == canonical:
            if d in synonyms or d == canonical:
                return 1.0 if s == d else 0.8

    # Related category groups (partial credit)
    RELATED = [
        {"HISTORIQUE", "CULTUREL"},
        {"NATURE", "AVENTURE"},
        {"TRANSPORT", "TRANSFERT", "TRANSFER", "TRANSFERTS"},
    ]
    for group in RELATED:
        if s in group and d in group:
            return 0.6

    return 0.0


def score_price(scraped_price: Optional[float], dex_price: Optional[float]) -> float:
    """
    Price proximity ratio.
    10% weight.
    NULL on either side → neutral 0.5.
    """
    if scraped_price is None or dex_price is None:
        return 0.5
    if scraped_price <= 0 or dex_price <= 0:
        return 0.5

    ratio = max(scraped_price, dex_price) / min(scraped_price, dex_price)
    if ratio <= 1.3:   return 1.0   # within 30%
    if ratio <= 2.0:   return 0.7
    if ratio <= 3.0:   return 0.4
    return 0.1


def compute_similarity(scraped: Dict, dex: Dict, vector_sim: float) -> Dict:
    """
    Compute 6-dimension similarity score.
    vector_sim = cosine similarity from pgvector (0-1)

    Dimensions:
      title_score    35%  — keyword jaccard + substring bonus
      city_score     20%  — SequenceMatcher + unicode normalization
      country_score  10%  — exact country match (NEW — per user request)
      description    15%  — keyword jaccard on description
      category       15%  — exact category match / related group
      price           5%  — price ratio proximity

    country_score = 1.0 if countries match → prevents false positives across countries
    country_score = 0.5 if one is NULL   → neutral
    country_score = 0.0 if mismatch      → strong penalty (different country = different product)

    Country mismatch example that was wrong before:
      scraped: Rome, Italy — dex: Rome, United States → country_score=0.0 → lowers final score
    """
    # Pass activity_type so score_title can apply transfer direction logic
    atype_for_scoring = scraped.get("canonical_activity_type") or ""
    t_score = score_title(
        scraped.get("clean_title", ""),
        dex.get("clean_title", ""),
        dex.get("clean_title_en", ""),
        activity_type = atype_for_scoring,
    )
    c_score = score_city(
        scraped.get("normalized_city", ""),
        dex.get("normalized_city", ""),
    )
    country_score = score_country(
        scraped.get("normalized_country", ""),
        dex.get("normalized_country", ""),
    )
    d_score = score_description(
        scraped.get("clean_description", ""),
        dex.get("clean_description", ""),
    )
    cat_score = score_category(
        scraped.get("normalized_category", ""),
        dex.get("category", ""),
    )
    p_score = score_price(
        float(scraped["estimated_price"]) if scraped.get("estimated_price") else None,
        float(dex["price"]) if dex.get("price") else None,
    )

    # Rebalanced weights (6 dimensions, total 100%):
    # title 35% + city 20% + country 10% + desc 15% + category 15% + price 5%
    dimension_score = (
        t_score       * 0.35 +
        c_score       * 0.20 +
        country_score * 0.10 +
        d_score       * 0.15 +
        cat_score     * 0.15 +
        p_score       * 0.05
    )

    # Final: 60% dimension-based + 40% pure vector cosine
    final_score = round(dimension_score * 0.60 + vector_sim * 0.40, 4)

    return {
        "similarity_score":    final_score,
        "title_score":         round(t_score,       4),
        "city_score":          round(c_score,       4),
        "country_score":       round(country_score, 4),
        "description_score":   round(d_score,       4),
        "category_score":      round(cat_score,     4),
        "price_score":         round(p_score,       4),
        "vector_sim":          round(vector_sim,    4),
    }


def decide_match_status(similarity_score: float) -> Tuple[str, str]:
    """
    Layer 5: Deterministic decision based on similarity.
    Returns (match_status, match_type)
    """
    """
        Simplified decision: only two outcomes.
          similarity >= 0.85  → SIMILAR      (already in DEX catalog)
          similarity <  0.85  → NEW_PRODUCT  (candidate for validation + Odoo)
        Returns (match_status, match_type)
        """
    if similarity_score >= THRESHOLD_DUPLICATE:
        return "SIMILAR", "AUTO_REJECT"
    else:
        return "NEW_PRODUCT", "AUTO_ACCEPT"



# ─────────────────────────────────────────────────────────────
# Layer 2: Groq — AI reason for PENDING_REVIEW
# ─────────────────────────────────────────────────────────────

async def groq_confirm_match(scraped: Dict, dex: Dict, scores: Dict) -> str:
    """
    Layer 2: Groq explains WHY this might be a duplicate.
    Only called for PENDING_REVIEW cases (0.65-0.85).
    Returns a human-readable reason string.
    """
    if not config.groq_configured():
        return "Manual review required — similarity score in grey zone"

    prompt = f"""Compare these two travel products and determine if they are the same product.

SCRAPED PRODUCT:
Title: {scraped.get('clean_title', '')}
City: {scraped.get('normalized_city', '')}
Country: {scraped.get('normalized_country', '')}
Type: {scraped.get('canonical_activity_type', '')}
Category: {scraped.get('normalized_category', '')}
Price: {scraped.get('estimated_price', 'NULL')}€
Description: {(scraped.get('clean_description') or '')[:200]}

DEX EXISTING PRODUCT:
Title: {dex.get('clean_title', '')} / {dex.get('clean_title_en', '')}
City: {dex.get('normalized_city', '')}
Country: {dex.get('normalized_country', '')}
Type: {dex.get('canonical_activity_type', '')}
Category: {dex.get('category', '')}
Price: {dex.get('price', 'NULL')}€

Similarity scores: title={scores['title_score']:.2f} city={scores['city_score']:.2f} category={scores['category_score']:.2f} overall={scores['similarity_score']:.2f}

Question: Are these the same product? Reply with one sentence explaining if they are the same activity, similar but different, or clearly different."""

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as c:
            resp = await c.post(
                f"{config.GROQ_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {config.GROQ_API_KEYS[0]}", "Content-Type": "application/json"},
                json={
                    "model":       config.GROQ_FALLBACK_MODEL,
                    "messages":    [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens":  150,
                },
            )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.debug(f"  groq_confirm_match error: {e}")

    return "Manual review required — similarity score in grey zone"


# ─────────────────────────────────────────────────────────────
# Layer 2: Groq — Price estimation for ACCEPTED products
# ─────────────────────────────────────────────────────────────

async def estimate_price(
    scraped: Dict,
    similar_dex_prices: List[float],
) -> Optional[float]:
    """
    Layer 2: Estimate price for ACCEPTED products with NULL price.
    Uses Groq with context: city, country, activity_type, category,
    + prices of similar DEX products in the same city/category.

    This is the ONLY place LLM does price estimation.
    Validation: min 5€, max 5000€.
    """
    if not config.groq_configured():
        return None

    city     = scraped.get("normalized_city") or ""
    country  = scraped.get("normalized_country") or ""
    atype    = scraped.get("canonical_activity_type") or "EXCURSION"
    category = scraped.get("normalized_category") or ""
    title    = scraped.get("clean_title") or ""
    desc     = (scraped.get("clean_description") or "")[:200]

    price_context = ""
    if similar_dex_prices:
        avg = sum(similar_dex_prices) / len(similar_dex_prices)
        price_context = f"\nSimilar DEX products in {city}: prices range from {min(similar_dex_prices):.0f}€ to {max(similar_dex_prices):.0f}€ (avg {avg:.0f}€)"

    prompt = f"""Estimate the price in EUR for this tourism product.

Product: {title}
Location: {city}, {country}
Activity type: {atype}
Category: {category}
Description: {desc}
{price_context}

Rules:
- Price must be realistic for this destination and activity type
- Return ONLY a number (integer or decimal), nothing else
- If genuinely uncertain, return the midpoint of similar products
- Min: 5€, Max: 5000€

Price in EUR:"""

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
            resp = await c.post(
                f"{config.GROQ_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {config.GROQ_API_KEYS[0]}", "Content-Type": "application/json"},
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
        # Extract number
        m = re.search(r"\d+(?:[.,]\d+)?", raw)
        if m:
            price = float(m.group().replace(",", "."))
            if 5 <= price <= 5000:
                return round(price, 2)
    except Exception as e:
        logger.debug(f"  estimate_price error: {e}")

    return None


# ─────────────────────────────────────────────────────────────
# Save match to produit_matches
# ─────────────────────────────────────────────────────────────

async def save_match(
    scraped_product_id: int,
    dex_product_id: int,
    run_id: str,
    scores: Dict,
    match_status: str,
    match_type: str,
    ai_reason: str,
    db: DatabaseManager,
):
    """Save match result to produit_matches table (from ProduitMatches model)."""
    try:
        async with db.engine.begin() as conn:
            await conn.execute(text("""
                INSERT INTO produit_matches (
                    scraped_product_id, dex_product_id, run_id,
                    similarity_score, title_score, city_score,
                    description_score, category_score, price_score,
                    supplier_score,
                    match_status, match_type, ai_reason,
                    created_at
                ) VALUES (
                    :spid, :dpid, :run_id,
                    :sim, :title, :city,
                    :desc, :cat, :price,
                    :supplier,
                    :status, :mtype, :reason,
                    NOW()
                )
                ON CONFLICT DO NOTHING
            """), {
                "spid":     scraped_product_id,
                "dpid":     dex_product_id,
                "run_id":   run_id,
                "sim":      scores["similarity_score"],
                "title":    scores["title_score"],
                "city":     scores["city_score"],
                "desc":     scores["description_score"],
                "cat":      scores["category_score"],
                "price":    scores["price_score"],
                "supplier": scores.get("country_score", 0.0),  # country_score stored in supplier_score column
                "status":   match_status,
                "mtype":    match_type,
                "reason":   ai_reason,
            })
    except Exception as e:
        logger.error(f"  save_match error: {e}")


async def update_estimated_price(
    scraped_product_id: int,
    price: float,
    db: DatabaseManager,
):
    """Update estimated_price on scraped_products_normalized after estimation."""
    try:
        async with db.engine.begin() as conn:
            await conn.execute(text("""
                UPDATE scraped_products_normalized
                SET estimated_price    = :price,
                    price_is_estimated = TRUE
                WHERE scraped_product_id = :pid
            """), {"price": price, "pid": scraped_product_id})
    except Exception as e:
        logger.debug(f"  update_price error: {e}")


# ─────────────────────────────────────────────────────────────
# MatchingPipeline — main class
# ─────────────────────────────────────────────────────────────

class MatchingPipeline:
    """





    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def run(
        self,
        limit:     int = 100,
        run_id:    Optional[str] = None,
        groq_reasons: bool = True,
    ) -> Dict:
        from time import time
        start = time()
        stats = dict(
            total=0, new_product=0, duplicate=0,
            pending=0, failed=0, price_estimated=0,
        )

        logger.info("=" * 60)
        logger.info("🔗 Phase evaluation — Matching and validationPipeline")
        logger.info(f"   limit       : {limit}")
        logger.info(f"   groq reasons: {groq_reasons}")
        logger.info("=" * 60)

        rows = await self._load_unmatched(limit, run_id)
        if not rows:
            logger.info("  ✅ Nothing to match")
            return stats

        # Country distribution — shows how many products per country we'll match
        country_counts: Dict[str, int] = {}
        for r in rows:
            c = r.get("normalized_country") or "NULL"
            country_counts[c] = country_counts.get(c, 0) + 1
        top_countries = sorted(country_counts.items(), key=lambda x: -x[1])[:8]
        logger.info(f"📦 {len(rows)} products to match against DEX")
        logger.info(f"   Countries: " + " | ".join(f"{c}={n}" for c, n in top_countries))

        for scraped in rows:
            await self._match_one(scraped, stats, run_id or "", groq_reasons)

        elapsed = time() - start
        logger.info("=" * 60)
        logger.info(f"✅ Matching done in {elapsed:.1f}s")
        logger.info(f"   total={stats['total']}")
        logger.info(f"   🆕 NEW_PRODUCT (pending validation) : {stats['new_product']}")
        logger.info(f"   ❌ SIMILAR (auto-excluded)           : {stats['duplicate']}")
        logger.info(f"   ❌ Failed:                {stats['failed']}")
        logger.info(f"   💶 Prices estimated:      {stats['price_estimated']}")
        logger.info("=" * 60)
        return stats

    async def _load_unmatched(self, limit: int, run_id: Optional[str]) -> List[Dict]:
        """Load normalized scraped products not yet in produit_matches."""
        async with self.db.engine.connect() as conn:
            if run_id:
                r = await conn.execute(text("""
                    SELECT
                        spn.id, spn.scraped_product_id, spn.fournisseur_id, spn.run_id,
                        spn.clean_title, spn.clean_description,
                        spn.normalized_city, spn.normalized_country,
                        spn.canonical_activity_type, spn.normalized_category,
                        spn.estimated_price, spn.duration_minutes,
                        spn.embedding, spn.matching_signature
                    FROM scraped_products_normalized spn
                    WHERE spn.embedding IS NOT NULL
                      AND spn.scraped_product_id NOT IN (
                          SELECT DISTINCT scraped_product_id
                          FROM produit_matches
                          WHERE scraped_product_id IS NOT NULL
                      )
                      AND spn.run_id = :run_id
                    ORDER BY spn.id
                    LIMIT :limit
                """), {"limit": limit, "run_id": run_id})
            else:
                r = await conn.execute(text("""
                    SELECT
                        spn.id, spn.scraped_product_id, spn.fournisseur_id, spn.run_id,
                        spn.clean_title, spn.clean_description,
                        spn.normalized_city, spn.normalized_country,
                        spn.canonical_activity_type, spn.normalized_category,
                        spn.estimated_price, spn.duration_minutes,
                        spn.embedding, spn.matching_signature
                    FROM scraped_products_normalized spn
                    WHERE spn.embedding IS NOT NULL
                      AND spn.scraped_product_id NOT IN (
                          SELECT DISTINCT scraped_product_id
                          FROM produit_matches
                          WHERE scraped_product_id IS NOT NULL
                      )
                    ORDER BY spn.id
                    LIMIT :limit
                """), {"limit": limit})
            return [dict(row._mapping) for row in r.fetchall()]

    async def _match_one(
        self,
        scraped: Dict,
        stats: Dict,
        run_id: str,
        groq_reasons: bool,
    ):
        pid   = scraped.get("scraped_product_id")
        title = scraped.get("clean_title", "")[:40]
        stats["total"] += 1

        try:
            # ── Step 1: Get DEX candidates ────────────────────
            candidates = await get_dex_candidates(scraped, self.db)

            if not candidates:
                # No DEX candidates in same city/type → auto ACCEPTED
                await save_match(
                    scraped_product_id = pid,
                    dex_product_id     = None,
                    run_id             = run_id,
                    scores             = {
                        "similarity_score": 0.0,
                        "title_score":0.0, "city_score":0.0,
                        "description_score":0.0, "category_score":0.0,
                        "price_score":0.0, "vector_sim":0.0,
                    },
                    match_status="NEW_PRODUCT",
                    match_type="AUTO_ACCEPT",
                    ai_reason    = "No similar DEX products found in same city/activity type",
                    db           = self.db,
                )
                stats["new_product"] += 1
                logger.info(f"  ✅ {pid} | {title} | new_product (no candidates)")

                # Estimate price if NULL
                if not scraped.get("estimated_price"):
                    await self._estimate_and_save_price(scraped, [], stats)
                return

            # ── Step 2: Score all candidates, pick best ───────
            best_scores = None
            best_dex    = None
            best_sim    = -1.0

            for candidate in candidates:
                vector_sim = float(candidate.get("cosine_sim") or 0.0)
                scores     = compute_similarity(scraped, candidate, vector_sim)

                if scores["similarity_score"] > best_sim:
                    best_sim    = scores["similarity_score"]
                    best_scores = scores
                    best_dex    = candidate

            # ── Step 3: Decision ──────────────────────────────
            match_status, match_type = decide_match_status(best_sim)

            # ── Step 4: AI reason for PENDING ─────────────────
            ai_reason = ""


            if match_status == "SIMILAR":
                ai_reason = (
                    f"High similarity ({best_sim:.2f}) with DEX product "
                    f"'{best_dex.get('clean_title_en') or best_dex.get('clean_title','')}' "
                    f"in {best_dex.get('normalized_city','')}"
                )
            elif match_status == "NEW_PRODUCT":
                ai_reason = (
                    f"Low similarity ({best_sim:.2f}) — new product not in DEX. "
                    f"Closest DEX match: '{best_dex.get('clean_title_en') or best_dex.get('clean_title','')}'"
                )

            # ── Step 5: Save match ────────────────────────────
            await save_match(
                scraped_product_id = pid,
                dex_product_id     = best_dex.get("dex_product_id") if best_dex else None,
                run_id             = run_id,
                scores             = best_scores,
                match_status       = match_status,
                match_type         = match_type,
                ai_reason          = ai_reason,
                db                 = self.db,
            )

            # ── Step 6: Price estimation for ACCEPTED only ────
            if match_status == "NEW_PRODUCT" and not scraped.get("estimated_price"):
                dex_prices = [
                    float(c["price"])
                    for c in candidates
                    if c.get("price") and float(c["price"]) > 0
                ]
                await self._estimate_and_save_price(scraped, dex_prices, stats)

            # Stats
            if match_status == "SIMILAR":
                ai_reason = f"High similarity ({best_sim:.2f}) — already in DEX catalog"
            elif match_status == "NEW_PRODUCT":
                ai_reason = f"Low similarity ({best_sim:.2f}) — new product not in DEX"

            icon = {"NEW_PRODUCT": "✅", "SIMILAR": "❌"}[match_status]
            logger.info(
                f"  {icon} {pid} | {title} | "
                f"{match_status} | sim={best_sim:.3f} | "
                f"t={best_scores['title_score']:.2f} "
                f"city={best_scores['city_score']:.2f} "
                f"cntry={best_scores.get('country_score',0):.2f} "
                f"cat={best_scores['category_score']:.2f} "
                f"vec={best_scores['vector_sim']:.2f}"
            )

        except Exception as e:
            stats["failed"] += 1
            logger.error(f"  ❌ {pid} match error: {e}")

    async def _estimate_and_save_price(
        self,
        scraped: Dict,
        dex_prices: List[float],
        stats: Dict,
    ):
        """Estimate price for ACCEPTED products with NULL price."""
        estimated = await estimate_price(scraped, dex_prices)
        if estimated:
            await update_estimated_price(
                scraped["scraped_product_id"], estimated, self.db
            )
            stats["price_estimated"] += 1
            logger.debug(
                f"    💶 Price estimated: {estimated}€ "
                f"for '{scraped.get('clean_title','')[:40]}'"
            )
