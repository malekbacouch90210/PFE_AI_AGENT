"""
rebuild_dex_normalized.py — FINAL
═══════════════════════════════════════════════════════════════════════════════
ONE-TIME EXECUTION — builds dex_products_normalized from scratch.

CONFIRMED from Excel analysis (Tables_liées_à_la_fiche_produit_V2.xlsx):

  DEAD → NOT USED:
    dex_produit.longitude / latitude   → 100% null (use localite instead)
    dex_produit.lieuRDV                → 100% null
    dex_detail.chapeau                 → 100% null in ALL 79999 rows
    dex_produit.categorie              → 100% null
    localite.codeISO                   → 100% null (use nomEN for country)

  ALIVE → USED:
    dex_produit.nom/nomEN              → clean_title / clean_title_en
    dex_produit.adresse                → "Italie - Rome" → city + country fallback
    dex_detail.texte (lang=44)         → clean_description (79% populated)
    dex_localite via idVille           → geo_lat/geo_lng + city name
    dex_produit.typePrestation         → 424=EXCURSION 429=TRANSFER 449=TICKET
    dex_produit.prixAppel / devise     → price + currency
    dex_produit.ProductCategory/Type   → category + product_type
    dex_produit.organisateur           → supplier_name
    dex_produit.idFournisseur          → supplier_id
    dex_produit.actif / vendable       → is_active / is_vendable
    dex_produit.enPromo / prive / b2c  → activity_tags

  adresse field format: "Italie - Rome" → split on " - "
    Left part  = country in French → normalize via lookup + qwen2.5:7b
    Right part = city in French/local → normalize via lookup + qwen2.5:7b

  Geo priority:
    1. localite.nomEN (English name, ~90% populated)
    2. localite.nom (French name) → qwen2.5:7b normalize
    3. adresse split city part → qwen2.5:7b normalize

Run:
    python rebuild_dex_normalized.py
    python rebuild_dex_normalized.py --only-vendable   ← 2,289 products
    python rebuild_dex_normalized.py --limit 100       ← test run
    python rebuild_dex_normalized.py --no-truncate     ← incremental update
    python rebuild_dex_normalized.py --batch-size 200 --max-concurrent 5

Models (from config.py):
    qwen2.5:7b        → city/country normalization
    mxbai-embed-large → 1024-dim embeddings
═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent))

from config import config
from database import DatabaseManager

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ─────────────────────────────────────────────────────────────
# Constants — from config.py
# ─────────────────────────────────────────────────────────────

OLLAMA_BASE_URL = config.OLLAMA_URL            # http://localhost:11434
MODEL_CLASSIFY  = config.OLLAMA_MODEL_CLASSIFY  # qwen2.5:7b
MODEL_EMBED     = config.OLLAMA_MODEL_EMBED     # mxbai-embed-large
OLLAMA_TIMEOUT  = config.OLLAMA_TIMEOUT         # 45s

# DEX typePrestation → canonical type
TYPE_MAP = {
    "424": "EXCURSION",   # 19,618 products
    "429": "TRANSFER",    # 2,783 products
    "449": "TICKET",      # 1,985 products
}

# DEX devise → currency code
DEVISE_MAP = {
    "403": "EUR",
    "404": "USD",
    "405": "GBP",
    "407": "CHF",
}

# LLM cache — avoid re-calling for same inputs
_city_cache:    dict = {}
_country_cache: dict = {}


# ─────────────────────────────────────────────────────────────
# Ollama helpers
# ─────────────────────────────────────────────────────────────

async def ollama_generate(client: httpx.AsyncClient, prompt: str, model: str = None) -> str:
    m = model or MODEL_CLASSIFY
    try:
        resp = await client.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={"model": m, "prompt": prompt, "stream": False},
            timeout=OLLAMA_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except Exception as e:
        logger.warning(f"Ollama generate error [{m}]: {e}")
        return ""


async def ollama_embed(client: httpx.AsyncClient, text_input: str) -> Optional[list]:
    try:
        resp = await client.post(
            f"{OLLAMA_BASE_URL}/api/embeddings",
            json={"model": MODEL_EMBED, "prompt": text_input},
            timeout=OLLAMA_TIMEOUT,
        )
        resp.raise_for_status()
        vec = resp.json().get("embedding")
        if vec and len(vec) == 1024:
            return vec
        logger.warning(f"Unexpected embedding dim: {len(vec) if vec else 0}")
        return None
    except Exception as e:
        logger.warning(f"Ollama embed error: {e}")
        return None


async def check_ollama(client: httpx.AsyncClient) -> bool:
    try:
        r = await client.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        ok_c = any(MODEL_CLASSIFY.split(":")[0] in m for m in models)
        ok_e = any(MODEL_EMBED.split(":")[0] in m for m in models)
        if not ok_c:
            logger.error(f"❌ Missing: {MODEL_CLASSIFY} — run: ollama pull {MODEL_CLASSIFY}")
        if not ok_e:
            logger.error(f"❌ Missing: {MODEL_EMBED} — run: ollama pull {MODEL_EMBED}")
        return ok_c and ok_e
    except Exception as e:
        logger.error(f"❌ Ollama not reachable at {OLLAMA_BASE_URL}: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# French → English lookup tables
# ─────────────────────────────────────────────────────────────

_FR_COUNTRY_MAP = {
    "italie": "Italy", "france": "France", "espagne": "Spain",
    "allemagne": "Germany", "portugal": "Portugal", "grèce": "Greece",
    "royaume-uni": "United Kingdom", "angleterre": "United Kingdom",
    "autriche": "Austria", "belgique": "Belgium", "suisse": "Switzerland",
    "pays-bas": "Netherlands", "hollande": "Netherlands",
    "suède": "Sweden", "norvège": "Norway", "danemark": "Denmark",
    "finlande": "Finland", "irlande": "Ireland", "pologne": "Poland",
    "croatie": "Croatia", "hongrie": "Hungary", "roumanie": "Romania",
    "bulgarie": "Bulgaria", "république tchèque": "Czech Republic",
    "russie": "Russia", "ukraine": "Ukraine",
    "etats-unis": "United States", "états-unis": "United States",
    "usa": "United States", "mexique": "Mexico", "brésil": "Brazil",
    "argentine": "Argentina", "chili": "Chile", "pérou": "Peru",
    "colombie": "Colombia", "canada": "Canada", "cuba": "Cuba",
    "maroc": "Morocco", "tunisie": "Tunisia", "égypte": "Egypt",
    "egypte": "Egypt", "algérie": "Algeria", "algerie": "Algeria",
    "sénégal": "Senegal", "kenya": "Kenya", "afrique du sud": "South Africa",
    "nigeria": "Nigeria", "ghana": "Ghana", "tanzanie": "Tanzania",
    "ouganda": "Uganda", "rwanda": "Rwanda", "madagascar": "Madagascar",
    "mozambique": "Mozambique", "angola": "Angola", "zambie": "Zambia",
    "zimbabwe": "Zimbabwe", "botswana": "Botswana", "namibie": "Namibia",
    "maurice": "Mauritius", "seychelles": "Seychelles",
    "thaïlande": "Thailand", "thailande": "Thailand",
    "indonésie": "Indonesia", "vietnam": "Vietnam",
    "japon": "Japan", "inde": "India", "singapour": "Singapore",
    "malaisie": "Malaysia", "philippines": "Philippines",
    "corée du sud": "South Korea", "chine": "China",
    "hong kong": "Hong Kong", "sri lanka": "Sri Lanka",
    "népal": "Nepal", "cambodge": "Cambodia", "birmanie": "Myanmar",
    "australie": "Australia", "nouvelle-zélande": "New Zealand",
    "turquie": "Turkey", "israël": "Israel", "jordanie": "Jordan",
    "liban": "Lebanon", "émirats arabes unis": "United Arab Emirates",
    "emirats arabes unis": "United Arab Emirates",
    "arabie saoudite": "Saudi Arabia", "qatar": "Qatar",
    "koweït": "Kuwait", "oman": "Oman", "bahreïn": "Bahrain",
}

_FR_CITY_MAP = {
    "le caire": "Cairo", "moscou": "Moscow", "vienne": "Vienna",
    "varsovie": "Warsaw", "bucarest": "Bucharest", "budapest": "Budapest",
    "bruxelles": "Brussels", "athènes": "Athens", "lisbonne": "Lisbon",
    "florence": "Florence", "venise": "Venice", "naples": "Naples",
    "milan": "Milan", "munich": "Munich", "cologne": "Cologne",
    "francfort": "Frankfurt", "copenhague": "Copenhagen",
    "stockholm": "Stockholm", "oslo": "Oslo", "helsinki": "Helsinki",
    "dublin": "Dublin", "edimbourg": "Edinburgh", "édimbourg": "Edinburgh",
    "pékin": "Beijing", "shanghai": "Shanghai", "séoul": "Seoul",
    "bangkok": "Bangkok", "singapour": "Singapore", "hanoï": "Hanoi",
    "hô chi minh ville": "Ho Chi Minh City", "jakarta": "Jakarta",
    "manille": "Manila", "tunis": "Tunis", "alger": "Algiers",
    "casablanca": "Casablanca", "nairobi": "Nairobi", "lagos": "Lagos",
    "johannesburg": "Johannesburg", "le cap": "Cape Town",
    "buenos aires": "Buenos Aires", "rio de janeiro": "Rio de Janeiro",
    "sao paulo": "São Paulo", "santiago": "Santiago",
    "bogota": "Bogotá", "lima": "Lima", "mexico": "Mexico City",
    "la havane": "Havana", "new york": "New York",
    "los angeles": "Los Angeles", "nouvelle-orléans": "New Orleans",
    "saint-petersbourg": "Saint Petersburg",
    "saint pétersbourg": "Saint Petersburg",
}


def _normalize_country_fast(raw: str) -> str:
    if not raw:
        return ""
    return _FR_COUNTRY_MAP.get(raw.lower().strip(), raw.title())


def _normalize_city_fast(raw: str) -> str:
    if not raw:
        return ""
    return _FR_CITY_MAP.get(raw.lower().strip(), raw.strip())


def _looks_french(s: str) -> bool:
    return bool(re.search(r"[àâäéèêëîïôùûüç]", s.lower()))


async def normalize_city_llm(
    client: httpx.AsyncClient, city_raw: str, country_hint: str = ""
) -> str:
    key = f"{city_raw}|{country_hint}"
    if key in _city_cache:
        return _city_cache[key]
    prompt = (
        f"Convert this city name to its standard English spelling.\n"
        f"City: '{city_raw}'"
        + (f" (in {country_hint})" if country_hint else "")
        + "\nRespond with ONLY the English city name. No explanation."
    )
    result = (await ollama_generate(client, prompt)).strip().strip('"').strip("'")
    if not result or len(result) > 100:
        result = _normalize_city_fast(city_raw) or city_raw.strip()
    _city_cache[key] = result
    return result


async def normalize_country_llm(
    client: httpx.AsyncClient, country_raw: str
) -> str:
    if country_raw in _country_cache:
        return _country_cache[country_raw]
    prompt = (
        f"Convert this country name to its standard English spelling.\n"
        f"Country: '{country_raw}'\n"
        "Respond with ONLY the English country name. No explanation."
    )
    result = (await ollama_generate(client, prompt)).strip().strip('"').strip("'")
    if not result or len(result) > 80:
        result = _normalize_country_fast(country_raw) or country_raw.strip()
    _country_cache[country_raw] = result
    return result


# ─────────────────────────────────────────────────────────────
# adresse field parser
# ─────────────────────────────────────────────────────────────

def parse_adresse(adresse: str) -> tuple:
    """
    Parse DEX adresse field format: "Italie - Rome" → ("Rome", "Italie")
    Returns (city_raw, country_raw).
    """
    if not adresse:
        return "", ""
    adresse = adresse.strip()
    parts = re.split(r"\s*[-–]\s*", adresse, maxsplit=1)
    if len(parts) == 2:
        return parts[1].strip(), parts[0].strip()  # (city, country)
    return "", adresse  # only one part = country only


# ─────────────────────────────────────────────────────────────
# Activity type + price helpers
# ─────────────────────────────────────────────────────────────

def map_activity_type(typeprestation) -> str:
    if typeprestation is None:
        return "EXCURSION"
    return TYPE_MAP.get(str(typeprestation).strip(), "EXCURSION")


def map_currency(devise) -> str:
    if devise is None:
        return "EUR"
    return DEVISE_MAP.get(str(devise).strip(), "EUR")


def map_service_type(canonical: str) -> str:
    return {"EXCURSION": "TOUR", "TICKET": "ENTRANCE", "TRANSFER": "TRANSFER"}.get(canonical, "TOUR")


# ─────────────────────────────────────────────────────────────
# Matching signature + embedding text
# ─────────────────────────────────────────────────────────────

def build_matching_signature(city: str, activity_type: str, title: str) -> str:
    stopwords = {
        "the","a","an","of","in","at","to","for","and","or",
        "le","la","les","de","du","des","en","au","aux","un","une",
    }
    words = [w for w in title.lower().split() if w not in stopwords]
    title_key = " ".join(words[:3])
    raw = f"{city.lower()}|{activity_type}|{title_key}"
    h = hashlib.md5(raw.encode()).hexdigest()[:16]
    return f"{raw[:80]}_{h}"


def build_embedding_text(
    title: str,
    title_en: str,
    description: str,
    city: str,
    country: str,
    activity_type: str,
    category: str,
) -> str:
    """
    Build text for mxbai-embed-large.
    NOTE: chapeau is 100% NULL — not included.
    Use title_en if available for better embedding quality.
    """
    parts = []
    # Prefer English title for embedding (better multilingual similarity)
    if title_en:
        parts.append(title_en.strip())
    elif title:
        parts.append(title.strip())
    if city or country:
        parts.append(f"{city} {country}".strip())
    if activity_type:
        parts.append(activity_type)
    if category:
        parts.append(category)
    if description:
        # Clean HTML tags from description
        desc_clean = re.sub(r"<[^>]+>", " ", description)
        desc_clean = re.sub(r"\s+", " ", desc_clean).strip()
        parts.append(desc_clean[:400])
    return " | ".join(p for p in parts if p)


# ─────────────────────────────────────────────────────────────
# Per-product processor
# ─────────────────────────────────────────────────────────────

async def process_one(
    row,
    db: DatabaseManager,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    stats: dict,
):
    async with semaphore:
        try:
            (
                dex_id, nom, nomEN, description, typeprestation,
                prixappel, prixappelachat, devise,
                actif, vendable,
                idville, idlocalitedepart, idlocalitearrivee,
                idfournisseur, organisateur,
                dureeheure, dureeminute,
                enpromo, prive, b2c,
            ) = row

            # ── 1. Activity type ──────────────────────────────
            canonical_activity_type = map_activity_type(typeprestation)
            service_type            = map_service_type(canonical_activity_type)

            # ── 2. Title + description ────────────────────────
            clean_title    = (nom    or "").strip()[:500]
            clean_title_en = (nomEN  or "").strip()[:500]
            raw_desc       = (description or "").strip()
            # Clean HTML from description
            raw_desc = re.sub(r"<[^>]+>", " ", raw_desc)
            raw_desc = re.sub(r"\s+", " ", raw_desc).strip()
            clean_description = raw_desc[:2000] if raw_desc else ""

            # ── 3. Get dex_detail.texte (lang=44 FR) ──────────
            # NOTE: chapeau is 100% NULL — skip it entirely
            detail = await db.get_dex_detail(dex_id, lang_id=44)
            if detail and not clean_description:
                texte = re.sub(r"<[^>]+>", " ", detail.get("texte") or "")
                clean_description = re.sub(r"\s+", " ", texte).strip()[:2000]

            # ── 4. Geo from dex_localite ──────────────────────
            geo_lat = geo_lng = None
            localite_nom = localite_nomen = ""
            city_confidence = 0.0

            if idville:
                localite = await db.get_dex_localite(int(idville))
                if localite:
                    geo_lat        = localite.get("latitude")
                    geo_lng        = localite.get("longitude")
                    localite_nom   = (localite.get("nom")   or "").strip()
                    localite_nomen = (localite.get("nomen") or "").strip()
                    city_confidence = 0.9 if (geo_lat and geo_lng) else 0.5

            # ── 5. Transfer origin/destination ────────────────
            origin = destination = route_signature = ""
            if canonical_activity_type == "TRANSFER":
                if idlocalitedepart:
                    dep = await db.get_dex_localite(int(idlocalitedepart))
                    if dep:
                        origin = (dep.get("nomen") or dep.get("nom") or "").strip()
                if idlocalitearrivee:
                    arr = await db.get_dex_localite(int(idlocalitearrivee))
                    if arr:
                        destination = (arr.get("nomen") or arr.get("nom") or "").strip()
                if origin or destination:
                    route_signature = f"{origin} → {destination}"

            # ── 6. Parse adresse for city/country fallback ────
            async with db.engine.connect() as conn:
                addr_row = (await conn.execute(
                    text("SELECT adresse FROM dex_produit WHERE id = :id"),
                    {"id": dex_id},
                )).fetchone()
                adresse_raw = (addr_row[0] or "") if addr_row else ""

            city_from_addr, country_from_addr = parse_adresse(adresse_raw)

            # ── 7. Resolve best city + country ────────────────
            # Priority: localite.nomEN → localite.nom → adresse
            city_raw    = localite_nomen or localite_nom or city_from_addr or ""
            country_raw = country_from_addr or ""

            # Fast lookup (no LLM cost)
            normalized_city    = _normalize_city_fast(city_raw)
            normalized_country = _normalize_country_fast(country_raw)

            # LLM fallback only if fast lookup left French chars
            if city_raw and _looks_french(normalized_city):
                normalized_city = await normalize_city_llm(
                    client, city_raw, normalized_country
                )
            if country_raw and _looks_french(normalized_country):
                normalized_country = await normalize_country_llm(client, country_raw)

            # Fallback city confidence when no localite
            if not idville and city_from_addr:
                city_confidence = 0.3

            # ── 8. Category from ProductCategory/Type ─────────
            category = product_type_val = ""
            async with db.engine.connect() as conn:
                cat_row = (await conn.execute(
                    text("""
                        SELECT producttype, productcategory
                        FROM dex_produit WHERE id = :id
                    """),
                    {"id": dex_id},
                )).fetchone()
                if cat_row:
                    product_type_val = (cat_row[0] or "").strip()
                    category         = (cat_row[1] or "").strip()

            # ── 9. Price + currency ───────────────────────────
            price    = float(prixappel) if prixappel else None
            currency = map_currency(devise)
            is_promo = bool(enpromo)

            # ── 10. Duration ──────────────────────────────────
            duration_minutes = None
            h = int(dureeheure or 0)
            m = int(dureeminute or 0)
            if h or m:
                duration_minutes = h * 60 + m

            # ── 11. Activity tags ─────────────────────────────
            # Themes and languages fetched from join tables
            themes = []
            try:
                async with db.engine.connect() as conn:
                    t_rows = (await conn.execute(
                        text("SELECT theme FROM dex_produit_theme WHERE idproduit = :id"),
                        {"id": dex_id},
                    )).fetchall()
                    themes = [r[0] for r in t_rows]
            except Exception as e:
                logger.debug(f"  themes fetch skip dex_id={dex_id}: {e}")
                themes = []

            langs = []
            try:
                async with db.engine.connect() as conn:
                    l_rows = (await conn.execute(
                        text("""
                            SELECT DISTINCT "codeService"
                            FROM dex_produit_langue
                            WHERE "idProduit" = :id
                        """),
                        {"id": dex_id},
                    )).fetchall()
                    langs = [r[0] for r in l_rows if r[0]]
            except Exception as e:
                logger.debug(f"  langs fetch skip dex_id={dex_id}: {e}")
                langs = []

            activity_tags = {
                "is_promo":   is_promo,
                "is_private": bool(prive),
                "is_b2c":     bool(b2c),
                "themes":     themes,
                "languages":  langs,
            }

            # ── 12. Odoo ID ───────────────────────────────────
            # odoo_id already available from main row fetch — no extra query needed
            odoo_id = None
            try:
                odoo_id_raw = None
                async with db.engine.connect() as conn:
                    odoo_row = (await conn.execute(
                        text("SELECT idodoo FROM dex_produit WHERE id = :id"),
                        {"id": dex_id},
                    )).fetchone()
                    if odoo_row and odoo_row[0]:
                        odoo_id = int(odoo_row[0])
            except Exception:
                odoo_id = None

            # ── 13. Matching signature ────────────────────────
            matching_signature = build_matching_signature(
                normalized_city, canonical_activity_type,
                clean_title_en or clean_title,
            )

            # ── 14. Embedding ─────────────────────────────────
            emb_text = build_embedding_text(
                clean_title, clean_title_en, clean_description,
                normalized_city, normalized_country,
                canonical_activity_type, category,
            )
            embedding = await ollama_embed(client, emb_text)
            if not embedding:
                logger.warning(f"  Embedding failed dex_id={dex_id}")
                stats["errors"] += 1
                return

            # ── 15. Upsert ────────────────────────────────────
            async with db.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO dex_products_normalized (
                        dex_product_id,
                        clean_title, clean_title_en, clean_description,
                        normalized_city, normalized_country,
                        geo_lat, geo_lng, city_confidence,
                        canonical_activity_type, service_type,
                        category, product_type,
                        origin, destination, route_signature,
                        price, currency, is_promo,
                        duration_minutes,
                        is_active, is_vendable,
                        supplier_name, supplier_id, odoo_id,
                        activity_tags,
                        embedding, matching_signature,
                        normalization_confidence,
                        created_at, updated_at
                    ) VALUES (
                        :dex_product_id,
                        :clean_title, :clean_title_en, :clean_description,
                        :normalized_city, :normalized_country,
                        :geo_lat, :geo_lng, :city_confidence,
                        :canonical_activity_type, :service_type,
                        :category, :product_type,
                        :origin, :destination, :route_signature,
                        :price, :currency, :is_promo,
                        :duration_minutes,
                        :is_active, :is_vendable,
                        :supplier_name, :supplier_id, :odoo_id,
                        :activity_tags,
                        :embedding, :matching_signature,
                        :normalization_confidence,
                        NOW(), NOW()
                    )
                    ON CONFLICT (dex_product_id) DO UPDATE SET
                        clean_title              = EXCLUDED.clean_title,
                        clean_title_en           = EXCLUDED.clean_title_en,
                        clean_description        = EXCLUDED.clean_description,
                        normalized_city          = EXCLUDED.normalized_city,
                        normalized_country       = EXCLUDED.normalized_country,
                        geo_lat                  = EXCLUDED.geo_lat,
                        geo_lng                  = EXCLUDED.geo_lng,
                        city_confidence          = EXCLUDED.city_confidence,
                        canonical_activity_type  = EXCLUDED.canonical_activity_type,
                        service_type             = EXCLUDED.service_type,
                        category                 = EXCLUDED.category,
                        product_type             = EXCLUDED.product_type,
                        origin                   = EXCLUDED.origin,
                        destination              = EXCLUDED.destination,
                        route_signature          = EXCLUDED.route_signature,
                        price                    = EXCLUDED.price,
                        currency                 = EXCLUDED.currency,
                        is_promo                 = EXCLUDED.is_promo,
                        duration_minutes         = EXCLUDED.duration_minutes,
                        is_active                = EXCLUDED.is_active,
                        is_vendable              = EXCLUDED.is_vendable,
                        supplier_name            = EXCLUDED.supplier_name,
                        supplier_id              = EXCLUDED.supplier_id,
                        odoo_id                  = EXCLUDED.odoo_id,
                        activity_tags            = EXCLUDED.activity_tags,
                        embedding                = EXCLUDED.embedding,
                        matching_signature       = EXCLUDED.matching_signature,
                        normalization_confidence = EXCLUDED.normalization_confidence,
                        updated_at               = NOW()
                """), {
                    "dex_product_id":          dex_id,
                    "clean_title":             clean_title[:500] or None,
                    "clean_title_en":          clean_title_en[:500] or None,
                    "clean_description":       clean_description[:2000] or None,
                    "normalized_city":         normalized_city[:150] or None,
                    "normalized_country":      normalized_country[:100] or None,
                    "geo_lat":                 geo_lat,
                    "geo_lng":                 geo_lng,
                    "city_confidence":         city_confidence,
                    "canonical_activity_type": canonical_activity_type,
                    "service_type":            service_type,
                    "category":                category[:150] or None,
                    "product_type":            product_type_val[:150] or None,
                    "origin":                  origin[:255] or None,
                    "destination":             destination[:255] or None,
                    "route_signature":         route_signature[:300] or None,
                    "price":                   price,
                    "currency":                currency,
                    "is_promo":                is_promo,
                    "duration_minutes":        duration_minutes,
                    "is_active":               bool(actif),
                    "is_vendable":             bool(vendable),
                    "supplier_name":           (organisateur or "")[:255] or None,
                    "supplier_id":             idfournisseur,
                    "odoo_id":                 odoo_id,
                    "activity_tags":           json.dumps(activity_tags),
                    "embedding":               embedding,
                    "matching_signature":      matching_signature[:500],
                    "normalization_confidence": city_confidence,
                })

            stats["processed"] += 1

            if stats["processed"] % 100 == 0:
                elapsed = time.time() - stats["start_time"]
                rate = stats["processed"] / elapsed if elapsed > 0 else 0
                eta  = (stats["total"] - stats["processed"]) / rate if rate > 0 else 0
                logger.info(
                    f"  📊 {stats['processed']:,}/{stats['total']:,} "
                    f"({stats['processed']/stats['total']*100:.1f}%) | "
                    f"{rate:.1f}/s | ETA {eta/60:.1f}min | "
                    f"LLM cache: {len(_city_cache)+len(_country_cache)}"
                )

        except Exception as e:
            logger.error(f"  ❌ dex_id={dex_id if 'dex_id' in dir() else '?'}: {e}")
            stats["errors"] += 1


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def main(
    batch_size:     int  = 100,
    max_concurrent: int  = 3,
    only_vendable:  bool = False,
    limit:          Optional[int] = None,
    truncate:       bool = True,
):
    logger.info("═" * 60)
    logger.info("DEX Normalization Pipeline")
    logger.info(f"  Model classify : {MODEL_CLASSIFY}")
    logger.info(f"  Model embed    : {MODEL_EMBED}")
    logger.info(f"  Only vendable  : {only_vendable}")
    logger.info(f"  Truncate       : {truncate}")
    logger.info("═" * 60)

    db = DatabaseManager()

    # Check Ollama
    async with httpx.AsyncClient() as client:
        if not await check_ollama(client):
            logger.error("Ollama check failed — exiting")
            return

    # Ensure tables + vector extension
    async with db.engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    await db.create_tables()
    await db.create_embedding_indexes()

    # Count
    async with db.engine.connect() as conn:
        where = "WHERE vendable = 1" if only_vendable else ""
        total = (await conn.execute(
            text(f"SELECT COUNT(*) FROM dex_produit {where}")
        )).scalar() or 0

    if limit:
        total = min(total, limit)

    logger.info(f"📊 Products to normalize: {total:,}")

    if total == 0:
        logger.warning("No products found. Exiting.")
        return

    # Optional truncate
    if truncate:
        async with db.engine.begin() as conn:
            await conn.execute(
                text("TRUNCATE TABLE dex_products_normalized RESTART IDENTITY")
            )
        logger.info("🗑️  Table truncated — starting fresh")

    stats = {
        "processed":  0,
        "errors":     0,
        "total":      total,
        "start_time": time.time(),
    }
    semaphore = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient() as http_client:
        processed_total = 0
        offset          = 0

        while processed_total < total:
            current_batch = min(batch_size, total - processed_total)
            rows = await db.get_dex_products_batch(
                limit=current_batch,
                offset=offset,
                only_vendable=only_vendable,
            )
            if not rows:
                break

            tasks = [
                process_one(row, db, http_client, semaphore, stats)
                for row in rows
            ]
            await asyncio.gather(*tasks)

            processed_total += len(rows)
            offset          += len(rows)

            if processed_total >= total:
                break

    elapsed = time.time() - stats["start_time"]
    logger.info("═" * 60)
    logger.info("✅ Done")
    logger.info(f"   Processed  : {stats['processed']:,}")
    logger.info(f"   Errors     : {stats['errors']:,}")
    logger.info(f"   LLM cache  : {len(_city_cache)+len(_country_cache)} entries")
    logger.info(f"   Duration   : {elapsed/60:.1f} min")
    logger.info(f"   Speed      : {stats['processed']/elapsed:.1f} prod/s")
    logger.info("═" * 60)

    count = await db.get_dex_normalized_count()
    logger.info(f"📦 dex_products_normalized: {count:,} rows")


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rebuild dex_products_normalized from dex_produit."
    )
    parser.add_argument("--batch-size",     type=int,  default=100)
    parser.add_argument("--max-concurrent", type=int,  default=3)
    parser.add_argument("--only-vendable",  action="store_true")
    parser.add_argument("--limit",          type=int,  default=None)
    parser.add_argument("--no-truncate",    action="store_true")

    args = parser.parse_args()

    asyncio.run(main(
        batch_size     = args.batch_size,
        max_concurrent = args.max_concurrent,
        only_vendable  = args.only_vendable,
        limit          = args.limit,
        truncate       = not args.no_truncate,
    ))