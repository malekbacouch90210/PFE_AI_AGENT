
import asyncio
import itertools
import json
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import text

from config import config
from database import DatabaseManager

# ─────────────────────────────────────────────────────────────
# Schema — exact match to GROQ.txt
# ─────────────────────────────────────────────────────────────

class TravelProductSchema(BaseModel):
    nom_produit:             str           = Field(description="Exact marketing title of the tour/service.")
    description:             Optional[str] = Field(description="Brief summary of what is included.")
    canonical_activity_type: str           = Field(description="Exactly: EXCURSION, TICKET, or TRANSFER.")
    service_type:            str           = Field(description="TOUR | DAY_TRIP | TRANSFER | MUSEUM_TICKET | AIRPORT_SHUTTLE.")
    origin:                  Optional[str] = Field(description="Departure city for transfers only.")
    destination:             Optional[str] = Field(description="City or region where activity takes place.")
    pays_raw:                Optional[str] = Field(description="Country where activity happens — NOT company country.")
    prix_raw_text:           Optional[str] = Field(description="Raw price exactly as shown e.g. 'From € 79,90 per person'.")
    url_produit:             Optional[str] = Field(description="Absolute URL to this product page if visible.")
    duree:                   Optional[str] = Field(description="Duration e.g. '3 hours', 'Half-day', '1 day'.")

class TravelProductListSchema(BaseModel):
    products: List[TravelProductSchema]


# ─────────────────────────────────────────────────────────────
# Layer 2: Groq multi-key rotation
# ─────────────────────────────────────────────────────────────

_key_cycler = None

def _next_groq_key() -> Optional[str]:
    global _key_cycler
    if not config.GROQ_API_KEYS:
        return None
    if _key_cycler is None:
        _key_cycler = itertools.cycle(config.GROQ_API_KEYS)
    return next(_key_cycler)


# ─────────────────────────────────────────────────────────────
# Layer 5: Validation constants
# ─────────────────────────────────────────────────────────────

# Binary/media extensions — reject before Firecrawl
_REJECT_EXT = {
    ".avif",".webp",".jpg",".jpeg",".png",".gif",".svg",".ico",
    ".mp4",".mp3",".pdf",".zip",".gz",".css",".js",".woff",".woff2",
}

# CDN paths in URL — always binary
_REJECT_URL_PAT = [
    r"/insecure/plain/", r"s3://", r"/thumbnail", r"/thumb/",
    r"/img/", r"/images?/", r"/assets/", r"/static/", r"/cdn/",
]

# Product URL patterns — only scrape these paths
_PRODUCT_URL_PAT = [
    r"/tour[s]?/", r"/excursion[s]?/", r"/activit(y|ies)/",
    r"/ticket[s]?/", r"/transfer[s]?/", r"/experience[s]?/",
    r"/product[s]?/", r"/package[s]?/", r"/trip[s]?/",
    r"/things-to-do/", r"/visit[s]?/", r"/safari[s]?/",
    r"/day-tour[s]?/", r"/guide[s]?/",
]

_GARBAGE_TITLES = {
    "home","about us","contact","contact us","activities","tours",
    "transfers","tickets","book now","reserve now","check availability",
    "tour essentials","what's included","itinerary at a glance",
    "what is included","not included","important information",
    "overview","highlights","details","gallery","reviews","faq",
    "terms and conditions","cancellation policy","museum shop",
    "gift shop","buy a ticket","auditorium","error","404","403",
    "unsere besuche","karte & öffnungszeiten","öffnungszeiten",
    "kontakt","nos visites","plan & horaires","réservation","accueil",
    "orari","contatti","horarios","mapa","contacto",
}

_GARBAGE_PAT = [
    r"^what'?s\s+included", r"^itinerary\s+at",
    r"^tour\s+essentials?$", r"^book\s+",
    r"^auditorium\s+\d+", r"^gift\s+shop",
    r"^all\s+\w+$", r"^page\s+\d+$",
    r"^buy\s+a?\s+ticket",
]

# Attraction keywords — these are NOT cities
_ATTRACTION_KW = {
    "museum","musée","museo","palace","palais","castle","château",
    "gallery","galerie","cathedral","temple","basilica","monument",
    "tower","eiffel","colosseum","acropolis","vatican","louvre",
    "sagrada","alhambra","pyramid","sphinx","parthenon","ruins",
    "amphitheater","theatre","opera house","waterfall","cave",
}

_PRICE_MIN = config.PRICE_MIN_EUR
_PRICE_MAX = config.PRICE_MAX_EUR

_COUNTRY_URL_SLUGS = {
    "france":"France","italy":"Italy","spain":"Spain","greece":"Greece",
    "egypt":"Egypt","morocco":"Morocco","tunisia":"Tunisia","china":"China",
    "japan":"Japan","thailand":"Thailand","indonesia":"Indonesia","bali":"Indonesia",
    "usa":"United States","south-africa":"South Africa","kenya":"Kenya",
    "india":"India","portugal":"Portugal","germany":"Germany","turkey":"Turkey",
    "jordan":"Jordan","australia":"Australia","brazil":"Brazil","peru":"Peru",
    "mexico":"Mexico","vietnam":"Vietnam","cambodia":"Cambodia",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    ),
}


# ─────────────────────────────────────────────────────────────
# Layer 5: Validation helpers
# ─────────────────────────────────────────────────────────────

def _is_valid_url(url: str) -> bool:
    """Reject image/binary/CDN URLs before sending to Firecrawl."""
    u = url.lower()
    for ext in _REJECT_EXT:
        if u.endswith(ext) or f"{ext}?" in u:
            return False
    for pat in _REJECT_URL_PAT:
        if re.search(pat, u):
            return False
    return any(re.search(p, u) for p in _PRODUCT_URL_PAT)


def _is_garbage(nom: str) -> bool:
    n = nom.lower().strip()
    if len(nom) < config.TITLE_MIN_CHARS or len(nom.split()) < config.TITLE_MIN_WORDS:
        return True
    if n in _GARBAGE_TITLES:
        return True
    return any(re.search(p, n) for p in _GARBAGE_PAT)


def _is_attraction(name: str) -> bool:
    """Attraction names (Eiffel Tower) must not be stored as city."""
    n = name.lower()
    return any(kw in n for kw in _ATTRACTION_KW)


def _parse_price(raw: str) -> Tuple[Optional[float], str]:
    """
    Layer 5: Parse price with strict bounds.
    Null is better than hallucination — outside bounds → None.
    """
    if not raw:
        return None, "EUR"
    text = str(raw).strip()
    currency = "EUR"
    if "€" in text or "EUR" in text.upper():   currency = "EUR"
    elif "$" in text or "USD" in text.upper():  currency = "USD"
    elif "£" in text or "GBP" in text.upper():  currency = "GBP"
    nums = re.sub(r"[^\d.,]", " ", text).replace(",", ".")
    m = re.search(r"\d+(?:\.\d+)?", nums)
    if m:
        try:
            v = float(m.group())
            if _PRICE_MIN <= v <= _PRICE_MAX:
                return round(v, 2), currency
        except ValueError:
            pass
    return None, "EUR"


def _sanitize(text: str) -> str:
    """Layer 5: Strip NUL bytes PostgreSQL cannot store."""
    if not text:
        return text
    text = text.replace("\x00", "")
    return re.sub(r"[\x01-\x08\x0b\x0c\x0e-\x1f]", "", text)


def _country_from_url(url: str) -> Optional[str]:
    try:
        for part in urlparse(url.lower()).path.split("/"):
            c = _COUNTRY_URL_SLUGS.get(part)
            if c:
                return c
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────
# Layer 1: Firecrawl acquisition
# ─────────────────────────────────────────────────────────────

async def firecrawl_scrape(url: str) -> Optional[str]:
    """
    Layer 1 — Firecrawl fetches URL → clean markdown.

    Firecrawl handles JS rendering, bot protection, lazy loading.
    Returns markdown stripped of nav/footer/sidebar.
    Returns None on any failure — pipeline moves to next URL.
    """
    if not config.firecrawl_configured():
        logger.warning("⚠️ FIRECRAWL_API_KEY not configured in .env")
        return None

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            resp = await client.post(
                f"{config.FIRECRAWL_URL}/v1/scrape",
                headers={
                    "Authorization": f"Bearer {config.FIRECRAWL_API_KEY}",
                    "Content-Type":  "application/json",
                },
                json={
                    "url":             url,
                    "formats":         ["markdown"],
                    "onlyMainContent": True,
                    "excludeTags":     ["nav","footer","header","aside","script","style","form","iframe"],
                    "waitFor":         1000,
                },
            )

        if resp.status_code == 402:
            logger.warning("  💳 Firecrawl: credit limit reached")
            return None
        if resp.status_code != 200:
            logger.debug(f"  Firecrawl {resp.status_code}: {url[:60]}")
            return None

        data     = resp.json()
        markdown = data.get("data", {}).get("markdown", "")

        if not markdown or len(markdown) < 50:
            return None

        logger.debug(f"  🔥 Firecrawl: {len(markdown)} chars ← {url[:60]}")
        return markdown

    except Exception as e:
        logger.debug(f"  Firecrawl error {url[:60]}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Layer 2: Groq reasoning — product extraction
# ─────────────────────────────────────────────────────────────

_GROQ_SYSTEM = "You are a travel product data extractor. Return only valid JSON. Never invent data."

_GROQ_INSTRUCTION = """Extract all travel products from this page content.

Return fields:
- nom_produit: exact product title (skip navigation items, section headers, FAQ)
- description: what's included (null if not present)
- canonical_activity_type: EXCURSION | TICKET | TRANSFER only
- service_type: TOUR | DAY_TRIP | TRANSFER | MUSEUM_TICKET | AIRPORT_SHUTTLE
- destination: city where activity physically takes place (null if unknown)
- pays_raw: country where activity takes place, NOT company country (null if unknown)
- prix_raw_text: price exactly as shown e.g. "From € 79,90 per person" (null if not visible)
- url_produit: full URL to product page if visible (null otherwise)
- duree: duration if mentioned (null otherwise)

CRITICAL RULES:
- If destination is an attraction (museum/palace/tower), set destination=null
- If uncertain about any field, return null — do not guess
- Skip: nav menus, section headers, blog posts, "What's Included" text
- destination = WHERE activity happens physically (Rome, Athens, Casablanca)
- pays_raw = country of destination (Italy, Greece, Morocco)"""


async def groq_extract(markdown: str, url: str) -> List[Dict]:
    """
    Layer 2 — Groq reasoning: markdown → structured product list.

    Multi-key rotation + tiered model fallback.
    Returns [] if all attempts fail — never raises.
    Null is better than hallucination.
    """
    if not config.groq_configured():
        logger.warning("  ⚠️ No Groq keys configured")
        return []

    schema = TravelProductListSchema.model_json_schema()
    prompt = (
        f"URL: {url}\n\nPAGE CONTENT:\n{markdown[:5000]}\n\n"
        f"{_GROQ_INSTRUCTION}\n\n"
        f"Return JSON matching schema:\n{json.dumps(schema, indent=2)}"
    )

    models = [config.GROQ_PRIMARY_MODEL, config.GROQ_SECONDARY_MODEL, config.GROQ_FALLBACK_MODEL]

    for attempt, model in enumerate(models):
        api_key = _next_groq_key()
        if not api_key:
            break

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(45.0)) as client:
                resp = await client.post(
                    f"{config.GROQ_BASE_URL}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type":  "application/json",
                    },
                    json={
                        "model":           model,
                        "messages":        [
                            {"role": "system", "content": _GROQ_SYSTEM},
                            {"role": "user",   "content": prompt},
                        ],
                        "temperature":     0.1,
                        "max_tokens":      2000,
                        "response_format": {"type": "json_object"},
                    },
                )

            if resp.status_code == 429:
                logger.info(f"  ⏳ Groq 429 [{model}] attempt {attempt+1} → rotate key")
                await asyncio.sleep(2.0 + attempt * 2.0)
                continue

            if resp.status_code != 200:
                logger.debug(f"  Groq {resp.status_code} [{model}]")
                continue

            content = resp.json()["choices"][0]["message"]["content"]
            clean   = re.sub(r"```(?:json)?|```", "", content).strip()
            parsed  = json.loads(clean)

            if isinstance(parsed, dict) and "products" in parsed:
                items = parsed["products"]
            elif isinstance(parsed, list):
                items = parsed
            else:
                items = []

            logger.debug(f"  🤖 Groq [{model}]: {len(items)} items ← {url[:50]}")
            return [i for i in items if isinstance(i, dict) and not i.get("error")]

        except json.JSONDecodeError:
            logger.debug(f"  Groq JSON failed [{model}]")
            continue
        except Exception as e:
            logger.debug(f"  Groq error [{model}]: {e}")
            continue

    return []


# ─────────────────────────────────────────────────────────────
# Layer 3: PostgreSQL grounding — verify against villes/pays
# ─────────────────────────────────────────────────────────────

async def ground_city_country(
    destination: Optional[str],
    pays_raw: Optional[str],
    db: DatabaseManager,
) -> Tuple[Optional[str], Optional[str]]:

    canonical_city    = None
    canonical_country = None

    if not destination and not pays_raw:
        return None, None

    try:
        async with db.engine.connect() as conn:

            # Step 1: Verify country against pays table
            if pays_raw and len(pays_raw.strip()) > 1:
                r = await conn.execute(
                    text("SELECT nom FROM pays WHERE LOWER(nom) = LOWER(:c) LIMIT 1"),
                    {"c": pays_raw.strip()},
                )
                row = r.fetchone()
                if row:
                    canonical_country = row[0]

            # Step 2: Verify city — must not be an attraction
            if destination and len(destination.strip()) > 2:
                dest = destination.strip()

                # Layer 5 check: attraction names are NOT cities
                if _is_attraction(dest):
                    logger.debug(f"    🏛  '{dest}' = attraction → skip as city")
                    return None, canonical_country

                # Try city + country match first (most accurate)
                if canonical_country:
                    r = await conn.execute(
                        text("""
                            SELECT v.name
                            FROM villes v
                            JOIN pays p ON v.pays_id = p.id
                            WHERE LOWER(v.name) = LOWER(:city)
                              AND LOWER(p.nom)  = LOWER(:country)
                            LIMIT 1
                        """),
                        {"city": dest, "country": canonical_country},
                    )
                else:
                    r = await conn.execute(
                        text("SELECT name FROM villes WHERE LOWER(name) = LOWER(:city) LIMIT 1"),
                        {"city": dest},
                    )

                row = r.fetchone()
                if row:
                    canonical_city = row[0]
                    logger.debug(f"    ✅ DB verified: '{canonical_city}', '{canonical_country}'")
                else:

                    canonical_city = dest[:150]
                    logger.debug(f"    🔍 '{dest}' not in villes")

    except Exception as e:
        logger.warning(f"    ground_city_country error: {e}")

    return canonical_city, canonical_country


# ─────────────────────────────────────────────────────────────
# URL discovery
# ─────────────────────────────────────────────────────────────

async def discover_product_urls(domain: str) -> List[str]:
    """Find product page URLs via sitemap or homepage links."""
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15),
        headers=HEADERS,
        follow_redirects=True,
    ) as client:
        urls = await _try_sitemap(domain, client)
        if not urls:
            urls = await _scan_homepage(domain, client)

    product_urls = [u for u in urls if _is_valid_url(u)]
    logger.debug(f"    {len(product_urls)} valid product URLs from {len(urls)} total")
    return product_urls[:100]


async def _try_sitemap(domain: str, client) -> List[str]:
    for scheme in ("https://", "http://"):
        try:
            r = await client.get(f"{scheme}{domain}/sitemap.xml", timeout=10)
            if r.status_code != 200:
                continue
            from bs4 import BeautifulSoup
            locs = BeautifulSoup(r.text, "lxml-xml").find_all("loc")
            if locs:
                return [l.get_text().strip() for l in locs]
        except Exception:
            continue
    return []


async def _scan_homepage(domain: str, client) -> List[str]:
    for scheme in ("https://", "http://"):
        try:
            r = await client.get(f"{scheme}{domain}", timeout=10)
            if r.status_code >= 400:
                continue
            from bs4 import BeautifulSoup
            links = []
            for a in BeautifulSoup(r.text, "html.parser").find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("/"):
                    href = f"{scheme}{domain}{href}"
                elif not href.startswith("http"):
                    continue
                if domain in href:
                    links.append(href)
            return list(dict.fromkeys(links))
        except Exception:
            continue
    return []


# ─────────────────────────────────────────────────────────────
# Items → Produit dicts (respected.txt fields)
# ─────────────────────────────────────────────────────────────

def _classify_activity(nom: str, desc: str) -> str:
    text = f"{nom} {desc}".lower()
    if any(kw in text for kw in ["transfer","shuttle","taxi","airport","navette","chauffeur","driver"]):
        return "TRANSFER"
    if any(kw in text for kw in ["ticket","entrance","admission","entry","museum","monument","skip the line"]):
        return "TICKET"
    return "EXCURSION"


async def build_produits(
    items: List[Dict],
    supplier_id: Optional[int],
    run_id: str,
    fallback_url: str,
    db: DatabaseManager,
) -> List[Dict]:
    """
    Apply all 5 layers to convert Groq items → validated Produit dicts.
    Respects respected.txt field names exactly.
    """
    result = []
    seen   = set()

    for item in items:
        if not isinstance(item, dict):
            continue

        # ── Layer 5: title validation ─────────────────────────
        nom = _sanitize((item.get("nom_produit") or "").strip())
        if not nom or nom in seen or _is_garbage(nom):
            continue
        seen.add(nom)

        # ── Layer 5: price validation ─────────────────────────
        prix, devise = _parse_price(item.get("prix_raw_text", ""))
        # Null is better than hallucination — outside bounds → None already handled

        # ── Layer 2: activity type ────────────────────────────
        canonical = (item.get("canonical_activity_type") or "").upper().strip()
        if canonical not in {"EXCURSION", "TICKET", "TRANSFER"}:
            canonical = _classify_activity(nom, item.get("description", "") or "")

        # ── Layer 3: PostgreSQL grounding ─────────────────────
        destination = (item.get("destination") or "").strip()
        pays_raw    = (item.get("pays_raw") or "").strip()

        # URL country slug as zero-cost fallback
        if not pays_raw:
            pays_raw = _country_from_url(fallback_url) or ""

        canonical_city, canonical_country = await ground_city_country(
            destination, pays_raw, db
        )

        # ── Layer 5: sanitize all text fields ─────────────────
        desc       = _sanitize((item.get("description") or "").strip())[:2000]
        duree      = _sanitize((item.get("duree") or "").strip()) or None
        url_source = _sanitize((item.get("url_produit") or fallback_url or "").strip())[:1000]

        # ── Produit dict (respected.txt fields) ───────────────
        result.append({
            "fournisseur_id": supplier_id,
            "run_id":         run_id,
            "nom_produit":    nom[:500],
            "description":    desc or None,
            "prix":           prix,
            "devise":         devise,
            "duree":          duree,
            "categorie_raw":  None,            # Groq uses canonical_activity_type
            "ville_raw":      canonical_city,  # DB-verified or raw hint or None
            "pays_raw":       canonical_country,
            "source":         "firecrawl_groq",
            "url_source":     url_source or None,
            "booking_url":    url_source or None,
            "image_url":      None,
            # metadata passed to normalization pipeline
            "_canonical_type": canonical,
        })

    return result


# ─────────────────────────────────────────────────────────────
# ProductScraper
# ─────────────────────────────────────────────────────────────

class ProductScraper:


    def __init__(self):
        self.db = DatabaseManager()

    async def scrape_supplier(
        self,
        supplier: Dict,
        max_products: int = 20,
        run_id: str = "",
    ) -> List[Dict]:
        domain      = supplier.get("domain", "")
        supplier_id = supplier.get("id")

        if not domain:
            return []

        fc  = "✅" if config.firecrawl_configured() else "❌ (set FIRECRAWL_API_KEY)"
        gr  = f"✅ {config.groq_key_count()} key(s)" if config.groq_configured() else "❌ (set GROQ_API_KEY)"
        logger.info(f"  🕷  {domain} | Firecrawl:{fc} Groq:{gr} max={max_products}")

        urls = await discover_product_urls(domain)
        if not urls:
            logger.debug(f"    No product URLs for {domain}")
            return []

        products    = []
        seen_titles = set()

        for url in urls[:max_products * 3]:
            if len(products) >= max_products:
                break

            # Layer 1: Firecrawl
            markdown = await firecrawl_scrape(url)
            if not markdown:
                continue

            # Rate limit respect — 30 RPM Groq
            await asyncio.sleep(1.5)

            # Layer 2: Groq reasoning
            items = await groq_extract(markdown, url)
            if not items:
                continue

            # Layers 3+5: ground + validate
            new_products = await build_produits(
                items        = items,
                supplier_id  = supplier_id,
                run_id       = run_id,
                fallback_url = url,
                db           = self.db,
            )

            added = 0
            for p in new_products:
                nom = p.get("nom_produit", "")
                if nom and nom not in seen_titles:
                    seen_titles.add(nom)
                    products.append(p)
                    added += 1

            if added:
                logger.debug(f"    +{added} from {url[:60]}")

        logger.info(f"  ✅ {domain}: {len(products)} products")
        return products[:max_products]

    async def close(self):
        pass


def COUNTRY_SLUG_MAP():
    return None