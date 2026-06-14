"""
scrapers/product_scraper.py — FRESH REWRITE

5-LAYER ARCHITECTURE:
  Layer 1 — Firecrawl PRIMARY  → clean markdown
             ScrapingBee FALLBACK → HTML → BeautifulSoup → markdown-like text
  Layer 2 — Groq (reasoning)  → markdown → structured JSON product list
  Layer 3 — PostgreSQL grounding → city/country verified against villes/pays
  Layer 5 — Validation (safety) → title, price, URL, NUL bytes

KEY FIXES vs. previous version:
  ✅ ScrapingBee fallback when Firecrawl fails or credits exhausted (402)
  ✅ Concurrent URL scraping (semaphore-limited, not sequential)
  ✅ Removed fixed asyncio.sleep(1.5) — only sleeps on 429 responses
  ✅ Strict max_products enforcement — stops as soon as target reached
  ✅ URL limit = max_products * 2 (not *3), avoids over-fetching
  ✅ Groq multi-key rotation preserved
  ✅ All 5 validation layers preserved
  ✅ source = "firecrawl_groq" or "scrapingbee_groq"
"""

import asyncio
import itertools
import json
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import text

from config import config
from database import DatabaseManager


# ─────────────────────────────────────────────────────────────
# Pydantic schema — matches GROQ.txt exactly
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
# Groq multi-key rotation
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
# Layer 5 — Validation constants
# ─────────────────────────────────────────────────────────────

_REJECT_EXT = {
    ".avif", ".webp", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".mp4", ".mp3", ".pdf", ".zip", ".gz", ".css", ".js",
    ".woff", ".woff2",
}

_REJECT_URL_PAT = [
    r"/insecure/plain/", r"s3://", r"/thumbnail", r"/thumb/",
    r"/img/", r"/images?/", r"/assets/", r"/static/", r"/cdn/",
]

_PRODUCT_URL_PAT = [
    r"/tour[s]?/", r"/excursion[s]?/", r"/activit(y|ies)/",
    r"/ticket[s]?/", r"/transfer[s]?/", r"/experience[s]?/",
    r"/product[s]?/", r"/package[s]?/", r"/trip[s]?/",
    r"/things-to-do/", r"/visit[s]?/", r"/safari[s]?/",
    r"/day-tour[s]?/", r"/guide[s]?/",
]

_GARBAGE_TITLES = {
    "home", "about us", "contact", "contact us", "activities", "tours",
    "transfers", "tickets", "book now", "reserve now", "check availability",
    "tour essentials", "what's included", "itinerary at a glance",
    "what is included", "not included", "important information",
    "overview", "highlights", "details", "gallery", "reviews", "faq",
    "terms and conditions", "cancellation policy", "museum shop",
    "gift shop", "buy a ticket", "auditorium", "error", "404", "403",
    "unsere besuche", "karte & öffnungszeiten", "öffnungszeiten",
    "kontakt", "nos visites", "plan & horaires", "réservation", "accueil",
    "orari", "contatti", "horarios", "mapa", "contacto",
}

_GARBAGE_PAT = [
    r"^what'?s\s+included", r"^itinerary\s+at",
    r"^tour\s+essentials?$", r"^book\s+",
    r"^auditorium\s+\d+", r"^gift\s+shop",
    r"^all\s+\w+$", r"^page\s+\d+$",
    r"^buy\s+a?\s+ticket",
]

_ATTRACTION_KW = {
    "museum", "musée", "museo", "palace", "palais", "castle", "château",
    "gallery", "galerie", "cathedral", "temple", "basilica", "monument",
    "tower", "eiffel", "colosseum", "acropolis", "vatican", "louvre",
    "sagrada", "alhambra", "pyramid", "sphinx", "parthenon", "ruins",
    "amphitheater", "theatre", "opera house", "waterfall", "cave",
}

_PRICE_MIN = getattr(config, "PRICE_MIN_EUR", 1.0)
_PRICE_MAX = getattr(config, "PRICE_MAX_EUR", 5000.0)

_COUNTRY_URL_SLUGS = {
    "france": "France", "italy": "Italy", "spain": "Spain", "greece": "Greece",
    "egypt": "Egypt", "morocco": "Morocco", "tunisia": "Tunisia", "china": "China",
    "japan": "Japan", "thailand": "Thailand", "indonesia": "Indonesia", "bali": "Indonesia",
    "usa": "United States", "south-africa": "South Africa", "kenya": "Kenya",
    "india": "India", "portugal": "Portugal", "germany": "Germany", "turkey": "Turkey",
    "jordan": "Jordan", "australia": "Australia", "brazil": "Brazil", "peru": "Peru",
    "mexico": "Mexico", "vietnam": "Vietnam", "cambodia": "Cambodia",
    "malaysia": "Malaysia", "singapore": "Singapore", "uae": "United Arab Emirates",
    "dubai": "United Arab Emirates", "qatar": "Qatar",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    ),
}


# ─────────────────────────────────────────────────────────────
# Layer 5 — Validation helpers
# ─────────────────────────────────────────────────────────────

def _is_valid_url(url: str) -> bool:
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
    min_chars = getattr(config, "TITLE_MIN_CHARS", 8)
    min_words = getattr(config, "TITLE_MIN_WORDS", 2)
    if len(nom) < min_chars or len(nom.split()) < min_words:
        return True
    if n in _GARBAGE_TITLES:
        return True
    return any(re.search(p, n) for p in _GARBAGE_PAT)


def _is_attraction(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _ATTRACTION_KW)


def _parse_price(raw: str) -> Tuple[Optional[float], str]:
    if not raw:
        return None, "EUR"
    text = str(raw).strip()
    currency = "EUR"
    if "€" in text or "EUR" in text.upper():
        currency = "EUR"
    elif "$" in text or "USD" in text.upper():
        currency = "USD"
    elif "£" in text or "GBP" in text.upper():
        currency = "GBP"
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


def _classify_activity(nom: str, desc: str) -> str:
    text = f"{nom} {desc}".lower()
    if any(kw in text for kw in ["transfer", "shuttle", "taxi", "airport", "navette", "chauffeur", "driver"]):
        return "TRANSFER"
    if any(kw in text for kw in ["ticket", "entrance", "admission", "entry", "museum", "monument", "skip the line"]):
        return "TICKET"
    return "EXCURSION"


# ─────────────────────────────────────────────────────────────
# Layer 1a — Firecrawl (PRIMARY scraper)
# ─────────────────────────────────────────────────────────────

async def firecrawl_scrape(url: str, client: httpx.AsyncClient) -> Tuple[Optional[str], str]:
    """
    Layer 1a — Firecrawl fetches URL → clean markdown.
    Returns (content, source_tag) where source_tag is 'firecrawl_groq'.
    Returns (None, '') on failure — caller switches to ScrapingBee.
    """
    if not config.firecrawl_configured():
        return None, ""

    try:
        resp = await client.post(
            f"{config.FIRECRAWL_URL}/v1/scrape",
            headers={
                "Authorization": f"Bearer {config.FIRECRAWL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": True,
                "excludeTags": ["nav", "footer", "header", "aside", "script", "style", "form", "iframe"],
                "waitFor": 1000,
            },
            timeout=httpx.Timeout(30.0),
        )

        if resp.status_code == 402:
            logger.warning("  💳 Firecrawl: credits exhausted → fallback to ScrapingBee")
            return None, ""

        if resp.status_code != 200:
            logger.debug(f"  🔥 Firecrawl {resp.status_code}: {url[:60]}")
            return None, ""

        data     = resp.json()
        markdown = data.get("data", {}).get("markdown", "")

        if not markdown or len(markdown) < 50:
            return None, ""

        logger.debug(f"  🔥 Firecrawl OK: {len(markdown)} chars ← {url[:60]}")
        return markdown, "firecrawl_groq"

    except Exception as e:
        logger.debug(f"  🔥 Firecrawl error {url[:60]}: {e}")
        return None, ""


# ─────────────────────────────────────────────────────────────
# Layer 1b — ScrapingBee (FALLBACK scraper)
# ─────────────────────────────────────────────────────────────

SCRAPINGBEE_BASE = "https://app.scrapingbee.com/api/v1"

def _html_to_text(html: str) -> str:
    """Convert raw HTML from ScrapingBee to clean text for Groq."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Remove noise tags
        for tag in soup(["nav", "footer", "header", "aside", "script", "style", "form", "iframe", "noscript"]):
            tag.decompose()
        # Get text, preserve line breaks for headings and paragraphs
        lines = []
        for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "td", "th", "span", "div"]):
            t = el.get_text(separator=" ", strip=True)
            if t and len(t) > 2:
                lines.append(t)
        text = "\n".join(lines)
        # Collapse excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()
    except Exception as e:
        logger.debug(f"  🐝 HTML→text error: {e}")
        return ""


async def scrapingbee_scrape(url: str, client: httpx.AsyncClient) -> Tuple[Optional[str], str]:
    """
    Layer 1b — ScrapingBee fetches URL → raw HTML → converted to clean text.
    Returns (content, source_tag) where source_tag is 'scrapingbee_groq'.
    Returns (None, '') on failure.
    """
    api_key = getattr(config, "SCRAPINGBEE_API_KEY", "")
    if not api_key:
        return None, ""

    try:
        params = {
            "api_key":        api_key,
            "url":            url,
            "render_js":      "true",
            "wait":           "3000",
            "block_ads":      "true",
            "block_resources":"false",
        }
        resp = await client.get(SCRAPINGBEE_BASE, params=params, timeout=httpx.Timeout(45.0))

        if resp.status_code != 200:
            logger.debug(f"  🐝 ScrapingBee {resp.status_code}: {url[:60]}")
            return None, ""

        html = resp.text
        if not html or len(html) < 100:
            return None, ""

        text = _html_to_text(html)
        if not text or len(text) < 50:
            return None, ""

        logger.debug(f"  🐝 ScrapingBee OK: {len(text)} chars ← {url[:60]}")
        return text, "scrapingbee_groq"

    except Exception as e:
        logger.debug(f"  🐝 ScrapingBee error {url[:60]}: {e}")
        return None, ""


# ─────────────────────────────────────────────────────────────
# Layer 1 — Unified acquisition: Firecrawl → ScrapingBee fallback
# ─────────────────────────────────────────────────────────────

async def acquire_page_content(url: str, client: httpx.AsyncClient) -> Tuple[Optional[str], str]:
    """
    Try Firecrawl first. If it fails (any reason), fall back to ScrapingBee.
    Returns (content, source_tag) or (None, '') if both fail.
    """
    content, source = await firecrawl_scrape(url, client)
    if content:
        return content, source

    logger.debug(f"  ↩ Firecrawl failed for {url[:60]} — trying ScrapingBee")
    content, source = await scrapingbee_scrape(url, client)
    return content, source


# ─────────────────────────────────────────────────────────────
# Layer 2 — Groq reasoning: content → structured product list
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


async def groq_extract(content: str, url: str) -> List[Dict]:
    """
    Layer 2 — Groq reasoning: content → structured product list.
    Multi-key rotation + tiered model fallback.
    Returns [] if all attempts fail. No fixed sleep — only on 429.
    """
    if not config.groq_configured():
        logger.warning("  ⚠️ No Groq keys configured")
        return []

    schema = TravelProductListSchema.model_json_schema()
    prompt = (
        f"URL: {url}\n\nPAGE CONTENT:\n{content[:5000]}\n\n"
        f"{_GROQ_INSTRUCTION}\n\n"
        f"Return JSON matching schema:\n{json.dumps(schema, indent=2)}"
    )

    models = [
        getattr(config, "GROQ_PRIMARY_MODEL",   "llama-3.1-8b-instant"),
        getattr(config, "GROQ_SECONDARY_MODEL", "llama3-8b-8192"),
        getattr(config, "GROQ_FALLBACK_MODEL",  "mixtral-8x7b-32768"),
    ]

    async with httpx.AsyncClient(timeout=httpx.Timeout(45.0)) as groq_client:
        for attempt, model in enumerate(models):
            api_key = _next_groq_key()
            if not api_key:
                break

            try:
                resp = await groq_client.post(
                    f"{getattr(config, 'GROQ_BASE_URL', 'https://api.groq.com/openai/v1')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type":  "application/json",
                    },
                    json={
                        "model":           model,
                        "messages": [
                            {"role": "system", "content": _GROQ_SYSTEM},
                            {"role": "user",   "content": prompt},
                        ],
                        "temperature":     0.1,
                        "max_tokens":      2000,
                        "response_format": {"type": "json_object"},
                    },
                )

                if resp.status_code == 429:
                    wait = 2.0 + attempt * 2.0
                    logger.info(f"  ⏳ Groq 429 [{model}] → waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue

                if resp.status_code != 200:
                    logger.debug(f"  Groq {resp.status_code} [{model}]")
                    continue

                raw_content = resp.json()["choices"][0]["message"]["content"]
                clean       = re.sub(r"```(?:json)?|```", "", raw_content).strip()
                parsed      = json.loads(clean)

                if isinstance(parsed, dict) and "products" in parsed:
                    items = parsed["products"]
                elif isinstance(parsed, list):
                    items = parsed
                else:
                    items = []

                valid = [i for i in items if isinstance(i, dict) and not i.get("error")]
                logger.debug(f"  🤖 Groq [{model}]: {len(valid)} items ← {url[:50]}")
                return valid

            except json.JSONDecodeError:
                logger.debug(f"  Groq JSON parse failed [{model}]")
                continue
            except Exception as e:
                logger.debug(f"  Groq error [{model}]: {e}")
                continue

    return []


# ─────────────────────────────────────────────────────────────
# Layer 3 — PostgreSQL grounding
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

            if pays_raw and len(pays_raw.strip()) > 1:
                r = await conn.execute(
                    text("SELECT nom FROM pays WHERE LOWER(nom) = LOWER(:c) LIMIT 1"),
                    {"c": pays_raw.strip()},
                )
                row = r.fetchone()
                if row:
                    canonical_country = row[0]

            if destination and len(destination.strip()) > 2:
                dest = destination.strip()

                if _is_attraction(dest):
                    logger.debug(f"    🏛 '{dest}' = attraction → skip as city")
                    return None, canonical_country

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
                    logger.debug(f"    ✅ Verified: '{canonical_city}', '{canonical_country}'")
                else:
                    canonical_city = dest[:150]
                    logger.debug(f"    🔍 '{dest}' not in villes — storing as hint")

    except Exception as e:
        logger.warning(f"    ground_city_country error: {e}")

    return canonical_city, canonical_country


# ─────────────────────────────────────────────────────────────
# URL discovery
# ─────────────────────────────────────────────────────────────

async def discover_product_urls(domain: str) -> List[str]:
    """Find product page URLs via sitemap first, then homepage links."""
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15),
        headers=HEADERS,
        follow_redirects=True,
    ) as client:
        urls = await _try_sitemap(domain, client)
        if not urls:
            urls = await _scan_homepage(domain, client)

    product_urls = [u for u in urls if _is_valid_url(u)]
    logger.debug(f"    📋 {len(product_urls)} valid product URLs from {len(urls)} total — {domain}")
    return product_urls[:100]


async def _try_sitemap(domain: str, client: httpx.AsyncClient) -> List[str]:
    for scheme in ("https://", "http://"):
        try:
            r = await client.get(f"{scheme}{domain}/sitemap.xml", timeout=10)
            if r.status_code != 200:
                continue
            locs = BeautifulSoup(r.text, "lxml-xml").find_all("loc")
            if locs:
                return [l.get_text().strip() for l in locs]
        except Exception:
            continue
    return []


async def _scan_homepage(domain: str, client: httpx.AsyncClient) -> List[str]:
    for scheme in ("https://", "http://"):
        try:
            r = await client.get(f"{scheme}{domain}", timeout=10)
            if r.status_code >= 400:
                continue
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
# Layers 3+5 — Build validated product dicts
# ─────────────────────────────────────────────────────────────

async def build_produits(
    items:        List[Dict],
    supplier_id:  Optional[int],
    run_id:       str,
    fallback_url: str,
    source_tag:   str,
    db:           DatabaseManager,
) -> List[Dict]:
    """Apply all layers to convert Groq items → validated Produit dicts."""
    result = []
    seen   = set()

    for item in items:
        if not isinstance(item, dict):
            continue

        # Layer 5: title validation
        nom = _sanitize((item.get("nom_produit") or "").strip())
        if not nom or nom in seen or _is_garbage(nom):
            continue
        seen.add(nom)

        # Layer 5: price validation
        prix, devise = _parse_price(item.get("prix_raw_text", ""))

        # Layer 2: activity type
        canonical = (item.get("canonical_activity_type") or "").upper().strip()
        if canonical not in {"EXCURSION", "TICKET", "TRANSFER"}:
            canonical = _classify_activity(nom, item.get("description", "") or "")

        # Layer 3: PostgreSQL grounding
        destination = (item.get("destination") or "").strip()
        pays_raw    = (item.get("pays_raw") or "").strip()

        if not pays_raw:
            pays_raw = _country_from_url(fallback_url) or ""

        canonical_city, canonical_country = await ground_city_country(
            destination, pays_raw, db
        )

        # Layer 5: sanitize all text fields
        desc      = _sanitize((item.get("description") or "").strip())[:2000]
        duree     = _sanitize((item.get("duree") or "").strip()) or None
        url_src   = _sanitize((item.get("url_produit") or fallback_url or "").strip())[:1000]

        result.append({
            "fournisseur_id":   supplier_id,
            "run_id":           run_id,
            "nom_produit":      nom[:500],
            "description":      desc or None,
            "prix":             prix,
            "devise":           devise,
            "duree":            duree,
            "categorie_raw":    None,
            "ville_raw":        canonical_city,
            "pays_raw":         canonical_country,
            "source":           source_tag,
            "url_source":       url_src or None,
            "booking_url":      url_src or None,
            "image_url":        None,
            "_canonical_type":  canonical,
        })

    return result


# ─────────────────────────────────────────────────────────────
# Core per-URL worker (concurrent)
# ─────────────────────────────────────────────────────────────

async def _scrape_one_url(
    url:         str,
    supplier_id: Optional[int],
    run_id:      str,
    db:          DatabaseManager,
    http_client: httpx.AsyncClient,
    semaphore:   asyncio.Semaphore,
) -> List[Dict]:
    """
    Acquire content + extract products for a single URL.
    Wrapped in semaphore so we never fire more than N concurrent requests.
    """
    async with semaphore:
        # Layer 1: Firecrawl → ScrapingBee fallback
        content, source_tag = await acquire_page_content(url, http_client)
        if not content:
            return []

        # Layer 2: Groq (no fixed sleep — only sleeps on 429 inside groq_extract)
        items = await groq_extract(content, url)
        if not items:
            return []

        # Layers 3 + 5: ground + validate
        products = await build_produits(
            items        = items,
            supplier_id  = supplier_id,
            run_id       = run_id,
            fallback_url = url,
            source_tag   = source_tag,
            db           = db,
        )
        return products


# ─────────────────────────────────────────────────────────────
# ProductScraper — public interface
# ─────────────────────────────────────────────────────────────

# Max concurrent scraping requests per supplier
_MAX_CONCURRENT = 5


class ProductScraper:
    """
    Scrapes products from a supplier website.

    Flow per supplier:
      1. Discover product URLs (sitemap → homepage)
      2. Scrape URLs concurrently (semaphore-limited)
         - Each URL: Firecrawl → ScrapingBee fallback → Groq extraction
      3. Stop as soon as max_products reached
      4. Return deduplicated product list
    """

    def __init__(self):
        self.db = DatabaseManager()

    async def scrape_supplier(
        self,
        supplier:     Dict,
        max_products: int  = 20,
        run_id:       str  = "",
    ) -> List[Dict]:

        domain      = supplier.get("domain", "")
        supplier_id = supplier.get("id")

        if not domain:
            return []

        fc_status = "✅" if config.firecrawl_configured() else "❌ missing key"
        sb_key    = getattr(config, "SCRAPINGBEE_API_KEY", "")
        sb_status = "✅" if sb_key else "❌ missing key"
        gr_status = f"✅ {config.groq_key_count()} key(s)" if config.groq_configured() else "❌ missing key"

        logger.info(
            f"  🕷 {domain} | "
            f"Firecrawl:{fc_status} | ScrapingBee:{sb_status} | "
            f"Groq:{gr_status} | max={max_products}"
        )

        # Discover product URLs
        all_urls = await discover_product_urls(domain)
        if not all_urls:
            logger.debug(f"    No product URLs found for {domain}")
            return []

        # Limit URL pool to max_products * 2 (not *3, prevents over-fetching)
        url_pool = all_urls[: max_products * 2]
        logger.debug(f"    URL pool: {len(url_pool)} candidates for {domain}")

        products:    List[Dict] = []
        seen_titles: set        = set()

        semaphore   = asyncio.Semaphore(_MAX_CONCURRENT)

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(35.0),
            headers=HEADERS,
            follow_redirects=True,
        ) as http_client:

            # Launch concurrent tasks for all URLs in the pool
            tasks = [
                _scrape_one_url(url, supplier_id, run_id, self.db, http_client, semaphore)
                for url in url_pool
            ]

            # Process results as they complete; stop at max_products
            for coro in asyncio.as_completed(tasks):
                if len(products) >= max_products:
                    break

                try:
                    new_items = await coro
                except Exception as e:
                    logger.debug(f"    URL task error: {e}")
                    continue

                added = 0
                for p in new_items:
                    if len(products) >= max_products:
                        break
                    nom = p.get("nom_produit", "")
                    if nom and nom not in seen_titles:
                        seen_titles.add(nom)
                        products.append(p)
                        added += 1

                if added:
                    logger.debug(
                        f"    +{added} products | "
                        f"total={len(products)}/{max_products} | {domain}"
                    )

        final = products[:max_products]
        logger.info(
            f"  ✅ {domain}: {len(final)} products collected "
            f"(target={max_products})"
        )
        return final

    async def close(self):
        pass