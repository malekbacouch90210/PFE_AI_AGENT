"""
scrappa_vs_serpapi_comparison.py
=================================
Experiment: Scrappa.co vs SerpAPI — Supplier Discovery Comparison

Rules (same as FournisseurScraper):
  1. Supplier MUST have a website (domain != empty)
  2. Supplier MUST have API or Channel Manager or BOTH
  3. No activity type filtering in queries — tourism only
  4. Uses existing ScrappaClient for Scrappa.co
  5. Saves to new table: supplier_discovery_comparison

DB: Sourcing @ localhost:5432 (postgres / edward_alphanso)
Run: python scrappa_vs_serpapi_comparison.py
"""

import asyncio
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
from loguru import logger
from sqlalchemy import (
    Boolean, Column, DateTime, Integer,
    Numeric, String, Text,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

# Use the project's existing ScrappaClient
# Assumes this file sits at project root alongside scrapers/
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from scrapers.apis.scrappa_client import ScrappaClient
    SCRAPPA_CLIENT_AVAILABLE = True
except ImportError:
    SCRAPPA_CLIENT_AVAILABLE = False
    logger.warning("ScrappaClient not found — Scrappa.co search will be skipped")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

DATABASE_URL = "postgresql+psycopg://postgres:edward_alphanso@localhost:5432/Sourcing"

SCRAPPA_API_KEY = ""
SERPAPI_KEY     = ""

TEST_COUNTRIES        = ["Tunisia", "Morocco", "Italy", "Thailand", "Peru"]
MAX_RESULTS_PER_QUERY = 10
REQUEST_TIMEOUT       = 30
CONCURRENT_HOMEPAGE   = 5   # semaphore for homepage scoring


# ─────────────────────────────────────────────────────────────
# ORM — NEW TABLE only, fournisseurs untouched
# ─────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class SupplierDiscoveryComparison(Base):
    """
    Mirrors Fournisseur model exactly.
    Extra columns: api_source, query_used, fetch_time_ms, raw_result_count.
    SEPARATE table — fournisseurs is never modified.
    """
    __tablename__ = "supplier_discovery_comparison"

    id               = Column(Integer,      primary_key=True, autoincrement=True)
    nom              = Column(String(255),  nullable=False)
    ville            = Column(String(255),  nullable=True)
    adresse          = Column(Text)
    telephone        = Column(String(50))
    domain           = Column(String(500),  index=True)
    rating           = Column(Numeric(3, 2))
    nb_avis          = Column(Integer)
    score            = Column(Integer,      default=0)
    status           = Column(String(30),   default="discovered", index=True)
    has_api          = Column(Boolean,      default=False)
    api_name         = Column(String(100))
    has_channel      = Column(Boolean,      default=False)
    channel_name     = Column(String(100))
    type_connexion   = Column(String(20),   default="NONE")
    is_marketplace   = Column(Boolean,      default=False)
    marketplace_type = Column(String(50))
    # Comparison-specific
    api_source       = Column(String(30),   nullable=False, index=True)
    query_used       = Column(Text)
    country          = Column(String(100),  index=True)
    fetch_time_ms    = Column(Integer)
    raw_result_count = Column(Integer)
    description      = Column(Text)
    url              = Column(Text)
    created_at       = Column(DateTime,     default=datetime.utcnow)


# ─────────────────────────────────────────────────────────────
# SIGNALS — identical to FournisseurScraper
# ─────────────────────────────────────────────────────────────

CM_SIGNALS: Dict[str, str] = {
    "bokun": "Bokun", "rezdy": "Rezdy", "fareharbor": "FareHarbor",
    "ventrata": "Ventrata", "xola": "Xola", "checkfront": "Checkfront",
    "trekksoft": "TrekkSoft", "regiondo": "Regiondo", "peek.com": "Peek",
    "musement": "Musement", "tiqets": "Tiqets", "headout": "Headout",
    "bookingkit": "BookingKit", "planyo": "Planyo", "rezgo": "Rezgo",
    "viator": "Viator", "getyourguide": "GetYourGuide",
    "klook": "Klook", "kkday": "KKday", "civitatis": "Civitatis",
    "pelago": "Pelago", "12go": "12Go Asia", "withlocals": "WithLocals",
    "ctrip": "Ctrip", "trip.com": "Trip.com", "traveloka": "Traveloka",
    "despegar": "Despegar", "wetu.com": "Wetu", "tourplan": "TourPlan",
    "tripadvisor.com/experiences": "Tripadvisor Exp",
    "airbnb.com/experiences": "Airbnb Experiences",
    "partner.viator": "Viator Partner",
    "supplier.getyourguide": "GYG Supplier",
    "supplier.klook": "Klook Supplier",
}

API_SIGNALS = [
    "/api/v1", "/api/v2", "/api/v3", "swagger", "openapi", "api-docs",
    "api.viator", "api.getyourguide", "api.klook", "api.ctrip",
    "api.bokun", "api.rezdy", "api.fareharbor",
    "datafeed", "data-feed", "channelmanager", "channel-manager",
    "connectivity", "integration/api", "partner-portal",
    "supplier-portal", "extranet", "partners.viator",
    "xmlfeed", "xml-feed",
]

SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "tiktok.com",
    "tripadvisor.com", "yelp.com", "booking.com", "airbnb.com",
    "google.com", "maps.google.com", "wikipedia.org", "expedia.com",
    "trustpilot.com", "lonelyplanet.com", "whatsapp.com",
}

NOT_SUPPLIER = {
    "mall", "cinema", "hospital", "university", "furniture",
    "bank", "pharmacy", "supermarket", "school", "restaurant",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


# ─────────────────────────────────────────────────────────────
# QUERY BUILDER — no activity types, tourism-focused only
# ─────────────────────────────────────────────────────────────

def build_queries(country: str) -> List[str]:
    raw = [
        f"tour operator {country}",
        f"excursion operator {country}",
        f"travel agency {country} online booking",
        f"tour company {country} website",
        f"local tour operator {country}",
        f"tour guide company {country}",
        f"tourism company {country}",
        f"travel supplier {country} booking",
        f"tour operator {country} bokun",
        f"tour operator {country} fareharbor",
        f"tour operator {country} rezdy",
        f"tour operator {country} viator",
        f"tour company {country} getyourguide",
        f"excursion company {country} klook",
        f"tour operator {country} channel manager",
        f"experience provider {country} booking system",
        f"airport transfer {country} online booking",
        f"private transfer {country} booking",
        f"museum tickets {country} online",
        f"attraction tickets {country} booking",
    ]
    seen, unique = set(), []
    for q in raw:
        if q not in seen:
            seen.add(q)
            unique.append(q)
    return unique


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def extract_domain(raw: str) -> str:
    if not raw:
        return ""
    raw = re.sub(r"https?://", "", raw.strip().lower())
    raw = re.sub(r"^www\.", "", raw)
    return raw.split("/")[0].split("?")[0].split("#")[0].strip()


def detect_connexion(
    nom: str, description: str, domain: str
) -> Tuple[bool, Optional[str], bool, Optional[str], str]:
    """
    Returns: has_api, api_name, has_channel, channel_name, type_connexion
    Same logic as FournisseurScraper._score()
    """
    txt = f"{nom} {description} {domain}".lower()
    has_api      = any(sig in txt for sig in API_SIGNALS)
    channel_name = None
    for sig, name in CM_SIGNALS.items():
        if sig in txt:
            channel_name = name
            break
    has_channel = channel_name is not None

    if has_api and has_channel:
        tc = "BOTH"
    elif has_api:
        tc = "API"
    elif has_channel:
        tc = "CHANNEL"
    else:
        tc = "NONE"

    api_name = "API_DIRECT" if has_api else None
    return has_api, api_name, has_channel, channel_name, tc


def is_connected(has_api: bool, has_channel: bool) -> bool:
    """Hard rule: must have API or Channel Manager or BOTH."""
    return has_api or has_channel


def has_website(domain: str) -> bool:
    """Hard rule: must have a real website domain."""
    return bool(domain) and len(domain) >= 5


def is_travel_supplier(nom: str, domain: str) -> bool:
    combined = f"{nom} {domain}".lower()
    return any(kw in combined for kw in [
        "tour", "travel", "transfer", "excursion", "ticket",
        "shuttle", "activity", "experience", "trip", "booking",
        "safari", "cruise", "museum", "sightseeing", "adventure",
        "guide", "tourism", "cultural",
    ])


def score_from_html(html: str) -> int:
    """Same scoring as FournisseurScraper._score()"""
    h = html.lower()
    score = 0
    for sig in CM_SIGNALS:
        if sig in h:
            score += 50
            break
    if any(s in h for s in API_SIGNALS):
        score += 35
    if any(s in h for s in ["/tours/", "/excursions/", "/activities/", "/tickets/", "/transfers/"]):
        score += 10
    if any(s in h for s in ["book now", "book online", "reserve now", "réserver", "jetzt buchen"]):
        score += 5
    if any(s in h for s in ["from $", "from €", '"price":', "per person"]):
        score += 5
    return min(score, 100)


# ─────────────────────────────────────────────────────────────
# SCRAPPA.CO — uses existing ScrappaClient.search_google_maps()
# ─────────────────────────────────────────────────────────────

async def scrappa_search(
    query: str, scrappa: "ScrappaClient"
) -> Tuple[List[Dict], int]:
    t0 = time.monotonic()
    try:
        places = await scrappa.search_google_maps(query=query, limit=MAX_RESULTS_PER_QUERY)
        ms     = int((time.monotonic() - t0) * 1000)
        return places, ms
    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        logger.warning(f"  Scrappa.co error '{query}': {e}")
        return [], ms


def parse_scrappa_place(place: Dict) -> Dict:
    """Normalise a Scrappa.co Google Maps result."""
    return {
        "nom":         (place.get("nom") or place.get("name") or place.get("title") or "").strip(),
        "adresse":     place.get("adresse") or place.get("address") or place.get("formatted_address") or "",
        "telephone":   place.get("telephone") or place.get("phone") or place.get("phone_number") or "",
        "rating":      place.get("rating"),
        "nb_avis":     place.get("nb_avis") or place.get("reviews_count") or place.get("user_ratings_total") or 0,
        "url":         place.get("domain") or place.get("website") or place.get("url") or "",
        "description": place.get("description") or place.get("snippet") or "",
    }


# ─────────────────────────────────────────────────────────────
# SERPAPI — Google Maps search via httpx
# ─────────────────────────────────────────────────────────────

async def serpapi_search(
    query: str, client: httpx.AsyncClient
) -> Tuple[List[Dict], int]:
    t0 = time.monotonic()
    try:
        resp = await client.get(
            "https://serpapi.com/search.json",
            params={
                "engine":  "google_maps",
                "q":       query,
                "type":    "search",
                "api_key": SERPAPI_KEY,
            },
            timeout=REQUEST_TIMEOUT,
        )
        ms = int((time.monotonic() - t0) * 1000)
        if resp.status_code != 200:
            logger.warning(f"  SerpAPI HTTP {resp.status_code} for '{query}'")
            return [], ms
        data   = resp.json()
        places = data.get("local_results") or data.get("places") or data.get("results") or []
        return places, ms
    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        logger.warning(f"  SerpAPI error '{query}': {e}")
        return [], ms


def parse_serpapi_place(place: Dict) -> Dict:
    """Normalise a SerpAPI Google Maps result."""
    return {
        "nom":         (place.get("title") or place.get("name") or "").strip(),
        "adresse":     place.get("address") or "",
        "telephone":   place.get("phone") or "",
        "rating":      place.get("rating"),
        "nb_avis":     place.get("reviews") or 0,
        "url":         place.get("website") or "",
        "description": place.get("description") or place.get("snippet") or "",
    }


# ─────────────────────────────────────────────────────────────
# HOMEPAGE SCORER (identical to FournisseurScraper)
# ─────────────────────────────────────────────────────────────

async def fetch_and_score(
    domain: str, client: httpx.AsyncClient
) -> int:
    for scheme in ("https://", "http://"):
        try:
            resp = await client.get(
                f"{scheme}{domain}",
                timeout=12,
                headers=HEADERS,
                follow_redirects=True,
            )
            if resp.status_code >= 400:
                continue
            ct = resp.headers.get("content-type", "")
            if "text/html" not in ct and "text/plain" not in ct:
                continue
            html = resp.text[:80000]
            if len(html) >= 300:
                return score_from_html(html)
        except Exception:
            continue
    return -1   # unreachable


# ─────────────────────────────────────────────────────────────
# PROCESS PLACES — filter + score + build ORM record
# ─────────────────────────────────────────────────────────────

async def process_places(
    places: List[Dict],
    parse_fn,
    api_source: str,
    query: str,
    country: str,
    fetch_time_ms: int,
    seen_domains: set,
    http_client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> List[SupplierDiscoveryComparison]:

    raw_count = len(places)
    records   = []

    for place in places:
        parsed = parse_fn(place)
        nom    = parsed["nom"]

        if not nom or len(nom) < 3:
            continue
        if any(kw in nom.lower() for kw in NOT_SUPPLIER):
            continue

        # HARD RULE 1 — must have website
        domain = extract_domain(parsed["url"])
        if not has_website(domain):
            logger.debug(f"  ✗ No website: {nom}")
            continue
        if any(skip in domain for skip in SKIP_DOMAINS):
            continue
        if domain in seen_domains:
            continue
        if not is_travel_supplier(nom, domain):
            continue

        # Detect connexion from metadata (name + description + domain)
        has_api, api_name, has_channel, channel_name, tc = detect_connexion(
            nom, parsed["description"], domain
        )

        # Score homepage to confirm connexion signals
        async with semaphore:
            score = await fetch_and_score(domain, http_client)

        # Re-detect from homepage HTML if reachable
        # (score_from_html already uses CM_SIGNALS — use it to update tc)
        if score > 0:
            # Re-run detection on domain string (homepage already scored)
            # If score >= 50 it means a CM signal was found in HTML
            if score >= 50 and not has_channel:
                has_channel = True
                tc = "BOTH" if has_api else "CHANNEL"

        # HARD RULE 2 — must have API or Channel Manager or BOTH
        if not is_connected(has_api, has_channel):
            logger.debug(f"  ✗ No API/CM: {nom} ({domain})")
            continue

        seen_domains.add(domain)

        records.append(SupplierDiscoveryComparison(
            nom              = nom[:255],
            ville            = None,
            adresse          = (parsed["adresse"] or "")[:500],
            telephone        = (parsed["telephone"] or "")[:50],
            domain           = domain[:500],
            rating           = float(parsed["rating"]) if parsed["rating"] else None,
            nb_avis          = int(parsed["nb_avis"]) if parsed["nb_avis"] else 0,
            score            = max(score, 0),
            status           = "qualified",
            has_api          = has_api,
            api_name         = api_name,
            has_channel      = has_channel,
            channel_name     = channel_name,
            type_connexion   = tc,
            is_marketplace   = False,
            marketplace_type = None,
            api_source       = api_source,
            query_used       = query,
            country          = country,
            fetch_time_ms    = fetch_time_ms,
            raw_result_count = raw_count,
            description      = (parsed["description"] or "")[:1000],
            url              = (parsed["url"] or "")[:1000],
            created_at       = datetime.utcnow(),
        ))

    return records


# ─────────────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────────────

async def run_comparison():
    logger.info("=" * 65)
    logger.info("  Scrappa.co vs SerpAPI — Supplier Discovery Comparison")
    logger.info(f"  Countries : {TEST_COUNTRIES}")
    logger.info(f"  Rules     : website=required, connexion=API|CHANNEL|BOTH")
    logger.info("=" * 65)

    # Create DB engine + table
    engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Table 'supplier_discovery_comparison' ready")

    AsyncSession_ = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    semaphore     = asyncio.Semaphore(CONCURRENT_HOMEPAGE)

    # Init ScrappaClient with real key
    scrappa = None
    if SCRAPPA_CLIENT_AVAILABLE:
        scrappa = ScrappaClient()
        scrappa.api_key = SCRAPPA_API_KEY
        logger.info("✅ ScrappaClient initialised")
    else:
        logger.warning("⚠️  ScrappaClient not available — Scrappa.co skipped")

    stats = {
        "Scrappa.co": {"raw": 0, "saved": 0, "rejected_no_web": 0, "rejected_no_cm": 0, "ms": 0, "calls": 0},
        "SerpAPI":    {"raw": 0, "saved": 0, "rejected_no_web": 0, "rejected_no_cm": 0, "ms": 0, "calls": 0},
    }

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=REQUEST_TIMEOUT) as http:

        for country in TEST_COUNTRIES:
            queries      = build_queries(country)
            seen_domains = set()

            logger.info(f"\n📍 {country} — {len(queries)} queries")

            for query in queries:
                logger.debug(f"  🔍 {query}")

                # ── Scrappa.co ───────────────────────────────
                if scrappa:
                    scrappa_places, scrappa_ms = await scrappa_search(query, scrappa)
                    stats["Scrappa.co"]["raw"]   += len(scrappa_places)
                    stats["Scrappa.co"]["ms"]    += scrappa_ms
                    stats["Scrappa.co"]["calls"] += 1

                    scrappa_recs = await process_places(
                        places        = scrappa_places,
                        parse_fn      = parse_scrappa_place,
                        api_source    = "Scrappa.co",
                        query         = query,
                        country       = country,
                        fetch_time_ms = scrappa_ms,
                        seen_domains  = seen_domains,
                        http_client   = http,
                        semaphore     = semaphore,
                    )
                    stats["Scrappa.co"]["saved"] += len(scrappa_recs)
                else:
                    scrappa_recs = []

                # ── SerpAPI ──────────────────────────────────
                serpapi_places, serpapi_ms = await serpapi_search(query, http)
                stats["SerpAPI"]["raw"]   += len(serpapi_places)
                stats["SerpAPI"]["ms"]    += serpapi_ms
                stats["SerpAPI"]["calls"] += 1

                serpapi_recs = await process_places(
                    places        = serpapi_places,
                    parse_fn      = parse_serpapi_place,
                    api_source    = "SerpAPI",
                    query         = query,
                    country       = country,
                    fetch_time_ms = serpapi_ms,
                    seen_domains  = seen_domains,
                    http_client   = http,
                    semaphore     = semaphore,
                )
                stats["SerpAPI"]["saved"] += len(serpapi_recs)

                # ── Save to DB ───────────────────────────────
                all_recs = scrappa_recs + serpapi_recs
                if all_recs:
                    async with AsyncSession_() as session:
                        session.add_all(all_recs)
                        await session.commit()
                    logger.info(
                        f"  💾 Saved {len(scrappa_recs)} Scrappa + "
                        f"{len(serpapi_recs)} SerpAPI | query: '{query}'"
                    )

                await asyncio.sleep(0.5)

    # ── Final report ─────────────────────────────────────────
    logger.info("\n" + "=" * 65)
    logger.info("  RESULTS")
    logger.info("=" * 65)
    for name, s in stats.items():
        avg_ms = round(s["ms"] / s["calls"], 1) if s["calls"] > 0 else 0
        logger.info(
            f"\n  {name}\n"
            f"    API calls    : {s['calls']}\n"
            f"    Raw results  : {s['raw']}\n"
            f"    Saved (API/CM+web) : {s['saved']}\n"
            f"    Avg fetch ms : {avg_ms}"
        )

    logger.info("\n  All results → table: supplier_discovery_comparison")
    logger.info("=" * 65)

    if scrappa:
        await scrappa.close()
    await engine.dispose()


# ─────────────────────────────────────────────────────────────
# SQL — run after the experiment to compare results
# ─────────────────────────────────────────────────────────────

RESULTS_SQL = """
-- Summary per tool
SELECT
    api_source,
    COUNT(*)                                          AS total_saved,
    COUNT(*) FILTER (WHERE type_connexion = 'API')   AS api_only,
    COUNT(*) FILTER (WHERE type_connexion = 'CHANNEL') AS channel_only,
    COUNT(*) FILTER (WHERE type_connexion = 'BOTH')  AS both,
    ROUND(AVG(score)::numeric, 1)                    AS avg_homepage_score,
    ROUND(AVG(fetch_time_ms))                        AS avg_fetch_ms,
    COUNT(DISTINCT country)                          AS countries_covered
FROM supplier_discovery_comparison
GROUP BY api_source
ORDER BY api_source;

-- Per country
SELECT
    country,
    api_source,
    COUNT(*)                                          AS total,
    COUNT(*) FILTER (WHERE type_connexion != 'NONE') AS qualified,
    ROUND(AVG(score)::numeric, 1)                    AS avg_score
FROM supplier_discovery_comparison
GROUP BY country, api_source
ORDER BY country, api_source;
"""

if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | {level} | {message}")
    logger.add("scrappa_vs_serpapi.log", level="DEBUG", rotation="10 MB")

    print("\n" + "=" * 65)
    print("  SQL to query results after run:")
    print("=" * 65)
    print(RESULTS_SQL)

    asyncio.run(run_comparison())