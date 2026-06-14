"""

FIX — 5 suppliers instead of 20-30:
  Root cause: China search queries used generic English terms.
  Google Maps China returns local Chinese operators that don't use
  English keywords like "bokun" or "fareharbor".

  Solution: Add region-specific CM signals AND search queries:
  - China: ctrip, trip.com, tuniu, lvmama, mafengwo, tongcheng
  - Asia: klook, kkday, pelago, 12go
  - LATAM: despegar
  - Middle East: musement, tiqets

  Also: SCORE_MIN_START lowered to 0 for connected suppliers —
  if has_api or has_channel → ALWAYS include regardless of score.
  min/max is an INTERVAL — if 20 connected found, use all 20 even if < min.

RULES:
  ✅ Website required (domain != empty)
  ✅ API OR Channel Manager required (type_connexion != NONE)
  ✅ Min/max per country independently
  ✅ 50+ search queries per country
"""
import asyncio
import re
from typing import Dict, List, Optional, Tuple

import httpx
from loguru import logger

from config import config
from scrapers.apis.scrappa_client import ScrappaClient

# ─────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────
SCORE_TABLE = {
    "booking_engine":  50,
    "has_api":         35,
    "has_products":    10,
    "has_booking_cta":  5,
    "has_price":        5,
    "city_mention":     5,
}
SCORE_MIN_START = 0    # Accept ALL connected suppliers — min/max handles the limit
SCORE_MIN_FLOOR = 0

# ─────────────────────────────────────────────────────────────
# Channel Manager signals — GLOBAL including Asia, China, LATAM
# ─────────────────────────────────────────────────────────────
CM_SIGNALS: Dict[str, str] = {
    # Western CMs
    "bokun":                        "Bokun",
    "rezdy":                        "Rezdy",
    "fareharbor":                   "FareHarbor",
    "ventrata":                     "Ventrata",
    "xola":                         "Xola",
    "checkfront":                   "Checkfront",
    "trekksoft":                    "TrekkSoft",
    "regiondo":                     "Regiondo",
    "peek.com":                     "Peek",
    "zaui":                         "Zaui",
    "palisis":                      "Palisis",
    "tocaro":                       "Tocaro",
    "orioly":                       "Orioly",
    "musement":                     "Musement",
    "tiqets":                       "Tiqets",
    "headout":                      "Headout",
    "bookingkit":                   "BookingKit",
    "planyo":                       "Planyo",
    "rezgo":                        "Rezgo",
    "bookinglayer":                 "BookingLayer",
    "activiti":                     "Activiti",
    # OTA platforms
    "viator":                       "Viator",
    "getyourguide":                 "GetYourGuide",
    "klook":                        "Klook",
    "kkday":                        "KKday",
    "civitatis":                    "Civitatis",
    "withlocals":                   "WithLocals",
    "tourradar":                    "TourRadar",
    "isango":                       "Isango",
    "pelago":                       "Pelago",
    "12go":                         "12Go Asia",
    "partner.viator":               "Viator Partner",
    "getyourguide.com/partner":     "GYG Partner",
    "supplier.klook":               "Klook Supplier",
    # Asian platforms
    "ctrip":                        "Ctrip",
    "trip.com":                     "Trip.com",
    "tuniu":                        "Tuniu",
    "lvmama":                       "Lvmama",
    "mafengwo":                     "Mafengwo",
    "tongcheng":                    "Tongcheng",
    "qunar":                        "Qunar",
    "elong":                        "eLong",
    "traveloka":                    "Traveloka",
    "agoda":                        "Agoda",
    "airpaz":                       "Airpaz",
    # LATAM
    "despegar":                     "Despegar",
    "viajala":                      "Viajala",
    # Middle East / Africa
    "wetu.com":                     "Wetu",
    "tourplan":                     "TourPlan",
    "lemax":                        "Lemax",
    # Generic booking signals
    "booking-manager":              "Booking Manager",
    "channel-manager":              "Channel Manager",
    "tripadvisor.com/experiences":  "Tripadvisor Exp",
    "expedia.com/things-to-do":     "Expedia Activities",
    "airbnb.com/experiences":       "Airbnb Experiences",
}

# ─────────────────────────────────────────────────────────────
# API signals
# ─────────────────────────────────────────────────────────────
API_SIGNALS = [
    "/api/v1","/api/v2","/api/v3","/api/v4",
    "swagger","openapi","api-docs","developer.",
    "api.viator","api.getyourguide","api.klook","api.ctrip","api.trip.com",
    "api.bokun","api.rezdy","api.fareharbor",
    "xmlfeed","xml-feed","xml_feed",
    "datafeed","data-feed","data_feed",
    "channel-manager","channelmanager",
    "connectivity","integration/api",
    "webhook","webhooks",
    "partner-portal","supplier-portal",
    "extranet","partners.viator",
    "supplier.getyourguide","partner.klook",
]

PRODUCT_SIGNALS = [
    "/tours/","/tour/","/excursions/","/activities/",
    "/tickets/","/transfers/","/experiences/","/things-to-do/",
    '"@type":"Product"','"@type":"Tour"','"@type":"TouristAttraction"',
    "application/ld+json",
]

BOOKING_CTA = [
    "book now","book online","reserve now","check availability",
    "buy tickets","réserver","jetzt buchen","prenota",
    "book directly","instant booking","立即预订","在线预订",
]

PRICE_SIGNALS = [
    "from $","from €","from £","per person",'"price":','"pricecurrency":',
    "¥","cny","rmb","hkd",
]

SKIP_DOMAINS = {
    "facebook.com","instagram.com","twitter.com","x.com","linkedin.com",
    "youtube.com","pinterest.com","tiktok.com","weibo.com",
    "tripadvisor.com","yelp.com","booking.com","airbnb.com",
    "google.com","maps.google.com","wikipedia.org","expedia.com",
    "trustpilot.com","lonelyplanet.com","whatsapp.com",
}

NOT_SUPPLIER_KEYWORDS = {
    "mall","cinema","stadium","hospital","university",
    "furniture","bank","pharmacy","supermarket","school","restaurant",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

MARKETPLACE_URLS = {
    "viator":       "https://www.viator.com/searchResults/all?text={city}+{country}",
    "getyourguide": "https://www.getyourguide.com/s/?q={city}+{country}&et=ACTIVITY",
    "klook":        "https://www.klook.com/en-US/search/?query={city}",
}


# ─────────────────────────────────────────────────────────────
# Region-aware query builder — 50+ queries per country
# ─────────────────────────────────────────────────────────────

def _build_search_queries(country: str, activity_types: List[str]) -> List[str]:
    """
    Build 50+ targeted queries.
    Region-specific: China/Asia uses local platform names.
    """
    queries = []

    # ── Base ─────────────────────────────────────────────────
    queries += [
        f"tour operator {country}",
        f"excursion operator {country}",
        f"travel agency {country} online booking",
        f"activity provider {country}",
        f"tourism company {country}",
        f"tour company {country} website",
        f"travel supplier {country} booking",
        f"local tour operator {country}",
        f"tour guide company {country}",
        f"book tours online {country}",
        f"instant booking tours {country}",
        f"online reservation tours {country}",
        f"activities booking website {country}",
        f"day tours online booking {country}",
        f"experience provider {country}",
        f"tour operator with online reservations {country}",
        f"travel experiences {country}",
        f"adventure tours {country} booking",
        f"sightseeing tours {country} online",
        f"guided tours {country} booking",
        f"tour packages {country} online booking",
        f"activity booking platform {country}",
        f"excursions online reservation {country}",
    ]


    # ── Asia / China specific ────────────────────────────────
    country_lower = country.lower()
    if country_lower in {
        "china","japan","south korea","taiwan","hong kong",
        "thailand","vietnam","indonesia","malaysia","singapore",
        "philippines","india","nepal","sri lanka",
    }:
        queries += [
            f"tour operator {country} klook partner",
            f"tour company {country} kkday",
            f"excursion {country} traveloka",
            f"activities {country} agoda",
            f"tour operator {country} trip.com",
            f"travel agency {country} ctrip partner",
            f"tour operator {country} 12go",
            f"tour company {country} pelago",
        ]

    if country_lower == "china":
        queries += [
            f"旅游供应商 中国",       # Tourism supplier China
            f"导游公司 在线预订",      # Tour guide company online booking
            f"excursion {country} mafengwo",
            f"tour operator {country} tuniu",
            f"travel agency {country} lvmama",
        ]

    # ── LATAM specific ────────────────────────────────────────
    if country_lower in {
        "brazil","argentina","mexico","colombia","peru",
        "chile","ecuador","bolivia","venezuela",
    }:
        queries += [
            f"tour operator {country} despegar",
            f"excursion {country} viajala",
            f"agencia de viajes {country} online",
            f"operador turístico {country} booking",
        ]

    # ── Middle East / Africa specific ────────────────────────
    if country_lower in {
        "egypt","morocco","tunisia","kenya","south africa",
        "tanzania","jordan","uae","saudi arabia",
        "ghana","senegal","ethiopia",
    }:
        queries += [
            f"tour operator {country} musement",
            f"safari operator {country} booking system",
            f"tour company {country} wetu",
            f"excursion {country} tourplan",
            f"tour operator {country} isango",
        ]

    # ── Activity type specific ────────────────────────────────
    for atype in activity_types:
        queries += [
            f"{atype} operator {country} online",
            f"{atype} company {country} booking",
            f"{atype} provider {country} api",
            f"best {atype} {country}",
            f"{atype} {country} booking platform",
        ]

    # ── Transfer specific ─────────────────────────────────────
    if "transfer" in activity_types:
        queries += [
            f"airport transfer {country} online",
            f"private transfer {country} booking",
            f"shuttle service {country} api",
            f"airport taxi {country} booking system",
            f"private driver {country} online reservation",
        ]

    # ── Ticket specific ───────────────────────────────────────
    if "ticket" in activity_types:
        queries += [
            f"museum tickets {country} online",
            f"attraction tickets {country} booking",
            f"skip the line {country}",
            f"entrance tickets {country} online",
            f"tour tickets {country} booking platform",
        ]

    # ── Excursion specific ────────────────────────────────────
    if "excursion" in activity_types:
        queries += [
            f"day tours {country} online booking",
            f"guided excursions {country}",
            f"sightseeing tours {country} booking",
            f"cultural tours {country} online",
            f"adventure tours {country} booking",
        ]

    # Deduplicate
    seen, unique = set(), []
    for q in queries:
        if q not in seen:
            seen.add(q)
            unique.append(q)

    return unique


# ─────────────────────────────────────────────────────────────
# FournisseurScraper
# ─────────────────────────────────────────────────────────────

class FournisseurScraper:
    """

    THREE HARD REQUIREMENTS:
      1. Must have website (domain != empty)
      2. Must have API or Channel Manager
      3. Must be reachable

    MIN/MAX = interval [min, max] PER COUNTRY independently.
    If only 5 connected suppliers found → report 5 (cannot force more).
    """

    def __init__(self):
        self.scrappa     = ScrappaClient()

    async def discover(
        self,
        countries: List[str],
        activity_types: List[str],
        existing_domains: Optional[List[str]] = None,
        min_suppliers: int = 5,
        max_suppliers: int = 50,
        run_id: str = "",
        required_cities: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict]:
        existing_set    = {d.lower().strip() for d in (existing_domains or [])}
        required_cities = required_cities or {}
        all_qualified: List[Dict] = []

        logger.info("=" * 60)
        logger.info("🚀 FournisseurScraper v8 (Website + API/CM required)")
        logger.info(f"   Countries    : {countries}")
        logger.info(f"   Per-country  : [{min_suppliers}, {max_suppliers}]")
        logger.info("=" * 60)

        for country in countries:
            logger.info(f"\n📍 [{country}] interval=[{min_suppliers},{max_suppliers}]")

            raw        = await self._discover_raw_for_country(country, activity_types)
            logger.info(f"   📡 Raw: {len(raw)}")

            candidates = self._hard_filter(raw, existing_set)
            logger.info(f"   🌐 With website: {len(candidates)}")

            country_cities = {c.lower() for c in required_cities.get(country,[])}

            qualified = await self._qualify_adaptive(
                candidates      = candidates,
                required_cities = country_cities,
                min_suppliers   = min_suppliers,
                max_suppliers   = max_suppliers,
                run_id          = run_id,
                country         = country,
            )

            logger.info(f"   ✅ [{country}]: {len(qualified)} qualified")
            all_qualified.extend(qualified)

            for s in qualified:
                existing_set.add(s.get("domain","").lower())

        # Marketplace required cities
        marketplace_suppliers = await self._discover_marketplace_suppliers(
            required_cities=required_cities, run_id=run_id, existing_set=existing_set,
        )
        if marketplace_suppliers:
            logger.info(f"🏪 Marketplace: {len(marketplace_suppliers)}")
            all_qualified.extend(marketplace_suppliers)

        # Summary
        api_count  = sum(1 for s in all_qualified if s.get("has_api") and not s.get("has_channel"))
        cm_count   = sum(1 for s in all_qualified if s.get("has_channel") and not s.get("has_api"))
        both_count = sum(1 for s in all_qualified if s.get("has_api") and s.get("has_channel"))
        mkt_count  = sum(1 for s in all_qualified if s.get("is_marketplace"))

        logger.info("=" * 60)
        logger.info(f"✅ TOTAL: {len(all_qualified)} | API:{api_count} CM:{cm_count} Both:{both_count} Mkt:{mkt_count}")
        logger.info("=" * 60)
        return all_qualified

    async def _discover_raw_for_country(self, country: str, activity_types: List[str]) -> List[Dict]:
        raw, seen = [], set()
        queries   = _build_search_queries(country, activity_types)
        logger.info(f"   🔍 {len(queries)} queries for {country}")

        for query in queries:
            if query in seen: continue
            seen.add(query)
            try:
                results = await self.scrappa.search_google_maps(query=query, limit=20)
                for r in results:
                    r["target_country"] = country
                    r["search_type"]    = "maps"
                    raw.append(r)
                if results:
                    logger.debug(f"  Maps '{query}': {len(results)}")
            except Exception as e:
                logger.warning(f"  Maps error '{query}': {e}")
            await asyncio.sleep(0.3)

        return raw

    def _hard_filter(self, candidates: List[Dict], existing_set: set) -> List[Dict]:
        seen, result, no_web = set(), [], 0
        for c in candidates:
            nom    = (c.get("nom") or "").strip()
            domain = self._extract_domain_from_candidate(c)
            if not nom or len(nom) < 3: continue
            if not domain or len(domain) < 5:
                no_web += 1
                continue
            if any(skip in domain for skip in SKIP_DOMAINS): continue
            if domain in existing_set or domain in seen: continue
            if any(kw in nom.lower() for kw in NOT_SUPPLIER_KEYWORDS): continue
            seen.add(domain)
            c["domain"] = domain
            result.append(c)
        if no_web > 0:
            logger.debug(f"  🚫 {no_web} rejected: no website")
        return result

    async def _discover_marketplace_suppliers(
        self, required_cities: Dict[str,List[str]], run_id: str, existing_set: set,
    ) -> List[Dict]:
        if not required_cities: return []
        suppliers = []
        for country, cities in required_cities.items():
            for city in cities:
                for marketplace, url_template in MARKETPLACE_URLS.items():
                    url    = url_template.format(city=city.replace(" ","+"), country=country.replace(" ","+"))
                    domain = self._extract_domain(url)
                    if domain in existing_set: continue
                    logger.info(f"  🏪 {marketplace} → {city}, {country}")
                    suppliers.append({
                        "nom":f"{marketplace.title()} — {city}","domain":domain,
                        "adresse":"","telephone":"","rating":None,"nb_avis":0,"ville":city,
                        "pays_id":None,"has_api":True,"api_name":marketplace.title(),
                        "has_channel":False,"channel_name":None,"type_connexion":"MARKETPLACE",
                        "is_marketplace":True,"marketplace_type":marketplace,"score":60,
                        "status":"qualified","run_id":run_id,
                        "_detected_country":country,"_target_country":country,
                        "_marketplace_url":url,"_target_city":city,"_search_type":"marketplace",
                    })
                    existing_set.add(domain)
        return suppliers

    async def _qualify_adaptive(
        self,
        candidates: List[Dict],
        required_cities: set,
        min_suppliers: int,
        max_suppliers: int,
        run_id: str,
        country: str,
    ) -> List[Dict]:
        """
        Pre-score ALL candidates.
        Split into connected (has_api or has_channel) vs not.
        Accept ALL connected suppliers up to max_suppliers.
        Report if fewer than min found (country has limited connectable operators).
        """
        if not candidates:
            logger.warning(f"  [{country}] No candidates")
            return []

        logger.info(f"  📡 [{country}] Pre-scoring {len(candidates)} candidates...")
        scored = await self._score_all_candidates(
            candidates=candidates, required_cities=required_cities, run_id=run_id,
        )

        connected     = [s for s in scored if s.get("has_api") or s.get("has_channel")]
        not_connected = [s for s in scored if not s.get("has_api") and not s.get("has_channel")]

        logger.info(
            f"  📊 [{country}] {len(scored)} reachable | "
            f"✅ {len(connected)} API/CM | ❌ {len(not_connected)} NONE"
        )

        if not connected:
            logger.warning(
                f"  ⚠️ [{country}] 0 API/CM suppliers found. "
                f"Interval [{min_suppliers},{max_suppliers}] cannot be met. "
                f"This is expected for countries with few connected operators in Maps."
            )
            return []

        # Take up to max_suppliers from connected (sorted by score)
        qualified = connected[:max_suppliers]


        if len(qualified) < min_suppliers:
            logger.warning(
                f"  ⚠️ [{country}] Only {len(qualified)}/{min_suppliers} API/CM suppliers available. "
                f"Saving all {len(qualified)} found. "
                f"To find more: expand search queries or lower min_suppliers."
            )
        else:
            logger.info(
                f"  ✅ [{country}] {len(qualified)} suppliers in interval [{min_suppliers},{max_suppliers}]"
            )

        return qualified

    async def _score_all_candidates(
        self,
        candidates: List[Dict],
        required_cities: set,
        run_id: str,
    ) -> List[Dict]:
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_SUPPLIER_CHECKS)
        scored: List[Dict] = []

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(config.REQUEST_TIMEOUT),
            headers=HEADERS,
            follow_redirects=True,
        ) as client:

            async def score_one(c: Dict) -> Optional[Dict]:
                async with semaphore:
                    domain = c.get("domain","")
                    html = await self._fetch_homepage(domain, client)
                    if not html or len(html) < 300:
                        logger.debug(f"  ⚠️ Unreachable: {domain}")
                        return None
                    score, signals = self._score(html, required_cities)
                    if not self._is_travel_supplier(c.get("nom",""), domain, html[:1000]):
                        return None
                    detected_country, detected_city = self._extract_geo(html)
                    if not detected_country: detected_country = c.get("target_country","")
                    if not detected_city: detected_city = self._city_from_address(c.get("adresse",""))

                    has_api     = signals["has_api"]
                    has_channel = signals["channel_manager"] is not None
                    cm_name     = signals["channel_manager"]
                    if has_api and has_channel: tc = "BOTH"
                    elif has_api:               tc = "API"
                    elif has_channel:           tc = "CHANNEL"
                    else:                       tc = "NONE"
                    if has_api and has_channel:
                        api_name = "API_DIRECT"
                    elif has_api:
                        api_name = "API_DIRECT"
                    else:
                        api_name = None  # CM only — no direct API

                    icon = "✅" if tc != "NONE" else "❌"
                    logger.debug(
                        f"  {icon} {c.get('nom','')[:35]:<35} score={score:3d} {tc}"
                        + (f" [{cm_name}]" if cm_name else "")
                    )
                    return {
                        "nom":c.get("nom",""),"domain":domain,
                        "adresse":c.get("adresse",""),"telephone":c.get("telephone",""),
                        "rating":c.get("rating"),"nb_avis":c.get("nb_avis",0),
                        "ville":detected_city,"pays_id":None,
                        "has_api":has_api,"api_name":api_name,
                        "has_channel":has_channel,"channel_name":cm_name,
                        "type_connexion":tc,"is_marketplace":False,"marketplace_type":None,
                        "score":score,"status":"qualified","run_id":run_id,
                        "_detected_country":detected_country,
                        "_target_country":c.get("target_country",""),
                        "_search_type":c.get("search_type","maps"),
                    }

            results = await asyncio.gather(*[score_one(c) for c in candidates])
            scored  = [r for r in results if r is not None]

        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored

    async def _fetch_homepage(self, domain: str, client: httpx.AsyncClient) -> Optional[str]:
        for scheme in ("https://","http://"):
            try:
                resp = await client.get(f"{scheme}{domain}")
                if resp.status_code >= 400: continue
                ct = resp.headers.get("content-type","")
                if "text/html" not in ct and "text/plain" not in ct: continue
                text = resp.text[:config.HOMEPAGE_MAX_BYTES]
                if len(text) >= 300: return text
            except Exception: continue
        return None

    def _score(self, html: str, required_cities: set) -> Tuple[int, Dict]:
        h = html.lower()
        breakdown = {}
        signals = {"channel_manager":None,"has_api":False,"has_products":False,"has_booking_cta":False,"has_price":False,"breakdown":breakdown}
        score = 0
        for sig, name in CM_SIGNALS.items():
            if sig in h:
                signals["channel_manager"] = name
                score += SCORE_TABLE["booking_engine"]
                breakdown["booking_engine"] = f"+{SCORE_TABLE['booking_engine']} ({name})"
                break
        if any(s in h for s in API_SIGNALS):
            signals["has_api"] = True
            score += SCORE_TABLE["has_api"]
            breakdown["has_api"] = f"+{SCORE_TABLE['has_api']}"
        if any(s.lower() in h for s in PRODUCT_SIGNALS):
            signals["has_products"] = True
            score += SCORE_TABLE["has_products"]
        if any(s in h for s in BOOKING_CTA):
            signals["has_booking_cta"] = True
            score += SCORE_TABLE["has_booking_cta"]
        if any(s in h for s in PRICE_SIGNALS):
            signals["has_price"] = True
            score += SCORE_TABLE["has_price"]
        for city in required_cities:
            if city in h:
                score += SCORE_TABLE["city_mention"]
        breakdown["total"] = min(score, 100)
        return min(score, 100), signals

    def _is_travel_supplier(self, nom, domain, snippet) -> bool:
        combined = f"{nom} {domain} {snippet}".lower()
        return any(kw in combined for kw in [
            "tour","travel","transfer","excursion","ticket","shuttle",
            "activity","experience","trip","booking","safari","cruise",
            "museum","monument","cultural","sightseeing","adventure","guide","tourism",
        ])

    _COUNTRY_PATTERNS = [r'"addressCountry"\s*:\s*"([^"]{2,50})"',r'"country"\s*:\s*"([^"]{2,50})"']
    _CITY_PATTERNS    = [r'"addressLocality"\s*:\s*"([^"]+)"',r'"locality"\s*:\s*"([^"]+)"']

    def _extract_geo(self, html: str) -> Tuple[str, str]:
        country = city = ""
        for pat in self._COUNTRY_PATTERNS:
            m = re.search(pat, html, re.IGNORECASE)
            if m and len(m.group(1).strip()) > 2:
                country = m.group(1).strip(); break
        for pat in self._CITY_PATTERNS:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                city = m.group(1).strip()[:100]; break
        return country, city

    def _city_from_address(self, address: str) -> str:
        return address.split(",")[0].strip() if address else ""

    def _extract_domain_from_candidate(self, c: dict) -> str:
        raw = (c.get("domain") or c.get("website") or c.get("url") or
               c.get("link") or c.get("formatted_url") or c.get("site") or "")
        if not raw: return ""
        raw = re.sub(r"https?://","",raw.strip().lower())
        raw = re.sub(r"^www\.","",raw)
        return raw.split("/")[0].split("?")[0].split("#")[0].strip()

    def _extract_domain(self, url: str) -> str:
        raw = re.sub(r"https?://","",url.strip().lower())
        raw = re.sub(r"^www\.","",raw)
        return raw.split("/")[0].split("?")[0]

    async def close(self):
        await self.scrappa.close()
