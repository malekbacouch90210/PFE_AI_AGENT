"""

FIXES FROM produit_still_wrong__gueesing.csv:

PROBLEM 1 — ville_raw = garbage (Day×64, Tours×42, Island×7, Coffee, Cafe, Valley, Trip...)
  FIX: is_garbage_city() blocklist of 80+ non-city words
  These are cleared BEFORE any processing

PROBLEM 2 — pays_raw="United States" for 331 rows (Korean/Italian/Norwegian products)
  FIX: NER reads title+description first → OVERRIDES wrong pays_raw
  "DMZ Tours from Seoul" → NER extracts Seoul + South Korea → overrides "France"

PROBLEM 3 — ville_raw = country name (Morocco×20, Italy×9, Norway×11)
  FIX: is_garbage_city() blocks country names stored as cities

PROBLEM 4 — ville_raw = attraction (BELvue museum, Colosseum, Córdoba Mosque)
  FIX: ATTRACTION_IN_NAME check in is_garbage_city()

PROBLEM 5 — Price cap 2000€ too aggressive (8-day Morocco tour = 2295€ is valid)
  FIX: PRICE_MAX raised to 5000€, PRICE_ABSURD at 6000€ only

PROBLEM 6 — ON CONFLICT crash (UNIQUE constraint missing from DB)
  FIX: ensure_unique_constraint() in run_normalization.py (runs before pipeline)

MODELS:
  qwen2.5:7b       = constrained city selection (OLLAMA_MODEL_CLASSIFY)
  mxbai-embed-large = 1024-dim embeddings (OLLAMA_MODEL_EMBED)
  llama-3.1-8b-instant = NER extraction (GROQ_FALLBACK_MODEL)
"""
import asyncio
import hashlib
import json
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import httpx
from loguru import logger
from sqlalchemy import text

from config import config
from database import DatabaseManager

# ─────────────────────────────────────────────────────────────
# Caches
# ─────────────────────────────────────────────────────────────

_embedding_cache: Dict[str, list] = {}
_country_cache:   Dict[str, Optional[str]] = {}

# ─────────────────────────────────────────────────────────────
# PROBLEM 1+3+4 FIX: garbage blocklists
# ─────────────────────────────────────────────────────────────

GARBAGE_CITY_WORDS = {
    "day","days","tour","tours","trip","visit","guide","guides","island","islands",
    "valley","mountain","mountains","lake","lakes","falls","waterfall","waterfalls",
    "park","parks","forest","desert","glacier","coast","bay","cape","beach","river",
    "canyon","plateau","oasis","spring","springs","gulf","strait",
    "north","south","east","west","central","upper","lower","new","old","great",
    "grand","big","little","small","royal","national","natural","classic",
    "best","top","first","last","next","back","front",
    "home","house","hotel","resort","villa","palace","castle","chateau",
    "coffee","cafe","bar","shop","store","market","mall",
    "story","tales","welcome","friendly","scenic","beautiful","amazing",
    "early","late","morning","evening","night","daily","weekly",
    "airport","driver","economy","business","class","transfer","shuttle","train",
    "see","look","view","vista","panorama","landscape",
    "time","light","day","rise","fall","sun","moon","star","sky","cloud","rain","snow",
    "plan","real","zone","dome","bell","bridge","tower","gate","arch","wall",
    "circle","loop","ring","trail","path","road","way","route","pass",
    "temple","arena","forum","basilica","chapel",
    "ball","bowl","cup","race","game","match","play",
    "talent","liberty","freedom","glory","grace",
    "gold","golden","silver","bronze","iron","steel",
    "falcon","eagle","lion","bear","wolf","fox",
    "bono","made","force","note","mark","march",
    "roman","berber","arabic","english","french","spanish","latin","greek",
    "holy","sacred","saint","san","santa","santo","sainte",
    "city","town","village","area","region","district","province","state","county",
    "national park","nature","adventure","experience","journey","holiday","vacation",
}

COUNTRY_AS_CITY = {
    "morocco","maroc","italia","italy","norway","norge","egypt","china","chine",
    "jordan","angola","bolivia","asia","europe","africa","america","australia",
    "france","spain","germany","portugal","greece","turkey","ireland","scotland",
    "wales","england","india","vietnam","thailand","indonesia","malaysia",
    "argentina","brazil","mexico","colombia","peru","chile","venezuela",
    "russia","ukraine","poland","romania","hungary","czechia","austria",
    "switzerland","belgium","netherlands","denmark","sweden","finland",
    "canada","usa","qatar","uae","israel","iran","iraq","algeria","tunisia",
}

ATTRACTION_IN_NAME = {
    "museum","musée","museo","mosque","cathedral","cathedrale","colosseum",
    "kasbah","archaeological","heritage","shrine","amphitheater",
}


def is_garbage_city(ville: str) -> bool:
    if not ville or len(ville.strip()) < 2:
        return True
    v = ville.strip().lower()
    if v in GARBAGE_CITY_WORDS:
        return True
    if v in COUNTRY_AS_CITY:
        return True
    if len(v) <= 2:
        return True
    if any(kw in v for kw in ATTRACTION_IN_NAME) and len(v.split()) > 1:
        return True
    return False


# ─────────────────────────────────────────────────────────────
# Ollama helpers
# ─────────────────────────────────────────────────────────────

async def _ollama_generate(prompt: str, model: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=config.OLLAMA_TIMEOUT) as c:
            resp = await c.post(
                f"{config.OLLAMA_URL}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "").strip()
            return re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    except Exception as e:
        logger.debug(f"  ollama error [{model}]: {e}")
        return ""


async def _ollama_embed(text_input: str) -> Optional[list]:
    try:
        async with httpx.AsyncClient(timeout=config.OLLAMA_TIMEOUT) as c:
            resp = await c.post(
                f"{config.OLLAMA_URL}/api/embeddings",
                json={"model": config.OLLAMA_MODEL_EMBED, "prompt": text_input},
            )
            resp.raise_for_status()
            vec = resp.json().get("embedding")
            if vec and len(vec) == 1024:
                return vec
    except Exception as e:
        logger.debug(f"  embed error: {e}")
    return None


# ─────────────────────────────────────────────────────────────
# Layer 3: PostgreSQL truth
# ─────────────────────────────────────────────────────────────

async def verify_country(country: str, db: DatabaseManager) -> Optional[str]:
    if not country or len(country.strip()) < 2:
        return None
    key = country.lower().strip()
    if key in _country_cache:
        return _country_cache[key]
    try:
        async with db.engine.connect() as conn:
            r = await conn.execute(
                text("SELECT nom FROM pays WHERE LOWER(nom) = LOWER(:c) LIMIT 1"),
                {"c": country.strip()},
            )
            row = r.fetchone()
            result = row[0] if row else None
            _country_cache[key] = result
            return result
    except Exception:
        return None


async def trgm_find_city(
    phrase: str, country: Optional[str], db: DatabaseManager,
    min_score: float = 0.4
) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[float]]:
    if not phrase or len(phrase) < 2:
        return None, None, None, None
    try:
        async with db.engine.connect() as conn:
            if country:
                r = await conn.execute(text("""
                    SELECT v.name, p.nom, v.latitude, v.longitude,
                           similarity(v.name, :phrase) AS score
                    FROM villes v JOIN pays p ON v.pays_id = p.id
                    WHERE v.name % :phrase AND LOWER(p.nom) = LOWER(:country)
                    ORDER BY score DESC LIMIT 1
                """), {"phrase": phrase, "country": country})
            else:
                r = await conn.execute(text("""
                    SELECT v.name, p.nom, v.latitude, v.longitude,
                           similarity(v.name, :phrase) AS score
                    FROM villes v JOIN pays p ON v.pays_id = p.id
                    WHERE v.name % :phrase
                    ORDER BY score DESC LIMIT 1
                """), {"phrase": phrase})
            row = r.fetchone()
            if row and float(row[4]) >= min_score:
                return row[0], row[1], (float(row[2]) if row[2] else None), (float(row[3]) if row[3] else None)
    except Exception as e:
        logger.debug(f"  trgm_find_city error: {e}")
    return None, None, None, None


async def get_city_coords(city: str, country: str, db: DatabaseManager) -> Tuple[Optional[float], Optional[float]]:
    try:
        async with db.engine.connect() as conn:
            r = await conn.execute(text("""
                SELECT v.latitude, v.longitude FROM villes v
                JOIN pays p ON v.pays_id = p.id
                WHERE LOWER(v.name) = LOWER(:city) AND LOWER(p.nom) = LOWER(:country)
                LIMIT 1
            """), {"city": city, "country": country})
            row = r.fetchone()
            if row and row[0] and row[1]:
                return float(row[0]), float(row[1])
    except Exception:
        pass
    return None, None


# ─────────────────────────────────────────────────────────────
# Layer 2: Groq NER
# ─────────────────────────────────────────────────────────────

_NER_SYSTEM = (
    "You are a travel geography entity extractor. "
    "Read tourism product text and extract ONLY real city names WHERE the activity takes place. "
    "Rules: "
    "1. Do NOT extract descriptive words: Day, Time, Light, Valley, Island, Tours-as-noun, North, South. "
    "2. 'Tours' = city only if clearly the French city Tours, Loire Valley. "
    "3. Keep multi-word cities: New York, Sidi Bou Said, San Francisco. "
    "4. Clean possessives: Tromsø's → Tromsø. "
    "5. country = where activity takes place, NOT where company is based. "
    "6. Return null if genuinely uncertain. "
    "Return ONLY JSON."
)

_NER_PROMPT = """Tourism product:
Title: {title}
Description: {desc}

Return: {{"primary_city": "city or null", "primary_country": "country or null", "confidence": 0.0-1.0}}

Examples:
- "DMZ Tours from Seoul" → city=Seoul, country=South Korea
- "Polar Fjord Tromsø Cruise" → city=Tromsø, country=Norway
- "Transfer Fiumicino Airport to Rome" → city=Rome, country=Italy
- "FES TO MARRAKECH DESERT TOUR" → city=Fes, country=Morocco
- "Korea Local Tours" → city=null, country=South Korea
- "Family trip in Tunisian nature" → city=Tunis, country=Tunisia
- "3-Day Tour Algeria Constantine" → city=Constantine, country=Algeria
- "Vouvray Wine Discovery from Tours" → city=Vouvray, country=France"""


async def groq_ner(title: str, desc: str) -> Tuple[Optional[str], Optional[str], float]:
    if not config.groq_configured():
        return None, None, 0.0

    api_key = config.GROQ_API_KEYS[0]
    prompt  = _NER_PROMPT.format(title=title[:200], desc=(desc or "")[:300])

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(25.0)) as c:
            resp = await c.post(
                f"{config.GROQ_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "model":           config.GROQ_FALLBACK_MODEL,
                    "messages": [
                        {"role": "system", "content": _NER_SYSTEM},
                        {"role": "user",   "content": prompt},
                    ],
                    "temperature":     0.0,
                    "max_tokens":      120,
                    "response_format": {"type": "json_object"},
                },
            )

        if resp.status_code == 429:
            await asyncio.sleep(3)
            return None, None, 0.0
        if resp.status_code != 200:
            return None, None, 0.0

        content = resp.json()["choices"][0]["message"]["content"]
        data    = json.loads(re.sub(r"```(?:json)?|```", "", content).strip())
        city    = (data.get("primary_city")    or "").strip() or None
        country = (data.get("primary_country") or "").strip() or None
        conf    = float(data.get("confidence", 0.5))

        # Sanity: city must not be garbage
        if city and is_garbage_city(city):
            city = None

        return city, country, conf

    except Exception as e:
        logger.debug(f"  groq_ner error: {e}")
        return None, None, 0.0


# ─────────────────────────────────────────────────────────────
# Geography resolution
# ─────────────────────────────────────────────────────────────

async def resolve_geography(
    raw: Dict, db: DatabaseManager
) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[float], float, List[str]]:
    title     = (raw.get("nom_produit_en") or raw.get("nom_produit") or "").strip()
    desc      = (raw.get("description_en") or raw.get("description") or "").strip()
    ville_raw = (raw.get("ville_raw") or "").strip()
    pays_raw  = (raw.get("pays_raw")  or "").strip()
    atype     = raw.get("_canonical_type") or "EXCURSION"

    city = country = lat = lng = None
    confidence = 0.0
    sources    = []

    # STEP 1: Clear garbage ville_raw
    if ville_raw and is_garbage_city(ville_raw):
        logger.debug(f"    🗑 Garbage ville cleared: '{ville_raw}'")
        ville_raw = ""

    # STEP 2: NER from title+description (reads actual content)
    if config.groq_configured() and title:
        await asyncio.sleep(1.5)
        ner_city, ner_country, ner_conf = await groq_ner(title, desc)

        if ner_city or ner_country:
            if ner_city:
                v_city, v_country, v_lat, v_lng = await trgm_find_city(ner_city, ner_country, db)
                if v_city:
                    city       = v_city
                    country    = v_country or ner_country
                    lat, lng   = v_lat, v_lng
                    confidence = ner_conf * 0.9
                    sources.append("ner_trgm")
                    if pays_raw and country and pays_raw.lower() != country.lower():
                        logger.debug(f"    ⚠️  pays_raw='{pays_raw}' overridden → '{country}'")
                        sources.append("pays_raw_overridden")

            if not city and ner_country:
                v_country = await verify_country(ner_country, db)
                if v_country:
                    country    = v_country
                    confidence = max(confidence, ner_conf * 0.6)
                    sources.append("ner_country")

    # STEP 3: ville_raw (cleaned)
    if ville_raw and not city:
        use_country = country or pays_raw
        v_city, v_country, v_lat, v_lng = await trgm_find_city(ville_raw, use_country or None, db)
        if v_city:
            city       = v_city
            country    = country or v_country
            lat        = lat or v_lat
            lng        = lng or v_lng
            confidence = max(confidence, 0.75)
            sources.append("ville_raw_trgm")

    # STEP 4: pays_raw (only if not contradicted by text)
    if not country and pays_raw:
        obviously_wrong_us = pays_raw.lower() == "united states" and any(
            kw in title.lower() for kw in [
                "korea","seoul","tromsø","norway","nami","gangneung","busan",
                "jeju","tokyo","japan","beijing","shanghai","rome","roma",
                "italy","paris","france","barcelona","madrid","spain",
                "amsterdam","netherlands","morocc","marrakech","fes","fez",
            ]
        )
        if not obviously_wrong_us:
            v_country = await verify_country(pays_raw, db)
            if v_country:
                country    = v_country
                confidence = max(confidence, 0.35)
                sources.append("pays_raw")

    # STEP 5: Get coords if city+country but no coords yet
    if city and country and not lat:
        lat, lng = await get_city_coords(city, country, db)
        if lat and lng:
            confidence = max(confidence, 0.80)

    # STEP 6: qwen fallback — constrained from DB candidates
    if country and not city:
        selected, conf = await _qwen_pick_city(title, desc, country, atype, db)
        if selected:
            city       = selected
            lat, lng   = await get_city_coords(city, country, db)
            confidence = max(confidence, conf * 0.65)
            sources.append("qwen_constrained")

    return city, country, lat, lng, round(min(confidence, 1.0), 2), sources


async def _qwen_pick_city(
    title: str, desc: str, country: str, atype: str, db: DatabaseManager
) -> Tuple[Optional[str], float]:
    try:
        async with db.engine.connect() as conn:
            r = await conn.execute(text("""
                SELECT v.name FROM villes v JOIN pays p ON v.pays_id = p.id
                WHERE LOWER(p.nom) = LOWER(:country) ORDER BY v.name LIMIT 30
            """), {"country": country})
            candidates = [row[0] for row in r.fetchall()]
    except Exception:
        return None, 0.0

    if not candidates:
        return None, 0.0

    prompt = (
        f"Product: \"{title}\"\nDescription: {(desc or '')[:200]}\nCountry: {country}\n\n"
        f"Cities in {country}: {', '.join(candidates)}\n\n"
        f"Which city is the destination? Pick ONLY from the list.\n"
        f"Reply ONLY with JSON: {{\"city\": \"name\", \"confidence\": 0.0-1.0}}"
    )
    raw = await _ollama_generate(prompt, config.OLLAMA_MODEL_CLASSIFY)
    try:
        m = re.search(r"\{[^{}]+\}", raw)
        if not m:
            return None, 0.0
        data  = json.loads(m.group())
        city  = (data.get("city") or "").strip()
        conf  = float(data.get("confidence", 0.5))
        match = next((c for c in candidates if c.lower() == city.lower()), None)
        if match:
            return match, conf
    except Exception:
        pass
    return None, 0.0


# ─────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────

PRICE_MIN_EUR  = 5.0
PRICE_MAX_EUR  = 5000.0   # PROBLEM 5 FIX: was 2000, raised to 5000
PRICE_ABSURD   = 6000.0   # Only truly absurd values → NULL


def process_price(raw: Dict) -> Tuple[Optional[float], bool]:
    prix   = raw.get("prix")
    devise = (raw.get("devise") or "EUR").upper().strip()

    if prix is not None:
        try:
            v = float(prix)
            if v > 0:
                eur = _to_eur(v, devise)
                if PRICE_MIN_EUR <= eur < PRICE_ABSURD:
                    return round(eur, 2), eur > PRICE_MAX_EUR  # flag expensive tours
                else:
                    logger.debug(f"    💶 Price {eur}€ absurd → NULL")
                    return None, True
        except (ValueError, TypeError):
            pass

    text_val = f"{raw.get('nom_produit','')} {raw.get('description','')}"
    amt, cur = _price_from_text(text_val[:500])
    if amt:
        eur = _to_eur(amt, cur)
        if PRICE_MIN_EUR <= eur < PRICE_ABSURD:
            return round(eur, 2), False

    return None, True


def _to_eur(amount: float, currency: str) -> float:
    rates = {"EUR":1.0,"USD":0.92,"GBP":1.17,"CHF":1.05,
             "NOK":0.087,"SEK":0.088,"DKK":0.134,
             "TND":0.30,"MAD":0.094,"EGP":0.019,"KRW":0.00069}
    return round(amount * rates.get(currency.upper(), 1.0), 2)


def _price_from_text(text: str) -> Tuple[Optional[float], str]:
    for pat, cur in [(r"€\s*(\d+(?:[.,]\d+)?)","EUR"),
                     (r"(\d+(?:[.,]\d+)?)\s*€","EUR"),
                     (r"\$\s*(\d+(?:[.,]\d+)?)","USD"),
                     (r"£\s*(\d+(?:[.,]\d+)?)","GBP")]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                v = float(m.group(1).replace(",","."))
                if v > 0:
                    return v, cur
            except ValueError:
                pass
    return None, "EUR"


def classify_activity(categorie_raw: Optional[str], nom: str, desc: str) -> str:
    text = f"{nom} {desc or ''} {categorie_raw or ''}".lower()
    TRANSFER_SPECIFIC = [
        "airport transfer","airport shuttle","airport taxi","navette aéroport",
        "transfer from airport","transfer to airport","private transfer",
        "transfer from fiumicino","transfer from orly","transfer from cdg",
        "shuttle service","shuttle bus","taxi service","chauffeur service",
        "door to door","pick up from airport","pickup from airport",
    ]
    if any(kw in text for kw in TRANSFER_SPECIFIC):
        return "TRANSFER"
    if any(kw in text for kw in ["ticket","entrance","admission","entry",
                                  "skip the line","skip-the-line","fast track",
                                  "e-ticket","museum ticket","park ticket"]):
        return "TICKET"
    return "EXCURSION"


def map_category(atype: str, title: str, desc: str) -> str:
    if atype == "TRANSFER":
        return "TRANSPORT"
    text = f"{title} {desc}".lower()
    RULES = [
        (["museum","palais","castle","château","archaeological","ruins","palace",
          "monument","cathedral","ancient","roman","heritage","historical",
          "colosseum","acropolis","mosque","synagogue"],                      "HISTORIQUE"),
        (["art","gallery","culture","cultural","theatre","opera","architecture",
          "craft","artisan","handicraft","pottery","ceramics"],              "CULTUREL"),
        (["boat","sailing","cruise","catamaran","snorkel","diving","kayak",
          "beach","island","whale","dolphin","ocean","sea","fjord","fishing"], "NAUTIQUE"),
        (["safari","wildlife","national park","forest","mountain","hiking",
          "trek","desert","canyon","volcano","glacier","arctic","aurora",
          "reindeer","husky","northern lights","camel","quad","4x4","horse"],  "NATURE"),
        (["food","cooking","wine","culinary","gastronomy","tasting","market",
          "farm","olive","cheese","street food","workshop","savon","soap"],    "GASTRONOMIE"),
        (["adventure","zip line","bungee","paragliding","climbing","rappel",
          "extreme","atv","buggy","skydiving"],                               "AVENTURE"),
        (["spa","wellness","massage","relax","hammam","thermal","yoga"],       "BIEN_ETRE"),
        (["family","kids","children","theme park","zoo","aquarium"],           "FAMILLE"),
    ]
    for keywords, category in RULES:
        if any(kw in text for kw in keywords):
            return category
    return "CULTUREL"


# ─────────────────────────────────────────────────────────────
# Embeddings
# ─────────────────────────────────────────────────────────────

def build_embedding_text(title: str, desc: str, city: str, country: str, atype: str, category: str) -> str:
    parts = [
        f"Activity: {title}",
        f"Location: {city or 'Unknown'}, {country or 'Unknown'}",
        f"Type: {atype}", f"Category: {category}",
    ]
    if desc:
        clean = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", desc)).strip()
        if clean:
            parts.append(clean[:300])
    return " | ".join(p for p in parts if p)


async def get_embedding(emb_text: str) -> Optional[list]:
    key = hashlib.md5(emb_text.encode()).hexdigest()
    if key in _embedding_cache:
        return _embedding_cache[key]
    vec = await _ollama_embed(emb_text)
    if vec:
        _embedding_cache[key] = vec
    return vec


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def build_sig(city: str, atype: str, title: str) -> str:
    stop  = {"the","a","an","of","in","at","to","for","and","or","le","la","de","du"}
    words = [w for w in title.lower().split() if w not in stop]
    key   = f"{(city or '').lower()}|{atype}|{' '.join(words[:3])}"
    return f"{key[:80]}_{hashlib.md5(key.encode()).hexdigest()[:16]}"


def light_clean(text: str) -> str:
    if not text: return ""
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def sanitize(s: str) -> str:
    if not s: return s
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


def parse_duration(raw) -> Optional[int]:
    if not raw: return None
    t = str(raw).lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*h", t)
    if m: return int(float(m.group(1)) * 60)
    m = re.search(r"(\d+)\s*min", t)
    if m: return int(m.group(1))
    m = re.search(r"(\d+)\s*(?:day|jour)", t)
    if m: return int(m.group(1)) * 480
    return None


def extract_tags(text: str) -> dict:
    t = text.lower()
    return {
        "skip_the_line": "skip the line" in t or "fast track" in t,
        "guided":        "guided" in t or "with guide" in t,
        "private":       "private" in t or "privé" in t,
        "group":         "group" in t or "shared" in t,
        "airport":       "airport" in t or "aéroport" in t,
        "flexible":      "free cancellation" in t or "annulable" in t,
    }


# ─────────────────────────────────────────────────────────────
# NormalizationPipeline
# ─────────────────────────────────────────────────────────────

class NormalizationPipeline:

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def run(self, limit: int = 100, run_id: Optional[str] = None) -> Dict:
        start = time.time()
        stats = dict(total=0, ok=0, failed=0, geo_ok=0, geo_none=0,
                     emb_ok=0, emb_fail=0, price_real=0, price_null=0)

        logger.info("=" * 60)
        logger.info("🧠 Phase 2  — Normalization Pipeline + Phase 3 - Embedding Pipeline")
        logger.info(f"   classify : {config.OLLAMA_MODEL_CLASSIFY}")
        logger.info(f"   embed    : {config.OLLAMA_MODEL_EMBED}")
        logger.info(f"   NER      : {config.GROQ_FALLBACK_MODEL}")
        logger.info(f"   limit    : {limit} | run_id: {run_id or 'all'}")
        logger.info("=" * 60)

        rows = await self._load_pending(limit, run_id)
        if not rows:
            logger.info("  ✅ Nothing pending")
            return stats

        logger.info(f"📦 {len(rows)} products to normalize")
        sem = asyncio.Semaphore(config.NORMALIZATION_CONCURRENCY)

        async def process(raw: Dict):
            async with sem:
                await self._normalize_one(raw, stats)

        await asyncio.gather(*[process(r) for r in rows])

        elapsed = time.time() - start
        logger.info("=" * 60)
        logger.info(f"✅ Done in {elapsed:.1f}s")
        logger.info(f"   total={stats['total']} ok={stats['ok']} failed={stats['failed']}")
        logger.info(f"   geo_ok={stats['geo_ok']} no_geo={stats['geo_none']}")
        logger.info(f"   emb={stats['emb_ok']} price_real={stats['price_real']} null={stats['price_null']}")
        logger.info("=" * 60)
        return stats

    async def _load_pending(self, limit: int, run_id: Optional[str]) -> List[Dict]:
        async with self.db.engine.connect() as conn:
            if run_id:
                r = await conn.execute(text("""
                    SELECT p.id, p.fournisseur_id, p.run_id,
                           p.nom_produit, p.description,
                           p.prix, p.devise, p.duree,
                           p.categorie_raw, p.ville_raw, p.pays_raw,
                           p.source, p.url_source, p.image_url,
                           p.nom_produit_en, p.description_en
                    FROM produits p
                    WHERE p.id NOT IN (
                        SELECT scraped_product_id FROM scraped_products_normalized
                        WHERE scraped_product_id IS NOT NULL
                    ) AND p.run_id = :run_id
                    ORDER BY p.id LIMIT :limit
                """), {"limit": limit, "run_id": run_id})
            else:
                r = await conn.execute(text("""
                    SELECT p.id, p.fournisseur_id, p.run_id,
                           p.nom_produit, p.description,
                           p.prix, p.devise, p.duree,
                           p.categorie_raw, p.ville_raw, p.pays_raw,
                           p.source, p.url_source, p.image_url,
                           p.nom_produit_en, p.description_en
                    FROM produits p
                    WHERE p.id NOT IN (
                        SELECT scraped_product_id FROM scraped_products_normalized
                        WHERE scraped_product_id IS NOT NULL
                    )
                    ORDER BY p.id LIMIT :limit
                """), {"limit": limit})
            return [dict(row._mapping) for row in r.fetchall()]

    async def _normalize_one(self, raw: Dict, stats: Dict):
        pid = raw.get("id")
        stats["total"] += 1
        try:
            title = sanitize(light_clean(
                raw.get("nom_produit_en") or raw.get("nom_produit") or ""
            ))[:500]
            desc = sanitize(light_clean(
                raw.get("description_en") or raw.get("description") or ""
            ))[:2000]

            if not title or len(title) < 5:
                stats["failed"] += 1
                return

            atype = classify_activity(raw.get("categorie_raw"), title, desc)
            raw["_canonical_type"] = atype

            city, country, lat, lng, loc_conf, loc_sources = \
                await resolve_geography(raw, self.db)

            if city or country:
                stats["geo_ok"] += 1
            else:
                stats["geo_none"] += 1

            price_eur, price_is_est = process_price(raw)
            if price_eur:
                stats["price_real"] += 1
            else:
                stats["price_null"] += 1

            category = map_category(atype, title, desc)
            duration = parse_duration(raw.get("duree"))
            sig      = build_sig(city or "", atype, title)
            tags     = extract_tags(title + " " + desc)

            origin = dest_field = route_sig = None
            if atype == "TRANSFER":
                m = re.search(r"from\s+([a-z\s]+?)\s+to\s+([a-z\s]+?)(?:\s|$)",
                              (title + " " + desc).lower())
                if m:
                    origin     = m.group(1).strip().title()
                    dest_field = m.group(2).strip().title()
                    route_sig  = f"{origin} → {dest_field}"

            emb_text  = build_embedding_text(title, desc, city or "", country or "", atype, category)
            embedding = await get_embedding(emb_text)
            if embedding:
                stats["emb_ok"] += 1
            else:
                stats["emb_fail"] += 1

            confidence = round(min(sum([
                0.20 if title                              else 0,
                0.10 if desc                               else 0,
                0.20 if (price_eur and not price_is_est)   else 0,
                0.20 if city                               else 0,
                0.10 if country                            else 0,
                0.10, loc_conf * 0.10,
            ]), 1.0), 2)

            await self._save(
                pid, raw.get("fournisseur_id"), raw.get("run_id"),
                title, desc, city, country, lat, lng,
                loc_conf, json.dumps(loc_sources), atype, category,
                price_eur, price_is_est, duration,
                origin, dest_field, route_sig,
                sig, json.dumps(tags),
                raw.get("image_url"), embedding, emb_text, confidence,
            )

            stats["ok"] += 1
            logger.info(
                f"  ✅ {pid} | {title[:40]} | {atype} | "
                f"{city or '?'},{country or '?'} | "
                f"{price_eur or 'NULL'}€"
            )

        except Exception as e:
            stats["failed"] += 1
            logger.error(f"  ❌ {pid}: {e}")

    async def _save(
        self, raw_id, fournisseur_id, run_id,
        title, desc, city, country, lat, lng,
        loc_conf, loc_sources_json, atype, category,
        price, price_est, duration,
        origin, dest, route, sig, tags_json,
        image, embedding, emb_text, confidence,
    ):
        # PROBLEM 6 FIX: ON CONFLICT uses :param style (SQLAlchemy)
        # UNIQUE constraint must exist — ensure_unique_constraint() in run_normalization.py
        params = {
            "raw_id":raw_id, "fournisseur_id":fournisseur_id, "run_id":run_id,
            "title":title, "desc":desc, "city":city, "country":country,
            "lat":lat, "lng":lng, "loc_conf":loc_conf,
            "loc_sources":loc_sources_json,
            "atype":atype, "category":category, "price":price,
            "price_est":price_est, "duration":duration,
            "origin":origin, "dest":dest, "route":route,
            "sig":sig, "tags":tags_json, "image":image,
            "embedding":embedding, "emb_text":emb_text,
            "confidence":confidence,
        }

        sql = text("""
            INSERT INTO scraped_products_normalized (
                scraped_product_id, fournisseur_id, run_id,
                clean_title, clean_description,
                normalized_city, normalized_country, geo_lat, geo_lng,
                location_confidence, location_sources,
                canonical_activity_type, normalized_category,
                estimated_price, estimated_currency, price_is_estimated,
                duration_minutes, origin, destination, route_signature,
                matching_signature, activity_tags,
                image_url, embedding, embedding_text,
                normalization_confidence, classification_confidence, created_at
            ) VALUES (
                :raw_id, :fournisseur_id, :run_id,
                :title, :desc, :city, :country, :lat, :lng,
                :loc_conf, CAST(:loc_sources AS jsonb),
                :atype, :category, :price, 'EUR', :price_est,
                :duration, :origin, :dest, :route,
                :sig, CAST(:tags AS jsonb),
                :image, :embedding, :emb_text,
                :confidence, :confidence, NOW()
            )
            ON CONFLICT (scraped_product_id) DO UPDATE SET
                clean_title=EXCLUDED.clean_title,
                clean_description=EXCLUDED.clean_description,
                normalized_city=EXCLUDED.normalized_city,
                normalized_country=EXCLUDED.normalized_country,
                geo_lat=EXCLUDED.geo_lat, geo_lng=EXCLUDED.geo_lng,
                location_confidence=EXCLUDED.location_confidence,
                location_sources=EXCLUDED.location_sources,
                canonical_activity_type=EXCLUDED.canonical_activity_type,
                normalized_category=EXCLUDED.normalized_category,
                estimated_price=EXCLUDED.estimated_price,
                price_is_estimated=EXCLUDED.price_is_estimated,
                duration_minutes=EXCLUDED.duration_minutes,
                origin=EXCLUDED.origin, destination=EXCLUDED.destination,
                route_signature=EXCLUDED.route_signature,
                matching_signature=EXCLUDED.matching_signature,
                activity_tags=EXCLUDED.activity_tags,
                embedding=EXCLUDED.embedding,
                embedding_text=EXCLUDED.embedding_text,
                normalization_confidence=EXCLUDED.normalization_confidence
        """)
        async with self.db.engine.begin() as conn:
            await conn.execute(sql, params)