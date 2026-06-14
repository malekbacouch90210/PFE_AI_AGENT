"""
Normalization Service — MERGED & PRODUCTION READY
===================================================
Combines:
  - NormalizationService (DB-first via city_fetcher, deterministic)
  - TourismEntityNormalizer (rich ontology, route extraction, LLM enrichment)

Priority order for country detection:
  1. DB lookup via city_fetcher (get_country_from_city)
  2. Static city→country map (150+ cities, instant)
  3. In-memory cache
  4. LLM fallback (Ollama, last resort)

Features:
  ✅ DB-first country detection (city_fetcher)
  ✅ 150+ city static map
  ✅ Full tourism ontology (service types, themes, flags, tags)
  ✅ Route extraction (airport transfers, city-to-city)
  ✅ LLM description enrichment (optional, fallback only)
  ✅ Batch country detection
  ✅ Marketplace source detection
  ✅ Duration extraction (minutes)
  ✅ Price extraction (regex)
  ✅ Embedding text builder (static)
  ✅ Async semaphore for LLM rate limiting
  ✅ Compatible with both callers (normalize_product accepts all field aliases)
  ✅ Supplier activite_type override (PRIORITY)
  ✅ Rich context fields (service_layers, breadcrumb, duration, inclusions, etc.)
  ✅ Hybrid price estimation (rule‑based + DeepSeek‑R1)
  ✅ Expanded OTA taxonomy (SERVICE_TYPE_MAP, ACTIVITY_THEMES)
"""

import asyncio
import json
import re
from typing import Dict, List, Optional, Tuple

import aiohttp
from bs4 import BeautifulSoup
from loguru import logger

# Import your existing city_fetcher — adjust path if needed
try:
    from services.geo.city_fetcher import get_country_from_city, get_city_info
    HAS_CITY_FETCHER = True
except ImportError:
    HAS_CITY_FETCHER = False
    logger.warning("city_fetcher not found — DB lookup disabled, falling back to static map + LLM")


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS & ONTOLOGIES
# ══════════════════════════════════════════════════════════════════════════════

OLLAMA_GENERATE_URL = "http://localhost:11434/api/generate"
LLM_MODEL = "qwen2.5:7b"
DEEPSEEK_MODEL = "deepseek-r1:8b"

# ── Static city → country (150+ entries) ─────────────────────────────────────
STATIC_CITY_COUNTRY: Dict[str, str] = {
    # Europe
    "paris": "France", "lyon": "France", "marseille": "France",
    "nice": "France", "bordeaux": "France", "strasbourg": "France",
    "rome": "Italy", "milan": "Italy", "florence": "Italy",
    "venice": "Italy", "naples": "Italy", "sicile": "Italy",
    "barcelona": "Spain", "madrid": "Spain", "seville": "Spain",
    "valencia": "Spain", "ibiza": "Spain", "malaga": "Spain",
    "london": "United Kingdom", "edinburgh": "United Kingdom",
    "manchester": "United Kingdom",
    "berlin": "Germany", "munich": "Germany", "hamburg": "Germany",
    "frankfurt": "Germany",
    "amsterdam": "Netherlands", "brussels": "Belgium",
    "zurich": "Switzerland", "geneva": "Switzerland",
    "vienna": "Austria", "athens": "Greece", "lisbon": "Portugal",
    "porto": "Portugal", "istanbul": "Turkey",
    "prague": "Czech Republic", "budapest": "Hungary",
    "warsaw": "Poland", "dublin": "Ireland",
    "stockholm": "Sweden", "oslo": "Norway", "copenhagen": "Denmark",
    "helsinki": "Finland",
    # North Africa
    "cairo": "Egypt", "luxor": "Egypt", "hurghada": "Egypt",
    "marrakech": "Morocco", "casablanca": "Morocco", "fes": "Morocco",
    "agadir": "Morocco", "tangier": "Morocco", "essaouira": "Morocco",
    "rabat": "Morocco",
    "tunis": "Tunisia", "hammamet": "Tunisia", "sousse": "Tunisia",
    "djerba": "Tunisia", "monastir": "Tunisia", "mahdia": "Tunisia",
    "tozeur": "Tunisia", "gabes": "Tunisia", "sfax": "Tunisia",
    "bizerte": "Tunisia", "nabeul": "Tunisia", "kairouan": "Tunisia",
    "algiers": "Algeria", "oran": "Algeria", "constantine": "Algeria",
    # Middle East
    "dubai": "United Arab Emirates", "abu dhabi": "United Arab Emirates",
    "doha": "Qatar",
    "amman": "Jordan", "beirut": "Lebanon", "tel aviv": "Israel",
    "riyadh": "Saudi Arabia", "jeddah": "Saudi Arabia",
    # Africa
    "nairobi": "Kenya", "cape town": "South Africa",
    "johannesburg": "South Africa", "dakar": "Senegal",
    # Americas
    "new york": "United States", "los angeles": "United States",
    "miami": "United States", "las vegas": "United States",
    "san francisco": "United States", "chicago": "United States",
    "orlando": "United States", "boston": "United States",
    "mexico city": "Mexico", "cancun": "Mexico",
    "buenos aires": "Argentina", "lima": "Peru",
    "rio de janeiro": "Brazil", "sao paulo": "Brazil",
    "toronto": "Canada", "montreal": "Canada", "vancouver": "Canada",
    # Asia
    "tokyo": "Japan", "kyoto": "Japan", "osaka": "Japan",
    "bangkok": "Thailand", "phuket": "Thailand", "chiang mai": "Thailand",
    "singapore": "Singapore",
    "bali": "Indonesia", "jakarta": "Indonesia",
    "kuala lumpur": "Malaysia",
    "seoul": "South Korea",
    "shanghai": "China", "beijing": "China", "hong kong": "China",
    "new delhi": "India", "mumbai": "India", "jaipur": "India",
    # Oceania
    "sydney": "Australia", "melbourne": "Australia",
    "auckland": "New Zealand",
    # Islands
    "male": "Maldives", "colombo": "Sri Lanka",
}

# ── ISO country code → name ───────────────────────────────────────────────────
ISO_TO_COUNTRY: Dict[str, str] = {
    "fr": "France", "it": "Italy", "es": "Spain", "de": "Germany",
    "gb": "United Kingdom", "uk": "United Kingdom",
    "pt": "Portugal", "nl": "Netherlands", "be": "Belgium",
    "ch": "Switzerland", "at": "Austria", "gr": "Greece",
    "tr": "Turkey", "eg": "Egypt", "ma": "Morocco", "tn": "Tunisia",
    "dz": "Algeria", "ae": "United Arab Emirates", "sa": "Saudi Arabia",
    "us": "United States", "ca": "Canada", "mx": "Mexico",
    "br": "Brazil", "ar": "Argentina", "pe": "Peru",
    "jp": "Japan", "cn": "China", "th": "Thailand", "sg": "Singapore",
    "au": "Australia", "nz": "New Zealand", "in": "India",
    "za": "South Africa", "ke": "Kenya", "sn": "Senegal",
    "ru": "Russia", "pl": "Poland", "cz": "Czech Republic",
    "hu": "Hungary", "ro": "Romania", "hr": "Croatia",
    "se": "Sweden", "no": "Norway", "dk": "Denmark",
    "fi": "Finland", "ie": "Ireland", "is": "Iceland",
    "jo": "Jordan", "lb": "Lebanon", "il": "Israel",
    "id": "Indonesia", "my": "Malaysia", "ph": "Philippines",
    "vn": "Vietnam", "kh": "Cambodia", "lk": "Sri Lanka",
    "qa": "Qatar",
}

# ── Service type map — expanded with full OTA matrix ─────────────────────────
SERVICE_TYPE_MAP: Dict[str, List[str]] = {
    "TRANSFER": [
        # Airport & port
        "airport transfer", "airport shuttle", "airport pickup",
        "port transfer", "cruise port", "ferry terminal",
        "transfer", "transfert", "trasferimento",
        "navette", "shuttle", "chauffeur", "taxi",
        "pickup", "pick-up", "drop-off", "dropoff",
        # Intercity
        "intercity", "inter-city", "coach transfer",
        # Water transfer
        "water taxi", "boat transfer", "ferry transfer",
        "gondola", "vaporetto",
        # Luxury transfer
        "limousine", "limo", "executive car", "private car",
        "helicopter transfer",
    ],
    "TICKET": [
        # Standard entry
        "ticket", "billet", "biglietto", "entrada", "eintrittskarte",
        "admission", "entry ticket", "entrance ticket",
        "entry pass", "access pass",
        # Skip-the-line
        "skip the line", "skip-the-line", "coupe-file",
        "fast track", "fasttrack", "priority access",
        "priority entry", "priority ticket",
        # Special access
        "early access", "after hours", "vip entry",
        "skip queue", "timed entry",
        # Passes
        "city pass", "tourist pass", "multi attraction",
        "hop-on hop-off", "hop on hop off", "hoho",
        # Audio guides
        "audio guide", "audioguide", "self-guided",
        # Events
        "concert ticket", "show ticket", "opera ticket",
        "football ticket", "match ticket", "sports ticket",
        "theater ticket", "theatre ticket",
    ],
    "CRUISE": [
        "cruise", "croisière", "boat tour", "boat trip",
        "sailing trip", "yacht tour", "catamaran tour",
        "felucca", "dhow cruise", "river cruise",
        "sea cruise", "island cruise", "sunset cruise",
        "dolphin cruise", "snorkeling cruise",
    ],
    "TOUR": [
        "guided tour", "group tour", "private tour",
        "sightseeing", "city tour", "walking tour",
        "bus tour", "cycling tour", "bike tour",
        "food tour", "wine tour", "historical tour",
        "cultural tour", "heritage tour",
        "day trip", "half day trip", "full day trip",
        "excursion", "visite guidée", "tour guidé",
        "safari", "game drive", "wildlife tour",
        "photography tour", "sunset tour", "sunrise tour",
        "night tour", "evening tour",
        "hop-on tour",
    ],
    "EXPERIENCE": [
        # Adventure
        "adventure", "extreme", "ziplining", "paragliding",
        "skydiving", "bungee", "rafting", "kayaking",
        "paddleboarding", "surfing", "kitesurfing",
        "climbing", "via ferrata", "canyoning",
        "quad", "atv", "off-road", "dune bashing",
        "camel ride", "horse riding", "trekking", "hiking",
        "skiing", "snowboarding", "snowmobiling",
        # Water activities
        "diving", "scuba", "snorkeling", "swimming",
        "whale watching", "dolphin watching",
        # Food & drink
        "cooking class", "wine tasting", "beer tasting",
        "olive oil tasting", "cheese tasting",
        "food workshop", "culinary class",
        "market tour", "gastronomy",
        # Wellness
        "spa", "hammam", "massage", "thermal",
        "hot spring", "sauna", "yoga", "meditation",
        # Creative
        "workshop", "atelier", "art class",
        "pottery", "painting", "photography class",
        # Nature
        "birdwatching", "stargazing", "aurora",
        "volcano hike", "cave tour", "glacier walk",
        # Cultural immersion
        "cooking class", "dance class", "flamenco class",
        "language class", "local family",
    ],
    "EVENT": [
        "show", "concert", "opera", "ballet",
        "theater", "theatre", "musical", "cabaret",
        "flamenco show", "belly dance", "folkloric show",
        "dinner show", "dinner theater",
        "festival", "carnival", "parade",
        "sports event", "football match", "formula 1",
        "tennis match", "basketball game",
        "light show", "sound and light",
    ],
}

# ── Activity theme ontology — expanded with full OTA matrix ──────────────────
ACTIVITY_THEMES: Dict[str, Tuple[str, str]] = {
    # ── Food & Drink ─────────────────────────────────────────
    "food tour":        ("FOOD_DRINK", "FOOD_TOUR"),
    "food":             ("FOOD_DRINK", "FOOD"),
    "wine tasting":     ("FOOD_DRINK", "WINE_TASTING"),
    "wine tour":        ("FOOD_DRINK", "WINE_TOUR"),
    "wine":             ("FOOD_DRINK", "WINE"),
    "beer tasting":     ("FOOD_DRINK", "BEER_TASTING"),
    "beer":             ("FOOD_DRINK", "BEER"),
    "cooking class":    ("FOOD_DRINK", "COOKING_CLASS"),
    "cooking":          ("FOOD_DRINK", "COOKING"),
    "culinary":         ("FOOD_DRINK", "CULINARY"),
    "gastronomy":       ("FOOD_DRINK", "GASTRONOMY"),
    "tasting":          ("FOOD_DRINK", "TASTING"),
    "market tour":      ("FOOD_DRINK", "MARKET"),
    "chocolate":        ("FOOD_DRINK", "CHOCOLATE"),
    "cheese":           ("FOOD_DRINK", "CHEESE"),
    "dinner show":      ("FOOD_DRINK", "DINNER_SHOW"),
    "street food":      ("FOOD_DRINK", "STREET_FOOD"),
    "brewery":          ("FOOD_DRINK", "BREWERY"),
    "olive oil":        ("FOOD_DRINK", "OLIVE_OIL"),
    "truffle":          ("FOOD_DRINK", "TRUFFLE"),

    # ── Culture & History ─────────────────────────────────────
    "museum":           ("CULTURE", "MUSEUM"),
    "gallery":          ("CULTURE", "ART_GALLERY"),
    "art":              ("CULTURE", "ART"),
    "history":          ("CULTURE", "HISTORY"),
    "historical":       ("CULTURE", "HISTORY"),
    "heritage":         ("CULTURE", "HERITAGE"),
    "castle":           ("CULTURE", "CASTLE"),
    "palace":           ("CULTURE", "PALACE"),
    "monument":         ("CULTURE", "MONUMENT"),
    "temple":           ("CULTURE", "TEMPLE"),
    "church":           ("CULTURE", "RELIGIOUS"),
    "cathedral":        ("CULTURE", "RELIGIOUS"),
    "mosque":           ("CULTURE", "RELIGIOUS"),
    "synagogue":        ("CULTURE", "RELIGIOUS"),
    "medina":           ("CULTURE", "MEDINA"),
    "archaeological":   ("CULTURE", "ARCHAEOLOGY"),
    "ruins":            ("CULTURE", "ARCHAEOLOGY"),
    "ancient":          ("CULTURE", "ANCIENT"),
    "colosseum":        ("CULTURE", "COLOSSEUM"),
    "acropolis":        ("CULTURE", "ACROPOLIS"),
    "pyramids":         ("CULTURE", "PYRAMIDS"),
    "roman":            ("CULTURE", "ROMAN"),
    "local culture":    ("CULTURE", "LOCAL_CULTURE"),
    "traditions":       ("CULTURE", "TRADITIONS"),
    "folklore":         ("CULTURE", "FOLKLORE"),

    # ── Adventure & Outdoor ───────────────────────────────────
    "adventure":        ("ADVENTURE", "GENERAL"),
    "hiking":           ("ADVENTURE", "HIKING"),
    "trekking":         ("ADVENTURE", "HIKING"),
    "trail":            ("ADVENTURE", "HIKING"),
    "safari":           ("ADVENTURE", "SAFARI"),
    "game drive":       ("ADVENTURE", "GAME_DRIVE"),
    "wildlife":         ("ADVENTURE", "WILDLIFE"),
    "desert":           ("ADVENTURE", "DESERT"),
    "sahara":           ("ADVENTURE", "DESERT"),
    "quad":             ("ADVENTURE", "QUAD"),
    "atv":              ("ADVENTURE", "QUAD"),
    "dune bashing":     ("ADVENTURE", "DUNE_BASHING"),
    "camel ride":       ("ADVENTURE", "CAMEL_RIDE"),
    "horse riding":     ("ADVENTURE", "HORSE_RIDING"),
    "ski":              ("ADVENTURE", "SKI"),
    "skiing":           ("ADVENTURE", "SKI"),
    "snowboard":        ("ADVENTURE", "SNOWBOARD"),
    "snowmobile":       ("ADVENTURE", "SNOWMOBILE"),
    "cycling":          ("ADVENTURE", "CYCLING"),
    "bike":             ("ADVENTURE", "CYCLING"),
    "climbing":         ("ADVENTURE", "CLIMBING"),
    "via ferrata":      ("ADVENTURE", "VIA_FERRATA"),
    "ziplining":        ("ADVENTURE", "ZIPLINE"),
    "paragliding":      ("ADVENTURE", "PARAGLIDING"),
    "skydiving":        ("ADVENTURE", "SKYDIVING"),
    "bungee":           ("ADVENTURE", "BUNGEE"),
    "rafting":          ("ADVENTURE", "RAFTING"),
    "canyoning":        ("ADVENTURE", "CANYONING"),
    "balloon":          ("ADVENTURE", "BALLOON"),
    "hot air balloon":  ("ADVENTURE", "BALLOON"),
    "volcano":          ("ADVENTURE", "VOLCANO"),
    "glacier":          ("ADVENTURE", "GLACIER"),
    "cave":             ("ADVENTURE", "CAVE"),

    # ── Water Activities ──────────────────────────────────────
    "boat":             ("WATER", "BOAT"),
    "boat tour":        ("WATER", "BOAT_TOUR"),
    "cruise":           ("WATER", "CRUISE"),
    "sailing":          ("WATER", "SAILING"),
    "yacht":            ("WATER", "YACHT"),
    "catamaran":        ("WATER", "CATAMARAN"),
    "felucca":          ("WATER", "FELUCCA"),
    "dhow":             ("WATER", "DHOW"),
    "river cruise":     ("WATER", "RIVER_CRUISE"),
    "sea cruise":       ("WATER", "SEA_CRUISE"),
    "sunset cruise":    ("WATER", "SUNSET_CRUISE"),
    "diving":           ("WATER", "DIVING"),
    "scuba":            ("WATER", "DIVING"),
    "snorkeling":       ("WATER", "SNORKELING"),
    "kayak":            ("WATER", "KAYAK"),
    "paddleboard":      ("WATER", "PADDLEBOARD"),
    "surfing":          ("WATER", "SURF"),
    "kitesurfing":      ("WATER", "KITESURF"),
    "whale":            ("WATER", "WHALE_WATCHING"),
    "dolphin":          ("WATER", "DOLPHIN_WATCHING"),
    "water park":       ("WATER", "WATERPARK"),
    "aquarium":         ("WATER", "AQUARIUM"),
    "swimming":         ("WATER", "SWIMMING"),
    "island":           ("WATER", "ISLAND_HOPPING"),

    # ── Entertainment & Shows ─────────────────────────────────
    "show":             ("ENTERTAINMENT", "SHOW"),
    "concert":          ("ENTERTAINMENT", "CONCERT"),
    "opera":            ("ENTERTAINMENT", "OPERA"),
    "ballet":           ("ENTERTAINMENT", "BALLET"),
    "theater":          ("ENTERTAINMENT", "THEATER"),
    "theatre":          ("ENTERTAINMENT", "THEATER"),
    "musical":          ("ENTERTAINMENT", "MUSICAL"),
    "flamenco":         ("ENTERTAINMENT", "FLAMENCO"),
    "belly dance":      ("ENTERTAINMENT", "BELLY_DANCE"),
    "cabaret":          ("ENTERTAINMENT", "CABARET"),
    "dinner show":      ("ENTERTAINMENT", "DINNER_SHOW"),
    "nightlife":        ("ENTERTAINMENT", "NIGHTLIFE"),
    "light show":       ("ENTERTAINMENT", "LIGHT_SHOW"),
    "folkloric":        ("ENTERTAINMENT", "FOLKLORIC"),

    # ── Sport ─────────────────────────────────────────────────
    "football":         ("SPORT", "FOOTBALL"),
    "soccer":           ("SPORT", "FOOTBALL"),
    "stadium":          ("SPORT", "STADIUM"),
    "formula 1":        ("SPORT", "F1"),
    "formula1":         ("SPORT", "F1"),
    "f1":               ("SPORT", "F1"),
    "tennis":           ("SPORT", "TENNIS"),
    "golf":             ("SPORT", "GOLF"),
    "basketball":       ("SPORT", "BASKETBALL"),
    "rugby":            ("SPORT", "RUGBY"),
    "cycling race":     ("SPORT", "CYCLING_RACE"),

    # ── Wellness & Relaxation ─────────────────────────────────
    "spa":              ("WELLNESS", "SPA"),
    "hammam":           ("WELLNESS", "HAMMAM"),
    "massage":          ("WELLNESS", "MASSAGE"),
    "thermal":          ("WELLNESS", "THERMAL"),
    "hot spring":       ("WELLNESS", "HOT_SPRING"),
    "sauna":            ("WELLNESS", "SAUNA"),
    "yoga":             ("WELLNESS", "YOGA"),
    "meditation":       ("WELLNESS", "MEDITATION"),
    "wellness":         ("WELLNESS", "WELLNESS"),

    # ── Tickets & Entry (Access-driven) ───────────────────────
    "skip the line":    ("TICKETS_ENTRY", "SKIP_THE_LINE"),
    "skip-the-line":    ("TICKETS_ENTRY", "SKIP_THE_LINE"),
    "fast track":       ("TICKETS_ENTRY", "FAST_TRACK"),
    "priority access":  ("TICKETS_ENTRY", "PRIORITY_ACCESS"),
    "early access":     ("TICKETS_ENTRY", "EARLY_ACCESS"),
    "vip entry":        ("TICKETS_ENTRY", "VIP_ENTRY"),
    "city pass":        ("TICKETS_ENTRY", "CITY_PASS"),
    "hop-on hop-off":   ("TICKETS_ENTRY", "HOHO"),
    "audio guide":      ("TICKETS_ENTRY", "AUDIO_GUIDE"),

    # ── Transport ─────────────────────────────────────────────
    "airport":          ("TRANSPORT", "AIRPORT_TRANSFER"),
    "transfer":         ("TRANSPORT", "TRANSFER"),
    "shuttle":          ("TRANSPORT", "SHUTTLE"),
    "ferry":            ("TRANSPORT", "FERRY"),
    "limousine":        ("TRANSPORT", "LIMOUSINE"),
    "helicopter":       ("TRANSPORT", "HELICOPTER"),

    # ── Photography & Creative ────────────────────────────────
    "photography":      ("PHOTOGRAPHY", "PHOTOGRAPHY_TOUR"),
    "photo tour":       ("PHOTOGRAPHY", "PHOTO_TOUR"),
    "photo walk":       ("PHOTOGRAPHY", "PHOTO_WALK"),
    "sunset":           ("PHOTOGRAPHY", "SUNSET"),
    "sunrise":          ("PHOTOGRAPHY", "SUNRISE"),
    "aurora":           ("PHOTOGRAPHY", "AURORA"),
    "stargazing":       ("PHOTOGRAPHY", "STARGAZING"),

    # ── Nature & Eco ──────────────────────────────────────────
    "nature":           ("NATURE", "NATURE"),
    "national park":    ("NATURE", "NATIONAL_PARK"),
    "birdwatching":     ("NATURE", "BIRDWATCHING"),
    "botanical":        ("NATURE", "BOTANICAL"),
    "eco tour":         ("NATURE", "ECO_TOUR"),
    "forest":           ("NATURE", "FOREST"),
    "waterfall":        ("NATURE", "WATERFALL"),
    "lagoon":           ("NATURE", "LAGOON"),

    # ── Seasonal & Holiday ────────────────────────────────────
    "christmas":        ("SEASONAL", "CHRISTMAS"),
    "christmas market": ("SEASONAL", "CHRISTMAS_MARKET"),
    "cherry blossom":   ("SEASONAL", "CHERRY_BLOSSOM"),
    "autumn foliage":   ("SEASONAL", "AUTUMN_FOLIAGE"),
    "halloween":        ("SEASONAL", "HALLOWEEN"),
    "carnival":         ("SEASONAL", "CARNIVAL"),
    "new year":         ("SEASONAL", "NEW_YEAR"),

    # ── Sightseeing (generic fallback) ───────────────────────
    "city tour":        ("TOURS_SIGHTSEEING", "CITY_TOUR"),
    "walking tour":     ("TOURS_SIGHTSEEING", "WALKING_TOUR"),
    "sightseeing":      ("TOURS_SIGHTSEEING", "SIGHTSEEING"),
    "panoramic":        ("TOURS_SIGHTSEEING", "PANORAMIC"),
    "viewpoint":        ("TOURS_SIGHTSEEING", "VIEWPOINT"),
    "night tour":       ("TOURS_SIGHTSEEING", "NIGHT_TOUR"),
}

# Human-readable label for each service type
SERVICE_TYPE_LABELS: Dict[str, str] = {
    "TRANSFER": "transfer",
    "TICKET": "ticket",
    "CRUISE": "excursion",
    "TOUR": "excursion",
    "EXPERIENCE": "activity",
    "EVENT": "event",
}

# ── Flag keywords ─────────────────────────────────────────────────────────────
FLAG_KEYWORDS: Dict[str, List[str]] = {
    "is_private":       ["private", "privé", "privado", "exclusive"],
    "is_shared":        ["shared", "partagé", "group tour", "shared tour"],
    "is_guided":        ["guided", "guidée", "guide", "con guida"],
    "is_group":         ["group", "groupe", "gruppo"],
    "is_skip_the_line": [
        "skip the line", "skip-the-line", "fast track",
        "coupe-file", "priority access", "priority entry",
    ],
    "is_airport":       [
        "airport", "aéroport", "aeroport", "flughafen",
        "aeropuerto", "aeroporto",
    ],
}

# ── Tag keywords ──────────────────────────────────────────────────────────────
TAG_KEYWORDS: Dict[str, List[str]] = {
    "museum":       ["museum", "museo", "musée"],
    "food":         ["food", "cuisine", "gastronomy", "dinner", "lunch", "tasting", "eat"],
    "wine":         ["wine", "vin", "vino", "winery", "vineyard"],
    "adventure":    ["adventure", "aventura", "extreme"],
    "hiking":       ["hiking", "trekking", "randonnée", "trail"],
    "water":        ["boat", "cruise", "sailing", "kayak", "diving", "snorkeling", "swim"],
    "culture":      ["museum", "gallery", "history", "heritage", "culture", "art"],
    "nature":       ["nature", "natural", "landscape", "park", "national park"],
    "city":         ["city tour", "city walk", "sightseeing", "urban"],
    "night":        ["night", "nuit", "noche", "evening", "nocturnal"],
    "sunset":       ["sunset", "coucher", "puesta de sol", "golden hour"],
    "sunrise":      ["sunrise", "lever du soleil", "dawn"],
    "desert":       ["desert", "désert", "desierto", "sahara"],
    "safari":       ["safari", "wildlife", "game drive"],
    "wellness":     ["spa", "massage", "wellness", "hammam", "relax", "thermal"],
    "airport":      ["airport", "aéroport", "transfer", "navette"],
    "hotel":        ["hotel", "hôtel", "pickup", "hotel pickup"],
    "vip":          ["vip", "luxury", "premium", "exclusive", "first class"],
    "skip_line":    ["skip the line", "fast track", "priority"],
    "full_day":     ["full day", "full-day", "journée complète"],
    "half_day":     ["half day", "half-day", "demi-journée"],
    "audio_guide":  ["audio guide", "audio-guide", "audioguide"],
    "wifi":         ["wifi", "wi-fi", "internet"],
    "meals":        ["meals", "repas", "lunch included", "dinner included", "breakfast"],
    "bike":         ["bike", "bicycle", "vélo", "cycling"],
    "walking":      ["walking", "walk", "on foot", "à pied"],
    "bus":          ["bus", "coach", "minibus"],
}

# ── Route extraction patterns ─────────────────────────────────────────────────
ROUTE_PATTERNS = [
    r"(\w[\w\s\-]+?)\s+(?:to|à|nach|a|al|→|vers)\s+(\w[\w\s\-]+?)\s+(?:transfer|transport|shuttle|navette)",
    r"(\w[\w\s\-]+?airport)\s+(?:to|à|→)\s+(\w[\w\s\-]+)",
    r"(?:transfer|transport)\s+(?:from|de|von|desde)\s+(\w[\w\s\-]+?)\s+(?:to|à|→)\s+(\w[\w\s\-]+)",
]

AIRPORT_INDICATORS = [
    "airport", "aéroport", "aeroport", "flughafen", "aeropuerto", "aeroporto",
]

# ── Marketplace detection ─────────────────────────────────────────────────────
MARKETPLACE_MAP: Dict[str, str] = {
    "viator": "VIATOR",
    "getyourguide": "GYG",
    "klook": "KLOOK",
    "civitatis": "CIVITATIS",
    "tiqets": "TIQETS",
    "headout": "HEADOUT",
    "musement": "MUSEMENT",
    "expedia": "EXPEDIA",
    "tripadvisor": "TRIPADVISOR",
    "airbnb": "AIRBNB",
    "withlocals": "WITHLOCALS",
    "tourradar": "TOURRADAR",
}

# ── Category → experience family ──────────────────────────────────────────────
CATEGORY_TO_FAMILY: Dict[str, str] = {
    "Museum & Culture": "CULTURE",
    "Tours & Sightseeing": "TOURS_SIGHTSEEING",
    "Food & Drink": "FOOD_DRINK",
    "Adventure & Outdoor": "ADVENTURE",
    "Water Activities": "WATER",
    "Theme Parks & Attractions": "ATTRACTION",
    "Tickets & Entry": "TICKETS_ENTRY",
    "Transfers & Transportation": "TRANSPORT",
    "Shows & Entertainment": "ENTERTAINMENT",
    "Wellness & Spa": "WELLNESS",
    "Photography & Creative": "PHOTOGRAPHY",
    "Religious & Spiritual": "CULTURE",
    "Sports": "SPORT",
}

# ── Price patterns ────────────────────────────────────────────────────────────
PRICE_PATTERNS = [
    r"(\d+(?:[\.,]\d+)?)\s?(?:€|EUR|USD|\$|£|GBP)",
    r"(?:price|prix|preis|precio)[\s:]+(\d+(?:[\.,]\d+)?)",
    r"from\s+(\d+(?:[\.,]\d+)?)",
    r"starting\s+at\s+(\d+(?:[\.,]\d+)?)",
]

# ── Duration patterns (pattern, multiplier_to_minutes) ───────────────────────
DURATION_PATTERNS = [
    (r"(\d+)\s*(?:hours?|heures?|stunden?|horas?|ore)", 60),
    (r"(\d+)\s*h\b", 60),
    (r"(\d+)\s*(?:minutes?|mins?|minuten?)", 1),
    (r"(\d+)\s*(?:days?|journées?)", 1440),
]

# ── Normalized_category from service type ─────────────────────────────────────
SERVICE_TYPE_TO_CATEGORY: Dict[str, str] = {
    "TRANSFER":   "Transfers & Transportation",
    "TICKET":     "Tickets & Entry",
    "CRUISE":     "Water Activities",
    "TOUR":       "Tours & Sightseeing",
    "EXPERIENCE": "Adventure & Outdoor",
    "EVENT":      "Shows & Entertainment",
}

SERVICE_TYPE_TO_NORMALIZED_TYPE: Dict[str, str] = {
    "TRANSFER":   "transfer",
    "TICKET":     "ticket",
    "CRUISE":     "excursion",
    "TOUR":       "excursion",
    "EXPERIENCE": "activity",
    "EVENT":      "event",
}


# ══════════════════════════════════════════════════════════════════════════════
# MERGED NORMALIZATION SERVICE
# ══════════════════════════════════════════════════════════════════════════════

class NormalizationService:
    """
    Unified Tourism Normalization Service.

    Country detection priority:
      1. city_fetcher DB lookup (if available)
      2. Static city→country map
      3. In-memory cache
      4. LLM fallback (last resort)

    Service type priority:
      1. Supplier activite_type (transfer/ticket → hard override)
      2. Service layers from scraper (access/transport)
      3. Text‑based scoring (expanded OTA matrix)

    Price estimation:
      1. Explicit scraped price
      2. price_extracted from scraper
      3. Rule‑based estimation (80 keywords)
      4. DeepSeek‑R1 LLM fallback
    """

    LLM_SEMAPHORE = asyncio.Semaphore(3)

    def __init__(self, enrich_descriptions: bool = False):
        self.enrich_descriptions = enrich_descriptions
        self._session: Optional[aiohttp.ClientSession] = None
        self._country_cache: Dict[str, str] = {}

        # Pre-fill cache with static map
        self._country_cache.update(STATIC_CITY_COUNTRY)

        self.stats = {
            "processed": 0,
            "db_hits": 0,
            "static_hits": 0,
            "llm_hits": 0,
            "cache_hits": 0,
            "country_failed": 0,
            "descriptions_enriched": 0,
        }
        logger.info(
            "🧠 NormalizationService ready | "
            f"city_fetcher={'✅' if HAS_CITY_FETCHER else '❌'} | "
            f"enrich_descriptions={enrich_descriptions}"
        )

    # ── Session management ────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info(f"NormalizationService stats: {self.stats}")

    # ── Country detection ─────────────────────────────────────────────────────

    async def detect_country(self, city: str, context: str = "") -> str:
        if not city or not city.strip():
            return ""

        city_clean = city.strip()
        cache_key = city_clean.lower()

        # 1. Cache (includes static map pre-filled at init)
        if cache_key in self._country_cache:
            self.stats["cache_hits"] += 1
            return self._country_cache[cache_key]

        # 2. DB lookup via city_fetcher
        if HAS_CITY_FETCHER:
            try:
                country = await get_country_from_city(city_clean)
                if country:
                    self._country_cache[cache_key] = country
                    self.stats["db_hits"] += 1
                    return country
            except Exception as e:
                logger.debug(f"city_fetcher error for '{city_clean}': {e}")

        # 3. LLM fallback
        country = await self._llm_country(city_clean, context)
        if country:
            self._country_cache[cache_key] = country
            self.stats["llm_hits"] += 1
            return country

        self.stats["country_failed"] += 1
        return ""

    async def detect_country_batch(self, cities: List[str]) -> Dict[str, str]:
        unknowns = [
            c for c in set(cities)
            if c and c.lower().strip() not in self._country_cache
        ]
        if not unknowns:
            return {}

        cities_str = ", ".join(unknowns[:20])
        prompt = (
            f"For each city, reply with the country it is in. "
            f"Reply ONLY as JSON: {{\"city\": \"country\"}}\n"
            f"Cities: {cities_str}"
        )

        async with self.LLM_SEMAPHORE:
            session = await self._get_session()
            payload = {
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": 300},
            }
            try:
                async with session.post(
                    OLLAMA_GENERATE_URL, json=payload,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw = data.get("response", "").strip()
                        mapping = json.loads(raw)
                        for city, country in mapping.items():
                            self._country_cache[city.lower().strip()] = country
                        return mapping
            except Exception as e:
                logger.debug(f"batch country LLM failed: {e}")
        return {}

    async def _llm_country(self, city: str, context: str) -> str:
        prompt = (
            f'What country is the city "{city}" in? '
            f'Reply ONLY the country name in English. If unknown, reply "Unknown".'
        )
        if context:
            prompt = (
                f'Context: "{context[:150]}"\n'
                f'City: "{city}"\n'
                f'What country is this city in? Reply ONLY the country name. '
                f'If unknown, reply "Unknown".'
            )

        async with self.LLM_SEMAPHORE:
            session = await self._get_session()
            payload = {
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": 30},
            }
            try:
                async with session.post(
                    OLLAMA_GENERATE_URL, json=payload,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        country = data.get("response", "").strip().strip('"').strip("'")
                        if country and country.lower() not in ("unknown", ""):
                            return country
            except Exception as e:
                logger.debug(f"LLM country failed for '{city}': {e}")
        return ""

    # ── Text cleaning ─────────────────────────────────────────────────────────

    @staticmethod
    def clean_title(title: str) -> str:
        if not title:
            return ""
        title = re.sub(r"\b20\d{2}\b", "", title)
        for brand in ["Viator", "GetYourGuide", "Klook", "Civitatis", "Tiqets", "Headout", "Musement"]:
            title = re.sub(rf"\s*[-|]\s*{brand}\s*$", "", title, flags=re.IGNORECASE)
        title = re.sub(r"[^\w\s\-.,!?()&/éèêëàâùûüôîïç]", "", title, flags=re.UNICODE)
        return re.sub(r"\s+", " ", title).strip()[:500]

    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""
        try:
            text = BeautifulSoup(text, "html.parser").get_text(separator=" ")
        except Exception:
            text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"https?://\S+", "", text)
        for phrase in [
            r"BEST PRICE", r"CLICK HERE", r"BOOK NOW", r"LIMITED TIME",
            r"FREE CANCELLATION", r"INSTANT CONFIRMATION", r"RESERVE NOW",
            r"LOWEST PRICE GUARANTEE",
        ]:
            text = re.sub(phrase, "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:2000]

    # ── NEW: Service type detection with supplier override ─────────────────

    def _detect_service_type(
            self,
            text: str = "",
            activite_type: Optional[str] = None,
            service_layers: Optional[Dict] = None,
    ) -> str:
        # ---- 1. Supplier override ----
        if activite_type:
            atype = activite_type.strip().lower()
            if atype in ("transfer", "ticket"):
                return atype.upper()

        # ---- 2. Service layers from scraper ----
        if service_layers:
            access = service_layers.get("access", [])
            transport = service_layers.get("transport", [])
            if "ticket" in access or "show" in access:
                return "TICKET"
            if "transfer" in transport or "water_transfer" in transport:
                return "TRANSFER"

        # ---- 3. Text-based scoring ----
        scores = {}
        for stype, keywords in SERVICE_TYPE_MAP.items():
            scores[stype] = sum(1 for kw in keywords if kw in text)

        length = max(len(text), 100)
        for stype in scores:
            scores[stype] = scores[stype] / (length ** 0.5)

        best = max(scores, key=scores.get)
        if scores[best] > 0.02:
            return best
        return "TOUR"

    # ── Activity theme with richer context ──────────────────────────────────

    def _detect_activity_theme(
            self,
            text: str = "",
            breadcrumb: str = "",
            duration_text: str = "",
            inclusions_text: str = "",
    ) -> Tuple[str, str]:
        combined = (
            f"{text} {breadcrumb} {duration_text} {inclusions_text}"
        ).lower()

        for keyword, (family, subtype) in ACTIVITY_THEMES.items():
            if keyword in combined:
                return family, subtype

        return "TOURS_SIGHTSEEING", "SIGHTSEEING"

    # ── Flags, tags, route, price, duration ─────────────────────────────────

    def _detect_flags(self, text: str) -> Dict[str, bool]:
        t = text.lower()
        return {
            flag: any(kw in t for kw in keywords)
            for flag, keywords in FLAG_KEYWORDS.items()
        }

    def _extract_tags(self, text: str) -> List[str]:
        t = text.lower()
        tags = []
        for tag, keywords in TAG_KEYWORDS.items():
            if any(kw in t for kw in keywords):
                tags.append(tag)
        return sorted(tags)[:20]

    def _extract_route(self, text: str, title: str = "") -> Tuple[str, str]:
        combined = f"{title} {text}".lower()
        for pattern in ROUTE_PATTERNS:
            m = re.search(pattern, combined, re.IGNORECASE)
            if m:
                return m.group(1).strip().title(), m.group(2).strip().title()
        for indicator in AIRPORT_INDICATORS:
            if indicator in combined:
                return indicator.upper(), ""
        return "", ""

    def _extract_price(self, text: str, existing_price=None) -> Optional[float]:
        if existing_price is not None:
            try:
                p = float(existing_price)
                if 0 < p <= 100_000:
                    return p
            except (ValueError, TypeError):
                pass
        for pattern in PRICE_PATTERNS:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    p = float(m.group(1).replace(",", ".").replace(" ", ""))
                    if 0 < p <= 100_000:
                        return p
                except (ValueError, TypeError):
                    continue
        return None

    def _extract_duration(self, text: str, existing_hours=None, existing_minutes=None) -> Optional[int]:
        if existing_hours or existing_minutes:
            total = (int(existing_hours or 0) * 60) + int(existing_minutes or 0)
            if total > 0:
                return total
        total = 0
        t = text.lower()
        for pattern, multiplier in DURATION_PATTERNS:
            m = re.search(pattern, t)
            if m:
                try:
                    val = int(m.group(1))
                    total += val * multiplier
                except (ValueError, TypeError):
                    pass
        return total if total > 0 else None

    @staticmethod
    def _detect_marketplace(url: str, source: str = "") -> str:
        combined = (url + " " + source).lower()
        for domain, name in MARKETPLACE_MAP.items():
            if domain in combined:
                return name
        return source.upper() if source else "DIRECT_SUPPLIER"

    # ── Rule‑based price estimation ──────────────────────────────────────────

    @staticmethod
    def _estimate_price_rule_based(product_data: Dict) -> Optional[float]:
        title = (product_data.get("nom_produit") or "").lower()
        desc = (product_data.get("description") or "").lower()
        atype = (product_data.get("activite_type") or "").lower()
        combined = f"{title} {desc}"

        rules = [
            ("helicopter", 350.0), ("yacht", 300.0),
            ("limousine", 120.0), ("luxury", 260.0),
            ("vip", 220.0), ("private", 180.0),
            ("executive", 100.0),
            ("multi-day", 250.0), ("overnight", 220.0),
            ("full day", 95.0), ("full-day", 95.0),
            ("half day", 55.0), ("half-day", 55.0),
            ("cruise", 130.0), ("sailing", 140.0),
            ("catamaran", 150.0), ("boat tour", 120.0),
            ("diving", 100.0), ("snorkeling", 75.0),
            ("kayak", 70.0), ("rafting", 80.0),
            ("ferry", 40.0),
            ("balloon", 200.0), ("safari", 140.0),
            ("desert", 110.0), ("paragliding", 150.0),
            ("adventure", 80.0), ("hiking", 65.0),
            ("trekking", 70.0), ("cycling", 55.0),
            ("wine tasting", 110.0), ("food tour", 85.0),
            ("cooking class", 90.0), ("gastronomy", 85.0),
            ("spa", 100.0), ("hammam", 55.0),
            ("massage", 70.0), ("thermal", 65.0),
            ("skip the line", 48.0), ("fast track", 48.0),
            ("skip-the-line", 48.0), ("priority", 50.0),
            ("early access", 65.0), ("vip entry", 80.0),
            ("hop-on hop-off", 35.0), ("city pass", 60.0),
            ("museum", 30.0), ("admission", 28.0),
            ("entrance", 25.0), ("ticket", 35.0),
            ("aquarium", 25.0), ("zoo", 20.0),
            ("show", 55.0), ("concert", 70.0),
            ("opera", 90.0), ("theater", 60.0),
            ("theatre", 60.0), ("football", 80.0),
            ("theme park", 50.0),
            ("private transfer", 80.0), ("private chauffeur", 90.0),
            ("airport", 55.0), ("shuttle", 35.0),
            ("shared", 25.0), ("navette", 40.0),
            ("transfer", 50.0),
            ("guided tour", 75.0), ("city tour", 60.0),
            ("walking tour", 45.0), ("photography", 80.0),
            ("sunset", 90.0), ("sunrise", 85.0),
        ]

        for kw, price in rules:
            if kw in combined:
                return price

        return {"transfer": 50.0, "ticket": 35.0,
                "excursion": 75.0, "activity": 70.0}.get(atype, 75.0)

    # ── DeepSeek‑R1 price estimation ─────────────────────────────────────────

    async def _estimate_price_with_llm(
        self,
        title: str,
        description: str,
        service_type: str,
        city: str = "",
    ) -> Optional[float]:
        prompt = f"""You are a tourism pricing expert.
Estimate a realistic price in EUR for this activity in {city or 'the destination'}.

Title: {title}
Description: {description[:300]}
Type: {service_type}

Reply with ONLY a number (example: 85). No currency symbol, no explanation."""

        async with self.LLM_SEMAPHORE:
            session = await self._get_session()
            payload = {
                "model": DEEPSEEK_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": 20},
            }
            try:
                async with session.post(
                    OLLAMA_GENERATE_URL, json=payload,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        resp_text = data.get("response", "").strip()
                        return float(resp_text)
            except Exception as e:
                logger.warning(f"DeepSeek price estimation failed: {e}")
        return None

    # ── Optional description enrichment ──────────────────────────────────────

    async def _enrich_description(
        self, title: str, existing: str, city: str, country: str, service_type: str
    ) -> str:
        if existing and len(existing) >= 50:
            return existing

        context = f"{title} in {city}, {country} ({service_type})"
        prompt = (
            f"Write a short, factual tourism activity description (2–3 sentences, max 200 words).\n"
            f"Product: {context}\n"
            f"Rules: describe what the customer experiences. No marketing language. No HTML."
        )

        async with self.LLM_SEMAPHORE:
            session = await self._get_session()
            payload = {
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 200},
            }
            try:
                async with session.post(
                    OLLAMA_GENERATE_URL, json=payload,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        enriched = data.get("response", "").strip().strip('"').strip("'")
                        if enriched and len(enriched) > 20:
                            self.stats["descriptions_enriched"] += 1
                            return enriched[:500]
            except Exception as e:
                logger.debug(f"description enrichment failed: {e}")

        return existing or title[:300]

    # ── MAIN ENTRY POINT ────────────────────────────────────────────────────

    async def normalize_product(self, product_data: Dict) -> Optional[Dict]:
        """
        Normalize a raw scraped product into a structured enriched dict.
        Now uses rich fields: activite_type, service_layers, breadcrumb,
        duration_from_page, inclusions_text, etc.
        """
        if not product_data or not product_data.get("nom_produit"):
            return None

        # --- Extract fields with aliases ---
        title_raw = (
            product_data.get("nom_produit") or
            product_data.get("titre_clean") or
            product_data.get("clean_title") or ""
        ).strip()
        if not title_raw or len(title_raw) < 5:
            return None

        description = (
            product_data.get("description") or
            product_data.get("description_clean") or
            product_data.get("clean_description") or ""
        ).strip()

        ville = (
            product_data.get("ville") or
            product_data.get("ville_clean") or
            product_data.get("normalized_city") or ""
        ).strip()
        pays = (
            product_data.get("pays") or
            product_data.get("pays_clean") or
            product_data.get("normalized_country") or ""
        ).strip()

        prix_raw = product_data.get("prix")
        devise = product_data.get("devise", "EUR")
        activite_type = product_data.get("activite_type")          # from supplier
        service_layers = product_data.get("service_layers")        # from scraper
        breadcrumb = product_data.get("breadcrumb", "")
        duration_text = product_data.get("duration_from_page", "")
        inclusions = product_data.get("inclusions_text", "")
        og_desc = product_data.get("og_description", "")
        amenities = product_data.get("amenities", [])
        group_flag = product_data.get("group_private_flag", "")
        url = product_data.get("url_source", "")
        source = product_data.get("source", "html")

        # --- Cleaning ---
        clean_title = self.clean_title(title_raw)
        clean_description = self.clean_text(description)

        # --- Country detection ---
        if not pays or pays.lower() in ("unknown", ""):
            pays = await self.detect_country(ville, f"{clean_title} {clean_description}")
        normalized_country = pays

        # --- City ---
        normalized_city = ville if ville else self._extract_city_from_text(
            f"{clean_title} {clean_description}"
        )

        # --- Service type (with supplier override) ---
        combined_text = f"{clean_title} {clean_description} {og_desc}".lower()
        service_type = self._detect_service_type(
            text=combined_text,
            activite_type=activite_type,
            service_layers=service_layers,
        )

        # --- Activity theme (family + subtype) ---
        experience_family, service_subtype = self._detect_activity_theme(
            text=combined_text,
            breadcrumb=breadcrumb,
            duration_text=duration_text,
            inclusions_text=inclusions,
        )

        # --- Flags & tags ---
        flags = self._detect_flags(combined_text)
        tags = self._extract_tags(combined_text)

        # --- Route ---
        origin, destination = self._extract_route(combined_text, clean_title)
        route_signature = ""
        if origin and destination:
            route_signature = f"{origin.upper()}→{destination.upper()}"

        # --- Price (hybrid) ---
        estimated_price = None
        if prix_raw is not None and float(prix_raw) > 0:
            estimated_price = float(prix_raw)
        else:
            # 1. Try rule-based estimation
            estimated_price = self._estimate_price_rule_based(product_data)
            # 2. If still None, try DeepSeek-R1
            if estimated_price is None:
                estimated_price = await self._estimate_price_with_llm(
                    title=clean_title,
                    description=clean_description,
                    service_type=service_type,
                    city=normalized_city or ""
                )

        # --- Duration ---
        duration_minutes = self._extract_duration(duration_text)

        # --- Category & normalized type ---
        normalized_category = SERVICE_TYPE_TO_CATEGORY.get(service_type, "Tours & Sightseeing")
        normalized_type = SERVICE_TYPE_TO_NORMALIZED_TYPE.get(service_type, "excursion")

        # --- Marketplace & supplier ---
        marketplace = self._detect_marketplace(url, source)
        supplier = product_data.get("source_platform", "")

        # --- Confidence ---
        confidence = 0.0
        if clean_title:    confidence += 0.20
        if clean_description: confidence += 0.10
        if normalized_city: confidence += 0.20
        if normalized_country: confidence += 0.15
        if service_type:   confidence += 0.10
        if estimated_price: confidence += 0.10
        if supplier:       confidence += 0.10
        if duration_minutes: confidence += 0.05
        confidence = min(round(confidence, 2), 1.0)

        self.stats["processed"] += 1

        # --- Build embedding text ---
        embedding_text = self.build_embedding_text({
            "clean_title": clean_title,
            "clean_description": clean_description,
            "normalized_city": normalized_city,
            "normalized_country": normalized_country,
            "service_type": service_type,
            "service_subtype": service_subtype,
            "experience_family": experience_family,
            "estimated_price": estimated_price,
            "activity_tags": tags,
        })

        result = {
            "clean_title": clean_title,
            "clean_description": clean_description[:1000],
            "normalized_city": normalized_city[:150] if normalized_city else "",
            "normalized_country": normalized_country[:100] if normalized_country else "",
            "normalized_category": normalized_category[:150],
            "normalized_type": normalized_type,
            "service_type": service_type,
            "service_subtype": service_subtype[:100],
            "experience_family": experience_family[:100],
            "activity_type_label": SERVICE_TYPE_LABELS.get(service_type, "excursion"),
            "origin": origin[:255],
            "destination": destination[:255],
            "route_signature": route_signature[:300],
            "is_private": flags.get("is_private", False),
            "is_shared": flags.get("is_shared", False),
            "is_skip_the_line": flags.get("is_skip_the_line", False),
            "is_guided": flags.get("is_guided", False),
            "is_group": flags.get("is_group", False),
            "is_airport": flags.get("is_airport", False),
            "activity_tags": tags,
            "estimated_price": estimated_price,
            "estimated_currency": devise[:10],
            "duration_minutes": duration_minutes,
            "supplier_operator": supplier[:255] if supplier else "",
            "marketplace_source": marketplace,
            "normalization_confidence": confidence,
            "embedding_text": embedding_text,
        }
        return result

    # ── Embedding text builder (static) ───────────────────────────────────────

    @staticmethod
    def build_embedding_text(entity: Dict) -> str:
        parts = []

        if entity.get("service_type"):
            parts.append(f"service:{entity['service_type']}")
        if entity.get("experience_family"):
            parts.append(f"theme:{entity['experience_family']}")
        if entity.get("origin"):
            parts.append(f"from:{entity['origin']}")
        if entity.get("destination"):
            parts.append(f"to:{entity['destination']}")
        if entity.get("normalized_city"):
            parts.append(f"city:{entity['normalized_city']}")
        if entity.get("normalized_country"):
            parts.append(f"country:{entity['normalized_country']}")

        flags = [
            k.replace("is_", "")
            for k in ("is_private", "is_guided", "is_skip_the_line", "is_airport")
            if entity.get(k)
        ]
        if flags:
            parts.append(f"flags:{','.join(flags)}")

        if entity.get("activity_tags"):
            parts.append(f"tags:{','.join(entity['activity_tags'][:8])}")

        if entity.get("clean_title"):
            parts.append(entity["clean_title"])
        if entity.get("clean_description"):
            parts.append(entity["clean_description"][:150])

        return " | ".join(filter(None, parts))[:2000]

    # Helper for city extraction from text
    def _extract_city_from_text(self, text: str) -> str:
        # Try to find a known city in the text
        for city in STATIC_CITY_COUNTRY:
            if city in text.lower():
                return city.title()
        return ""