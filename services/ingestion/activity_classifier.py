"""
services/ingestion/activity_classifier.py
Deterministic rule-based classifier.

Converts ANY raw categorie string → canonical_activity_type.
Only 3 allowed values: EXCURSION | TICKET | TRANSFER

Rules defined in architecture spec (message 3 in what_happend).
Used at scraping time (Sprint 3) and normalization time (Sprint 4).
"""
import re
from typing import Optional

# ─────────────────────────────────────────────────────────────
# TRANSFER keywords — highest priority
# Airport/transport services → TRANSFER
# ─────────────────────────────────────────────────────────────
TRANSFER_KEYWORDS = {
    # Transport types
    "transfer", "shuttle", "taxi", "chauffeur", "limousine",
    "minibus", "bus company", "bus charter", "coach",
    "transportation", "transport service", "car service",
    "pickup", "drop-off", "airport transfer", "airport shuttle",
    "private driver", "ride", "minivan", "van service",
    # French
    "transfert", "navette", "chauffeur privé", "vtc",
    "transport aéroport", "service de transport",
    # Italian
    "trasferimento", "navetta", "autista",
    # Spanish
    "traslado", "transporte",
    # Infrastructure signals
    "metro company", "train service",
}

# ─────────────────────────────────────────────────────────────
# TICKET keywords — static attractions with entrance fees
# ─────────────────────────────────────────────────────────────
TICKET_KEYWORDS = {
    # Attractions
    "museum", "musée", "museo", "musee",
    "zoo", "aquarium", "aquatic park", "water park",
    "amusement park", "theme park", "parc d'attraction",
    "parc aquatique", "parco divertimenti",
    "castle", "château", "chateau", "palazzo",
    "archaeological site", "site archéologique",
    "monument", "historical landmark", "landmark",
    "observation deck", "viewpoint", "belvédère",
    "amphitheater", "amphithéâtre", "colosseum",
    "ruins", "ruines", "palace", "palais",
    "ticket office", "billetterie", "biglietteria",
    "performing arts", "theater", "theatre", "opera",
    "safari park", "wildlife park", "nature reserve",
    "botanical garden", "jardin botanique",
    "science center", "planetarium",
    "skip the line", "fast track", "coupe-file",
    # Signals
    "entrance", "entrée", "admission", "entry",
}

# ─────────────────────────────────────────────────────────────
# EXCURSION keywords — guided tours, activities, experiences
# Default fallback if nothing else matches
# ─────────────────────────────────────────────────────────────
EXCURSION_KEYWORDS = {
    "tour", "tour agency", "tour operator",
    "excursion", "excursions", "guided tour",
    "sightseeing", "city tour", "day tour", "day trip",
    "boat tour", "sailing", "cruise", "yacht tour",
    "scuba", "diving", "snorkeling",
    "whale watching", "dolphin watching",
    "balloon tour", "hot air balloon",
    "hiking", "trekking", "walking tour",
    "outdoor activity", "adventure", "activity organiser",
    "experience", "experiences",
    "travel agency", "travel agent", "agence de voyage",
    "agenzia viaggi", "agencia de viajes",
    "cooking class", "food tour", "gastronomic tour",
    "wine tour", "cycling tour", "horse riding",
    "nightlife tour", "pub crawl",
    # French
    "excursion", "visite guidée", "sortie",
    # Italian
    "gita", "escursione", "tour guidato",
    # Spanish
    "excursión", "visita guiada",
}

# ─────────────────────────────────────────────────────────────
# Classifier
# ─────────────────────────────────────────────────────────────

def classify_activity(
    categorie_raw: Optional[str],
    nom_produit: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """
    Deterministic rule-based classifier.
    Priority: TRANSFER > TICKET > EXCURSION (default).

    Args:
        categorie_raw: raw category from scraping
        nom_produit:   product name (additional signal)
        description:   product description (fallback signal)

    Returns:
        'EXCURSION' | 'TICKET' | 'TRANSFER'
    """
    # Build search text from all available signals
    parts = [
        (categorie_raw or "").lower(),
        (nom_produit or "")[:200].lower(),
        (description or "")[:500].lower(),
    ]
    text = " ".join(parts)

    # Normalize punctuation
    text = re.sub(r"[-_/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return "EXCURSION"  # default

    # ── Priority 1: TRANSFER ────────────────────────────────
    for kw in TRANSFER_KEYWORDS:
        if kw in text:
            return "TRANSFER"

    # ── Priority 2: TICKET ──────────────────────────────────
    for kw in TICKET_KEYWORDS:
        if kw in text:
            return "TICKET"

    # ── Priority 3: EXCURSION (default) ─────────────────────
    # Also catches: restaurant → EXCURSION (experience)
    # hotel → EXCURSION (experience context)
    return "EXCURSION"


def classify_supplier_categories(
    categories: Optional[list],
    supplier_name: Optional[str] = None,
) -> str:
    """
    Classify a supplier's raw Google Maps categories list
    into a canonical activity type.

    Used in Sprint 2 supplier qualification.

    Args:
        categories: list of raw strings from Google Maps
        supplier_name: supplier name for fallback

    Returns:
        'EXCURSION' | 'TICKET' | 'TRANSFER'
    """
    if not categories:
        return classify_activity(None, supplier_name)

    combined = " ".join(str(c).lower() for c in categories)
    return classify_activity(combined, supplier_name)
