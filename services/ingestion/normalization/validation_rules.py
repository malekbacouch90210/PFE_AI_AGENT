"""
services/ingestion/normalization/validation_rules.py
POST-AI VALIDATION RULES LAYER — Sprint 4

Runs AFTER AI normalization to catch known failure patterns.
Fixes wrong classifications before embedding + saving.

Problems solved:
  ❌ "Golf in Djerba" → classified as NAUTIQUE (should be SPORT)
  ❌ Stockholm supplier → country=Norway (should be Sweden)
  ❌ Airport transfer classified as EXCURSION
  ❌ Museum ticket classified as EXCURSION

Rule priority:
  1. Transfer rules (strongest signals — transport keywords)
  2. Ticket rules (attraction/venue keywords)
  3. Activity/category correction rules
  4. Country correction rules (supplier vs product location)
  5. Price plausibility rules
"""

import re
from dataclasses import dataclass
from typing import Optional

from loguru import logger


# ─────────────────────────────────────────────────────────────
# ValidationResult
# ─────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    """Result of validation — may correct AI output."""
    activity_type:    str
    category:         str
    normalized_city:  str
    normalized_country: str
    price_eur:        Optional[float]
    was_corrected:    bool = False
    corrections:      list = None

    def __post_init__(self):
        if self.corrections is None:
            self.corrections = []


# ─────────────────────────────────────────────────────────────
# Activity type correction rules
# ─────────────────────────────────────────────────────────────

# Keywords that FORCE activity type regardless of AI output
FORCE_TRANSFER = [
    "airport transfer", "airport shuttle", "private transfer",
    "private driver", "chauffeur service", "taxi service",
    "minibus transfer", "shuttle service", "port transfer",
    "hotel transfer", "cruise transfer",
    "navette aéroport", "transfert aéroport",
    "trasferimento aeroporto",
]

FORCE_TICKET = [
    "skip the line", "skip-the-line", "entrance ticket",
    "admission ticket", "museum ticket", "entry ticket",
    "coupe-file", "fast track entry",
    "biglietto ingresso", "entrada sin colas",
]

# Keywords that FORBID certain categories
CATEGORY_FORBID_RULES = {
    # If text contains these keywords → category CANNOT be NAUTIQUE
    "NAUTIQUE": [
        "golf", "golf course", "golf club", "golf tour",
        "tennis", "ski", "football", "cycling", "running",
        "yoga", "fitness", "gym",
    ],
    # If text contains these → category CANNOT be SPORT
    "SPORT": [
        "museum", "musée", "gallery", "cathedral", "castle",
        "palace", "ruins", "archaeological",
    ],
    # If text contains these → category CANNOT be CULTUREL
    "CULTUREL": [
        "airport", "aéroport", "shuttle", "transfer",
        "taxi", "driver",
    ],
}

# Category correction map — what to assign instead
CATEGORY_CORRECTION_MAP = {
    "golf":           "SPORT",
    "tennis":         "SPORT",
    "ski":            "SPORT",
    "cycling":        "SPORT",
    "football":       "SPORT",
    "yoga":           "BIEN_ETRE",
    "spa":            "BIEN_ETRE",
    "wellness":       "BIEN_ETRE",
    "hammam":         "BIEN_ETRE",
    "museum":         "HISTORIQUE",
    "castle":         "HISTORIQUE",
    "ruins":          "HISTORIQUE",
    "cathedral":      "HISTORIQUE",
    "palace":         "HISTORIQUE",
    "cathedral":      "HISTORIQUE",
    "food tour":      "GASTRONOMIE",
    "cooking class":  "GASTRONOMIE",
    "wine tour":      "GASTRONOMIE",
    "boat tour":      "NAUTIQUE",
    "sailing":        "NAUTIQUE",
    "snorkeling":     "NAUTIQUE",
    "diving":         "NAUTIQUE",
    "hiking":         "NATURE",
    "trekking":       "NATURE",
    "safari":         "NATURE",
    "wildlife":       "NATURE",
    "desert":         "AVENTURE",
    "4x4":            "AVENTURE",
    "quad":           "AVENTURE",
    "zip line":       "AVENTURE",
    "family":         "FAMILLE",
    "kids":           "FAMILLE",
    "theme park":     "FAMILLE",
    "zoo":            "FAMILLE",
}

# ─────────────────────────────────────────────────────────────
# Country correction rules
# ─────────────────────────────────────────────────────────────

# Supplier country → LIKELY product country override pairs
# When supplier is from country A but product mentions country B
COUNTRY_MENTION_PATTERNS = [
    # European cities with known countries
    ("stockholm", "sweden"),
    ("gothenburg", "sweden"),
    ("oslo", "norway"),
    ("bergen", "norway"),
    ("copenhagen", "denmark"),
    ("helsinki", "finland"),
    ("reykjavik", "iceland"),
    ("amsterdam", "netherlands"),
    ("brussels", "belgium"),
    ("zurich", "switzerland"),
    ("geneva", "switzerland"),
    ("vienna", "austria"),
    ("prague", "czech republic"),
    ("budapest", "hungary"),
    ("warsaw", "poland"),
    ("bucharest", "romania"),
    ("sofia", "bulgaria"),
    ("zagreb", "croatia"),
    ("ljubljana", "slovenia"),
    ("bratislava", "slovakia"),
    ("riga", "latvia"),
    ("tallinn", "estonia"),
    ("vilnius", "lithuania"),
    # North Africa
    ("tunis", "tunisia"),
    ("djerba", "tunisia"),
    ("sfax", "tunisia"),
    ("sousse", "tunisia"),
    ("casablanca", "morocco"),
    ("marrakech", "morocco"),
    ("fez", "morocco"),
    ("cairo", "egypt"),
    ("luxor", "egypt"),
    ("sharm el sheikh", "egypt"),
    ("algiers", "algeria"),
    ("constantine", "algeria"),
    # Middle East
    ("dubai", "united arab emirates"),
    ("abu dhabi", "united arab emirates"),
    ("doha", "qatar"),
    ("riyadh", "saudi arabia"),
    ("istanbul", "turkey"),
    ("amman", "jordan"),
    ("petra", "jordan"),
    # Asia
    ("tokyo", "japan"),
    ("osaka", "japan"),
    ("bangkok", "thailand"),
    ("phuket", "thailand"),
    ("bali", "indonesia"),
    ("singapore", "singapore"),
    ("hong kong", "china"),
    ("beijing", "china"),
    ("shanghai", "china"),
]


# ─────────────────────────────────────────────────────────────
# ValidationRules
# ─────────────────────────────────────────────────────────────

class ValidationRules:
    """
    Post-AI validation and correction layer.

    Applies deterministic rules on top of AI normalization output
    to catch known failure patterns.

    Run this AFTER AI normalization, BEFORE embedding.
    """

    def validate(
        self,
        activity_type:     str,
        category:          str,
        normalized_city:   str,
        normalized_country: str,
        price_eur:         Optional[float],
        clean_title:       str,
        clean_description: str,
    ) -> ValidationResult:
        """
        Validate and potentially correct AI normalization output.
        Returns ValidationResult with was_corrected=True if anything changed.
        """
        corrections = []
        combined_text = f"{clean_title} {clean_description}".lower()

        # ── Rule 1: Force activity type from strong signals ──
        activity_type, corrections = self._validate_activity_type(
            activity_type, combined_text, corrections
        )

        # ── Rule 2: Correct category ──────────────────────────
        category, corrections = self._validate_category(
            category, activity_type, combined_text, corrections
        )

        # ── Rule 3: Correct country from product text ─────────
        normalized_country, corrections = self._validate_country(
            normalized_country, normalized_city, combined_text, corrections
        )

        # ── Rule 4: Validate price plausibility ───────────────
        price_eur, corrections = self._validate_price(
            price_eur, activity_type, corrections
        )

        was_corrected = len(corrections) > 0
        if was_corrected:
            logger.debug(
                f"  🔧 Validation corrections for '{clean_title[:40]}':\n"
                + "\n".join(f"     {c}" for c in corrections)
            )

        return ValidationResult(
            activity_type     = activity_type,
            category          = category,
            normalized_city   = normalized_city,
            normalized_country= normalized_country,
            price_eur         = price_eur,
            was_corrected     = was_corrected,
            corrections       = corrections,
        )

    # ──────────────────────────────────────────────────────────
    # Rule 1 — Activity type
    # ──────────────────────────────────────────────────────────

    def _validate_activity_type(
        self, current: str, text: str, corrections: list
    ):
        # Force TRANSFER
        for signal in FORCE_TRANSFER:
            if signal in text:
                if current != "TRANSFER":
                    corrections.append(
                        f"activity_type: {current} → TRANSFER "
                        f"(forced by '{signal}')"
                    )
                    current = "TRANSFER"
                return current, corrections

        # Force TICKET
        for signal in FORCE_TICKET:
            if signal in text:
                if current != "TICKET":
                    corrections.append(
                        f"activity_type: {current} → TICKET "
                        f"(forced by '{signal}')"
                    )
                    current = "TICKET"
                return current, corrections

        return current, corrections

    # ──────────────────────────────────────────────────────────
    # Rule 2 — Category correction
    # ──────────────────────────────────────────────────────────

    def _validate_category(
        self, current: str, activity_type: str, text: str, corrections: list
    ):
        # Check if current category is forbidden given text content
        forbidden_keywords = CATEGORY_FORBID_RULES.get(current, [])
        for kw in forbidden_keywords:
            if kw in text:
                # Find the correct category from correction map
                new_cat = CATEGORY_CORRECTION_MAP.get(kw)
                if not new_cat:
                    # Use activity type as fallback
                    new_cat = {
                        "TRANSFER": "TRANSPORT",
                        "TICKET":   "CULTUREL",
                        "EXCURSION": "CULTUREL",
                    }.get(activity_type, "CULTUREL")
                if new_cat != current:
                    corrections.append(
                        f"category: {current} → {new_cat} "
                        f"('{kw}' found, {current} invalid)"
                    )
                    current = new_cat
                break

        # Transfer always → TRANSPORT
        if activity_type == "TRANSFER" and current != "TRANSPORT":
            corrections.append(
                f"category: {current} → TRANSPORT (activity=TRANSFER)"
            )
            current = "TRANSPORT"

        # Check correction map for positive signals
        for kw, cat in CATEGORY_CORRECTION_MAP.items():
            if kw in text and current == "CULTUREL" and cat != "CULTUREL":
                corrections.append(
                    f"category: {current} → {cat} (keyword '{kw}')"
                )
                current = cat
                break

        return current, corrections

    # ──────────────────────────────────────────────────────────
    # Rule 3 — Country correction from product content
    # ──────────────────────────────────────────────────────────

    def _validate_country(
        self,
        current_country: str,
        current_city:    str,
        text:            str,
        corrections:     list,
    ):
        """
        Validate country using city mentions in product text.
        Fixes Stockholm supplier → Norway (should be Sweden).

        IMPORTANT: pays_raw is WEAK SIGNAL — product may be in different country
        than the supplier's operating country.
        """
        text_lower = text.lower()
        city_lower = (current_city or "").lower()

        for city_hint, expected_country in COUNTRY_MENTION_PATTERNS:
            # City in product text → derive country
            if city_hint in text_lower or city_hint == city_lower:
                if current_country.lower() != expected_country:
                    # Only correct if we have a strong city signal
                    corrections.append(
                        f"country: '{current_country}' → '{expected_country.title()}' "
                        f"(city '{city_hint}' found in product)"
                    )
                    return expected_country.title(), corrections

        return current_country, corrections

    # ──────────────────────────────────────────────────────────
    # Rule 4 — Price plausibility
    # ──────────────────────────────────────────────────────────

    def _validate_price(
        self,
        price: Optional[float],
        activity_type: str,
        corrections: list,
    ):
        if price is None:
            return price, corrections

        # Price bounds by activity type
        bounds = {
            "EXCURSION": (3.0,  2000.0),
            "TICKET":    (1.0,  500.0),
            "TRANSFER":  (5.0,  1000.0),
        }
        lo, hi = bounds.get(activity_type, (1.0, 5000.0))

        if price < lo:
            corrections.append(
                f"price: {price}€ below minimum {lo}€ for {activity_type} → clamped"
            )
            price = lo
        elif price > hi:
            corrections.append(
                f"price: {price}€ above maximum {hi}€ for {activity_type} → clamped"
            )
            price = hi

        return price, corrections


# Singleton
validation_rules = ValidationRules()
