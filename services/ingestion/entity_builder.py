"""
Entity Builder — Unified Tourism Entity Constructor
===================================================

Purpose:
- Accepts ANY raw product (DEX or scraped)
- Produces canonical input for NormalizationService
- NO DB access, NO AI logic, NO matching

This replaces dex_entity_builder.py and scraped_entity_builder.py
"""

from typing import Dict, Optional
from loguru import logger
from datetime import datetime


class EntityBuilder:
    """
    Single entry point for ALL product types (DEX + scraped).
    Pure transformation — no AI, no DB, no matching.
    """

    def __init__(self):
        logger.info("🏗️ Unified EntityBuilder ready (DEX + Scraped)")

    def build_canonical(self, product: Dict, source: str = "UNKNOWN") -> Optional[Dict]:
        """
        Convert ANY input into unified canonical schema.

        Args:
            product: raw DEX or scraped product dict
            source: "DEX" or "SCRAPED" (auto-detected if not provided)

        Returns:
            Canonical dict with standardized fields ready for normalization
        """
        # Auto-detect source if not provided
        if source == "UNKNOWN":
            source = self._detect_source(product)

        if source == "DEX":
            return self._canonicalize_dex(product)
        else:
            return self._canonicalize_scraped(product)

    def _detect_source(self, product: Dict) -> str:
        """Detect if product is from DEX or scraped."""
        if product.get("nomen") is not None or product.get("dex_product_id") is not None:
            return "DEX"
        if product.get("nom_produit") is not None or product.get("source_platform") is not None:
            return "SCRAPED"
        return "SCRAPED"

    def _canonicalize_dex(self, product: Dict) -> Dict:
        """Convert DEX product to canonical schema."""
        return {
            # Core identity
            "title": product.get("nomen") or product.get("nom") or product.get("nominitial") or "",
            "description": product.get("description") or "",

            # Geography
            "city": product.get("city_name") or product.get("ville") or "",
            "country": product.get("country_code") or "",

            # Pricing
            "price_min": product.get("prixmini"),
            "price_max": product.get("prixmaxi"),
            "currency": product.get("devise", "EUR"),

            # Business
            "operator": product.get("organisateur") or "",
            "service_type_raw": product.get("typeprestation") or "",

            # Metadata
            "source": "DEX",
            "source_id": product.get("id") or product.get("dex_product_id"),
            "url": product.get("url_source") or product.get("booking_url"),

            # Timestamp
            "created_at": datetime.utcnow().isoformat(),
        }

    def _canonicalize_scraped(self, product: Dict) -> Dict:
        """Convert scraped product to canonical schema."""
        return {
            # Core identity
            "title": product.get("nom_produit") or product.get("titre_clean") or product.get("clean_title") or "",
            "description": product.get("description") or product.get("description_clean") or "",

            # Geography
            "city": product.get("ville") or product.get("ville_clean") or product.get("normalized_city") or "",
            "country": product.get("pays") or product.get("pays_clean") or product.get("normalized_country") or "",

            # Pricing
            "price_min": product.get("prix") or product.get("estimated_price"),
            "price_max": product.get("prix_max") or product.get("estimated_price_max"),
            "currency": product.get("devise") or product.get("estimated_currency") or "EUR",

            # Business
            "operator": product.get("operator") or product.get("fournisseur") or product.get("supplier_operator") or "",
            "service_type_raw": product.get("activite_type") or product.get("normalized_type") or "",

            # Metadata
            "source": "SCRAPED",
            "source_id": product.get("id") or product.get("scraped_product_id") or product.get("produit_id"),
            "url": product.get("url_source") or product.get("booking_url") or product.get("url"),

            # Image (store URL only, never download)
            "image_url": product.get("image_url"),

            # Timestamp
            "created_at": datetime.utcnow().isoformat(),
        }

    def build_embedding_text(self, entity: Dict) -> str:
        """
        Build embedding text from canonical entity.
        This MUST be consistent for matching to work.
        """
        parts = []

        # Service type (strongest signal)
        if entity.get("service_type"):
            parts.append(f"service:{entity['service_type']}")

        # Location
        if entity.get("normalized_city"):
            parts.append(f"city:{entity['normalized_city']}")
        if entity.get("normalized_country"):
            parts.append(f"country:{entity['normalized_country']}")

        # Category/theme
        if entity.get("experience_family"):
            parts.append(f"theme:{entity['experience_family']}")

        # Title and description
        if entity.get("clean_title"):
            parts.append(entity["clean_title"])
        if entity.get("clean_description"):
            parts.append(entity["clean_description"][:200])

        return " | ".join(filter(None, parts))[:2000]