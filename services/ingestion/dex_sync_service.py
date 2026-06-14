"""
DEX Sync Service — Creates new DEX products from matched NEW_PRODUCT items.
Uses unified EntityBuilder and NormalizationService.
"""

from datetime import datetime
from typing import Dict, Optional
from loguru import logger
from sqlalchemy import select, func
from database import DatabaseManager
from models import (
    DexProduit, DexLocalite, DexDetail, DexProduitService,
    DexTypeServiceActivite, ProduitMatches, ScrapedProductNormalized
)
from services.ingestion.entity_builder import EntityBuilder
from services.normalization_service import NormalizationService


class DexSyncService:
    """Synchronizes NEW_PRODUCT matches into DEX tables."""

    def __init__(self):
        self.db = DatabaseManager()
        self.entity_builder = EntityBuilder()
        self.normalizer = NormalizationService()
        logger.info("🔄 DEX Sync Service ready (using unified EntityBuilder)")

    async def sync_new_products(self, batch_size: int = 100) -> Dict:
        """
        Sync all NEW_PRODUCT matches to DEX tables.
        Creates: dex_produit, dex_localite, dex_detail entries.
        """
        stats = {"created": 0, "skipped": 0, "errors": 0}

        async with self.db.AsyncSessionLocal() as session:
            # Get NEW_PRODUCT matches that haven't been synced yet
            result = await session.execute(
                select(ProduitMatches)
                .where(
                    (ProduitMatches.match_status == "NEW_PRODUCT") &
                    (ProduitMatches.dex_product_id.is_(None))
                )
                .limit(batch_size)
            )
            matches = result.scalars().all()

            if not matches:
                logger.info("No new products to sync")
                return stats

            for match in matches:
                try:
                    # Get the normalized scraped product
                    scraped_result = await session.execute(
                        select(ScrapedProductNormalized).where(
                            ScrapedProductNormalized.scraped_product_id == match.scraped_product_id
                        )
                    )
                    scraped = scraped_result.scalar_one_or_none()
                    if not scraped:
                        continue

                    # Build canonical entity using unified EntityBuilder
                    canonical = self.entity_builder.build_canonical(
                        product={
                            "clean_title": scraped.clean_title,
                            "clean_description": scraped.clean_description,
                            "normalized_city": scraped.normalized_city,
                            "normalized_country": scraped.normalized_country,
                            "normalized_type": scraped.normalized_type,
                            "normalized_category": scraped.normalized_category,
                            "estimated_price": scraped.estimated_price,
                            "estimated_currency": scraped.estimated_currency,
                            "supplier_operator": scraped.supplier_operator,
                            "image_url": scraped.image_url,
                        },
                        source="SCRAPED"
                    )

                    # Normalize using the unified normalizer
                    normalized = await self.normalizer.normalize_product(canonical)

                    # 1. Find or create localite (city)
                    localite_id = await self._get_or_create_localite(
                        session,
                        scraped.normalized_city,
                        scraped.normalized_country
                    )

                    # 2. Create DEX product
                    price = match.estimated_missing_price or scraped.estimated_price or 0
                    currency = scraped.estimated_currency or "EUR"

                    # Use normalized service type if available
                    service_type = normalized.get("normalized_type") if normalized else scraped.normalized_type
                    typeprestation = self._map_type_to_code(service_type or scraped.normalized_type)

                    # Get title from normalized data
                    title = normalized.get("clean_title") if normalized else scraped.clean_title or "Scraped Product"

                    new_dex = DexProduit(
                        nom=title[:200],
                        nomen=title[:200],
                        nominitial=title[:200],
                        description=scraped.clean_description or title,
                        typeprestation=typeprestation,
                        producttype=service_type or scraped.normalized_type,
                        productcategory=scraped.normalized_category,
                        idville=localite_id,
                        prixappel=float(price) if price else None,
                        prixappelachat=float(price) * 0.85 if price else None,
                        prixmini=float(price) * 0.8 if price else None,
                        prixmaxi=float(price) * 1.2 if price else None,
                        devise=currency,
                        tauxtva=0,
                        actif=1,
                        vendable=1,
                        b2c=1,
                        valide=1,
                        datecreation=datetime.utcnow(),
                        datemodification=datetime.utcnow(),
                        organisateur=scraped.supplier_operator or "Scraped Supplier",
                        adresse=f"{scraped.normalized_city}, {scraped.normalized_country}",
                    )
                    session.add(new_dex)
                    await session.flush()  # Get the new ID

                    # 3. Create dex_detail (French)
                    detail = DexDetail(
                        idobjet=new_dex.id,
                        idlangue=1,  # French
                        ordre=1,
                        titre=title[:200],
                        texte=scraped.clean_description or title,
                    )
                    session.add(detail)

                    # English detail
                    detail_en = DexDetail(
                        idobjet=new_dex.id,
                        idlangue=2,  # English
                        ordre=2,
                        titre=title[:200],
                        texte=scraped.clean_description or title,
                    )
                    session.add(detail_en)

                    # 4. Create dex_produit_service
                    service_type_id = await self._get_service_type_id(session, typeprestation)
                    if service_type_id:
                        service = DexProduitService(
                            idproduit=new_dex.id,
                            idtypeserviceactivite=service_type_id,
                            idlocalite=localite_id,
                            inclut=0,
                            ordre=1,
                            description=scraped.clean_description or title,
                            descriptionen=scraped.clean_description or title,
                            lieu=f"{scraped.normalized_city}, {scraped.normalized_country}",
                        )
                        session.add(service)

                    # Update match with dex_product_id
                    match.dex_product_id = new_dex.id
                    stats["created"] += 1
                    logger.info(f"  ✅ Created DEX product #{new_dex.id}: {title[:50]}")

                except Exception as e:
                    logger.error(f"Sync error match_id={match.id}: {e}")
                    stats["errors"] += 1

            await session.commit()

        logger.info(f"✅ Sync complete: {stats}")
        return stats

    async def _get_or_create_localite(self, session, city: str, country: str) -> int:
        """Find existing localite or create new one."""
        if not city:
            # Get a default localite
            result = await session.execute(select(DexLocalite).limit(1))
            default = result.scalar_one_or_none()
            if default:
                return default.id
            return 1  # Fallback

        # Try exact match
        result = await session.execute(
            select(DexLocalite).where(
                (func.lower(DexLocalite.nom) == city.lower()) |
                (func.lower(DexLocalite.nomen) == city.lower())
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            return existing.id

        # Create new localite
        iso_map = {
            "france": "fr", "italy": "it", "spain": "es", "germany": "de",
            "united kingdom": "gb", "portugal": "pt", "netherlands": "nl",
            "tunisia": "tn", "morocco": "ma", "egypt": "eg",
            "united states": "us", "mexico": "mx", "brazil": "br",
            "japan": "jp", "thailand": "th", "australia": "au",
        }
        country_lower = (country or "").lower()
        iso_code = iso_map.get(country_lower, country_lower[:2])

        new_localite = DexLocalite(
            nom=city,
            nomen=city,
            nompt=city,
            codeiso=iso_code,
            active=1,
            typelocalite=1,
        )
        session.add(new_localite)
        await session.flush()
        return new_localite.id

    def _map_type_to_code(self, normalized_type: str) -> str:
        """Map type string to DEX typeprestation code."""
        type_map = {
            "excursion": "2",
            "ticket": "449",
            "transfer": "3",
            "activity": "2",
            "event": "2",
        }
        return type_map.get(normalized_type, "2")

    async def _get_service_type_id(self, session, type_code: str) -> Optional[int]:
        """Get service type ID from dex_type_service_activite."""
        result = await session.execute(
            select(DexTypeServiceActivite).where(
                DexTypeServiceActivite.code == type_code
            )
        )
        service_type = result.scalar_one_or_none()
        if service_type:
            return service_type.id
        return None

    async def close(self):
        """Clean up resources."""
        await self.normalizer.close()