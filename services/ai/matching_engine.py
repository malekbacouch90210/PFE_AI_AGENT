# services/ai/matching_engine.py
"""
Structured Tourism Matching Engine V2.
Uses: city + service_type + route + theme + flags + embeddings.
NOT embeddings-first anymore.
"""

import asyncio
import aiohttp
import json
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger
from sqlalchemy import text, select, func
from database import DatabaseManager
from models import ScrapedProductNormalized, DexProductNormalized, ProduitMatches


class TourismMatchingEngine:
    """
    Hybrid tourism matching engine.

    Scoring weights:
      - Service type match: 20%
      - Route match: 20%
      - City match: 15%
      - Theme match: 15%
      - Flags match: 10%
      - Embedding similarity: 20%
    """

    WEIGHTS = {
        "service_type": 0.20,
        "route": 0.20,
        "city": 0.15,
        "theme": 0.15,
        "flags": 0.10,
        "embedding": 0.20,
    }

    OLLAMA_URL = "http://localhost:11434/api/generate"
    LLM_MODEL = "qwen2.5:7b"

    MATCH_THRESHOLD = 0.70
    REVIEW_THRESHOLD = 0.50
    BATCH_SIZE = 50

    def __init__(self):
        self.db = DatabaseManager()
        self._session = None
        self.stats = {"matched": 0, "new": 0, "review": 0, "errors": 0}

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    # ══════════════════════════════════════════════════════════
    # MAIN MATCHING
    # ══════════════════════════════════════════════════════════

    async def match_all_unmatched(self) -> Dict:
        """Match all unmatched products using structured tourism logic."""

        async with self.db.AsyncSessionLocal() as session:
            matched_ids = select(ProduitMatches.scraped_product_id)
            total = await session.execute(
                select(func.count(ScrapedProductNormalized.id)).where(
                    (ScrapedProductNormalized.embedding.isnot(None)) &
                    (~ScrapedProductNormalized.scraped_product_id.in_(matched_ids))
                )
            )
            total = total.scalar() or 0

        if total == 0:
            logger.info("✅ All products already matched!")
            return {"total": 0, "matched": 0, "new": 0, "review": 0}

        logger.info(f"🔍 {total:,} products to match with structured tourism logic")

        offset = 0
        while offset < total:
            async with self.db.AsyncSessionLocal() as session:
                result = await session.execute(
                    select(ScrapedProductNormalized)
                    .where(
                        (ScrapedProductNormalized.embedding.isnot(None)) &
                        (~ScrapedProductNormalized.scraped_product_id.in_(matched_ids))
                    )
                    .order_by(ScrapedProductNormalized.id)
                    .offset(offset)
                    .limit(self.BATCH_SIZE)
                )
                batch = result.scalars().all()

                if not batch:
                    break

                for scraped in batch:
                    try:
                        await self._match_single_structured(scraped, session)
                    except Exception as e:
                        logger.error(f"Match error id={scraped.id}: {e}")
                        self.stats["errors"] += 1

                await session.commit()

                progress = min(offset + self.BATCH_SIZE, total)
                logger.info(
                    f"📊 {progress:,}/{total:,} | "
                    f"M={self.stats['matched']} N={self.stats['new']} R={self.stats['review']}"
                )
                offset += self.BATCH_SIZE

        return {
            "total": total,
            "matched": self.stats["matched"],
            "new": self.stats["new"],
            "review": self.stats["review"],
        }

    async def _match_single_structured(self, scraped: ScrapedProductNormalized, session) -> None:
        """Match one product using structured tourism scoring."""

        # Skip if already matched
        existing = await session.execute(
            select(ProduitMatches).where(ProduitMatches.scraped_product_id == scraped.scraped_product_id)
        )
        if existing.scalar_one_or_none():
            return

        # Get candidates filtered by city + service_type
        candidates = await self._get_filtered_candidates(scraped, session)

        if not candidates:
            await self._save_new(session, scraped)
            self.stats["new"] += 1
            return

        # Score each candidate
        best_score = 0
        best_match = None

        for candidate in candidates:
            score = self._compute_structured_score(scraped, candidate)
            if score > best_score:
                best_score = score
                best_match = candidate
                best_match["final_score"] = score

        if best_score >= self.MATCH_THRESHOLD:
            await self._save_match(session, scraped, best_match, "MATCHED")
            self.stats["matched"] += 1
        elif best_score >= self.REVIEW_THRESHOLD:
            await self._save_match(session, scraped, best_match, "REVIEW_REQUIRED")
            self.stats["review"] += 1
        else:
            await self._save_new(session, scraped)
            self.stats["new"] += 1

    # ══════════════════════════════════════════════════════════
    # FILTERED CANDIDATE RETRIEVAL
    # ══════════════════════════════════════════════════════════

    async def _get_filtered_candidates(self, scraped, session) -> List[Dict]:
        """Get candidates filtered by city + service_type first."""

        # Base query with embedding similarity
        emb_str = ",".join(str(v) for v in (scraped.embedding or []))

        sql = """
            SELECT 
                d.dex_product_id,
                d.clean_title,
                d.service_type,
                d.service_subtype,
                d.experience_family,
                d.normalized_city,
                d.normalized_country,
                d.origin,
                d.destination,
                d.route_signature,
                d.is_private,
                d.is_shared,
                d.is_skip_the_line,
                d.is_guided,
                d.is_group,
                d.is_airport,
                d.activity_tags,
                d.supplier_operator,
                d.price_min,
                d.price_max,
                d.currency,
                1 - (d.embedding <=> ARRAY[{}]::vector) AS embedding_sim
            FROM dex_products_normalized d
            WHERE d.embedding IS NOT NULL
        """.format(emb_str)

        params = {"limit": 10}

        # ✅ Filter by same service_type (strongest filter)
        if scraped.service_type:
            sql += " AND d.service_type = :service_type"
            params["service_type"] = scraped.service_type

        # ✅ Filter by same city
        if scraped.normalized_city:
            sql += " AND LOWER(d.normalized_city) = LOWER(:city)"
            params["city"] = scraped.normalized_city

        sql += " ORDER BY d.embedding <=> ARRAY[{}]::vector LIMIT :limit".format(emb_str)

        result = await session.execute(text(sql), params)
        rows = result.fetchall()

        return [
            {
                "dex_product_id": row[0],
                "clean_title": row[1],
                "service_type": row[2],
                "service_subtype": row[3],
                "experience_family": row[4],
                "normalized_city": row[5],
                "normalized_country": row[6],
                "origin": row[7],
                "destination": row[8],
                "route_signature": row[9],
                "is_private": row[10],
                "is_shared": row[11],
                "is_skip_the_line": row[12],
                "is_guided": row[13],
                "is_group": row[14],
                "is_airport": row[15],
                "activity_tags": row[16],
                "supplier_operator": row[17],
                "price_min": float(row[18]) if row[18] else None,
                "price_max": float(row[19]) if row[19] else None,
                "currency": row[20],
                "embedding_sim": float(row[21]) if row[21] else 0,
            }
            for row in rows
        ]

    # ══════════════════════════════════════════════════════════
    # STRUCTURED SCORING
    # ══════════════════════════════════════════════════════════

    def _compute_structured_score(self, scraped, candidate) -> float:
        """Compute weighted structured tourism score."""

        # 1. Service type score (20%)
        service_score = 1.0 if scraped.service_type == candidate.get("service_type") else 0.0

        # 2. Route score (20%)
        route_score = 0.0
        if scraped.route_signature and candidate.get("route_signature"):
            if scraped.route_signature.lower() == candidate["route_signature"].lower():
                route_score = 1.0
            elif scraped.origin and candidate.get("origin"):
                if scraped.origin.lower() == candidate["origin"].lower():
                    route_score = 0.7
                elif scraped.destination and candidate.get("destination"):
                    if scraped.destination.lower() == candidate["destination"].lower():
                        route_score = 0.7
        elif scraped.service_type == "TRANSFER" and candidate.get("service_type") == "TRANSFER":
            route_score = 0.3  # Both transfers but no route match

        # 3. City score (15%)
        city_score = 0.0
        if scraped.normalized_city and candidate.get("normalized_city"):
            if scraped.normalized_city.lower() == candidate["normalized_city"].lower():
                city_score = 1.0
            elif scraped.normalized_country == candidate.get("normalized_country"):
                city_score = 0.5

        # 4. Theme score (15%)
        theme_score = 0.0
        if scraped.experience_family == candidate.get("experience_family"):
            theme_score = 1.0
            if scraped.service_subtype == candidate.get("service_subtype"):
                theme_score = 1.0  # Exact match
            else:
                theme_score = 0.7  # Same family, different subtype

        # 5. Flags score (10%)
        flags_score = 0.0
        flags_to_check = ["is_private", "is_shared", "is_skip_the_line", "is_guided", "is_group", "is_airport"]
        matches = 0
        for flag in flags_to_check:
            if getattr(scraped, flag, False) == candidate.get(flag, False):
                matches += 1
        flags_score = matches / len(flags_to_check) if flags_to_check else 0

        # 6. Embedding score (20%)
        embedding_score = candidate.get("embedding_sim", 0)

        # Weighted final score
        final = (
                service_score * self.WEIGHTS["service_type"] +
                route_score * self.WEIGHTS["route"] +
                city_score * self.WEIGHTS["city"] +
                theme_score * self.WEIGHTS["theme"] +
                flags_score * self.WEIGHTS["flags"] +
                embedding_score * self.WEIGHTS["embedding"]
        )

        return final

    # ══════════════════════════════════════════════════════════
    # SAVE
    # ══════════════════════════════════════════════════════════

    async def _save_match(self, session, scraped, match, status):
        obj = ProduitMatches(
            scraped_product_id=scraped.scraped_product_id,
            dex_product_id=match["dex_product_id"],
            similarity_score=match.get("final_score", 0),
            match_status=status,
            match_type="STRUCTURED_MATCH" if match.get("final_score", 0) >= 0.85 else "SEMANTIC_MATCH",
            ai_reason=f"Service: {scraped.service_type} | City: {scraped.normalized_city} | Score: {match.get('final_score', 0):.3f}",
            source_platform=scraped.source_platform,
            created_at=datetime.utcnow(),
        )
        session.add(obj)

    async def _save_new(self, session, scraped):
        obj = ProduitMatches(
            scraped_product_id=scraped.scraped_product_id,
            dex_product_id=None,
            similarity_score=0.0,
            match_status="NEW_PRODUCT",
            match_type="NEW_PRODUCT",
            ai_reason=f"No matching DEX product for {scraped.service_type} in {scraped.normalized_city}",
            source_platform=scraped.source_platform,
            created_at=datetime.utcnow(),
        )
        session.add(obj)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()