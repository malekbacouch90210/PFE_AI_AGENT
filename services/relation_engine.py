"""
Relation Engine — Semantic matching between suppliers and products
This is the INTELLIGENT part of your system.
"""

from typing import List, Dict
from loguru import logger
from sqlalchemy import text

from database import DatabaseManager
from services.ai.embedding_service import EmbeddingService


class RelationEngine:
    """
    Matches products to suppliers using semantic similarity.
    This replaces complex ownership detection.
    """

    def __init__(self):
        self.db = DatabaseManager()
        self.embedding_service = EmbeddingService()

        # Weights for hybrid scoring
        self.weights = {
            "embedding": 0.65,
            "city": 0.20,
            "category": 0.15,
        }

        self.thresholds = {
            "high_match": 0.85,  # Direct match
            "medium_match": 0.70,  # Related
            "low_match": 0.55,  # Weak relation
        }

    async def compute_similarity(self, supplier: Dict, product: Dict) -> float:
        """
        Compute similarity score between a supplier and a product.
        Returns score between 0 and 1.
        """
        score = 0.0

        # 1. City similarity
        supplier_city = (supplier.get("ville") or "").lower()
        product_city = (product.get("ville") or "").lower()

        if supplier_city and product_city:
            if supplier_city == product_city:
                score += self.weights["city"]
            elif supplier_city in product_city or product_city in supplier_city:
                score += self.weights["city"] * 0.7

        # 2. Category similarity
        supplier_cat = (supplier.get("activite_type") or "").lower()
        product_cat = (product.get("activite_type") or "").lower()

        if supplier_cat and product_cat:
            if supplier_cat == product_cat:
                score += self.weights["category"]
            elif supplier_cat in product_cat or product_cat in supplier_cat:
                score += self.weights["category"] * 0.6

        # 3. Embedding similarity (requires embeddings)
        supplier_text = self._build_supplier_text(supplier)
        product_text = self._build_product_text(product)

        if supplier_text and product_text:
            supplier_emb = await self.embedding_service.generate_embedding(supplier_text)
            product_emb = await self.embedding_service.generate_embedding(product_text)

            if supplier_emb and product_emb:
                emb_score = self._cosine_similarity(supplier_emb, product_emb)
                score += emb_score * self.weights["embedding"]

        return min(score, 1.0)

    def _build_supplier_text(self, supplier: Dict) -> str:
        """Build text representation for supplier."""
        parts = []

        name = supplier.get("nom", "")
        if name:
            parts.append(name)

        city = supplier.get("ville", "")
        if city:
            parts.append(f"located in {city}")

        activity = supplier.get("activite_type", "")
        if activity:
            parts.append(f"offering {activity}")

        # Add channel manager info if available
        if supplier.get("has_channel"):
            parts.append("online booking available")

        return " ".join(parts)[:500]

    def _build_product_text(self, product: Dict) -> str:
        """Build text representation for product."""
        parts = []

        title = product.get("nom_produit", "")
        if title:
            parts.append(title)

        desc = product.get("description", "")
        if desc:
            parts.append(desc[:200])

        city = product.get("ville", "")
        if city:
            parts.append(f"in {city}")

        return " ".join(parts)[:500]

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if not a or not b:
            return 0.0

        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(y * y for y in b) ** 0.5

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot / (norm_a * norm_b)

    async def match_products_to_suppliers(self, batch_size: int = 100):
        """
        Match unmatched products to suppliers using semantic similarity.
        Updates fournisseur_id in produits table.
        """
        logger.info("🔗 Starting supplier-product matching...")

        async with self.db.AsyncSessionLocal() as session:
            # Get all suppliers
            suppliers_result = await session.execute(
                text("SELECT id, nom, ville, activite_type, has_channel FROM fournisseurs")
            )
            suppliers = suppliers_result.fetchall()

            if not suppliers:
                logger.warning("No suppliers found")
                return

            # Get products without supplier_id
            products_result = await session.execute(
                text(
                    "SELECT id, nom_produit, description, ville, activite_type FROM produits WHERE fournisseur_id IS NULL")
            )
            products = products_result.fetchall()

            logger.info(f"📊 {len(suppliers)} suppliers, {len(products)} products to match")

            matches = 0
            for product in products:
                best_match = None
                best_score = 0.0

                for supplier in suppliers:
                    # Create dict representations
                    supplier_dict = {
                        "nom": supplier[1],
                        "ville": supplier[2],
                        "activite_type": supplier[3],
                        "has_channel": supplier[4],
                    }
                    product_dict = {
                        "nom_produit": product[1],
                        "description": product[2],
                        "ville": product[3],
                        "activite_type": product[4],
                    }

                    score = await self.compute_similarity(supplier_dict, product_dict)

                    if score > best_score:
                        best_score = score
                        best_match = supplier[0]

                # Apply threshold
                if best_score >= self.thresholds["low_match"]:
                    await session.execute(
                        text("UPDATE produits SET fournisseur_id = :sid, relation_score = :score WHERE id = :pid"),
                        {"sid": best_match, "score": best_score, "pid": product[0]}
                    )
                    matches += 1

                    if matches % 50 == 0:
                        await session.commit()
                        logger.info(f"  ✅ Matched {matches} products so far...")

            await session.commit()

        logger.info(f"✅ Finished! Matched {matches} products to suppliers")

    async def close(self):
        await self.embedding_service.close()


# Singleton
_relation_engine = None


def get_relation_engine() -> RelationEngine:
    global _relation_engine
    if _relation_engine is None:
        _relation_engine = RelationEngine()
    return _relation_engine