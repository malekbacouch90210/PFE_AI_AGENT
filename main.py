# ========================================================
# MUST BE AT THE VERY TOP — BEFORE ANY OTHER IMPORTS
# ========================================================
import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# ========================================================

import asyncio
from loguru import logger
from database import DatabaseManager
from sqlalchemy import text


class DatabaseInitializer:
    def __init__(self):
        self.db = DatabaseManager()

    async def init(self):
        logger.info("=" * 60)
        logger.info("🚀 DEX AI Sourcing Agent - Database Initializer")
        logger.info("=" * 60)

        try:
            # 1. Schema synchronization (tables + columns)
            await self.db.create_tables()

            # 2. Core reference data
            await self.db.init_zones_and_countries()

            # 3. Cities import
            await self.import_cities()

            # 4. DEX validation
            await self.check_dex_tables()

            logger.info("=" * 60)
            logger.success("✅ INITIALISATION TERMINÉE AVEC SUCCÈS !")
            logger.info("=" * 60)

        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"❌ ÉCHEC CRITIQUE DE L'INITIALISATION: {e}")
            logger.error("=" * 60)
            raise
        finally:
            if hasattr(self.db, 'engine'):
                await self.db.engine.dispose()
                logger.info("🔌 Connexions à la base de données fermées")

    async def import_cities(self):
        """Import cities from JSON file"""
        try:
            logger.info("🌍 Début de l'import des villes depuis cities.json...")

            if hasattr(self.db, 'import_cities_from_json'):
                await self.db.import_cities_from_json("cities.json")
                logger.success("✅ Import des villes terminé")
            else:
                logger.warning("⚠️ Méthode import_cities_from_json() non implémentée dans DatabaseManager")

        except FileNotFoundError:
            logger.error("❌ Fichier cities.json introuvable")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'import des villes: {e}")

    async def check_dex_tables(self):
        """Validate DEX reference tables"""
        try:
            if await self.db.table_exists('dex_produit'):
                count = await self.db.get_dex_count()
                logger.success(f"📚 dex_produit → {count:,} produits trouvés")
            else:
                logger.warning("⚠️ Table 'dex_produit' non trouvée (import DEX manquant ?)")

            # Optional: check normalized table
            if await self.db.table_exists('dex_products_normalized'):
                norm_count = await self.db.get_dex_normalized_count()
                logger.info(f"📊 dex_products_normalized → {norm_count:,} enregistrements")

        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la vérification DEX: {e}")


async def main():
    initializer = DatabaseInitializer()
    await initializer.init()


if __name__ == "__main__":
    asyncio.run(main())