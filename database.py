"""
database.py — DEX AI Sourcing Agent
DatabaseManager: async SQLAlchemy engine + all DB operations.

Changes from old version:
  - Removed migrate_produits_table() — old columns are gone from models
  - Updated save_fournisseur() — removed mapping/zone/activite_type fields
  - Updated _PRODUIT_ALLOWED_FIELDS — matches clean Produit model
  - Updated save_produit() — no more junk fields
  - Updated get_all_fournisseurs() — removed mapping field refs
  - Updated get_fournisseurs_by_country() — uses pays_id join properly
  - Updated get_stats() — no more mapping='OK' filter
  - Added get_fournisseurs_by_run() — new utility
  - Added save_scraping_run() / update_scraping_run() — clean version
  - Added get_dex_normalized_count() — correct table name
  - JUNK_TITLES / JUNK_PATTERNS kept as-is (working correctly)
  - Zones/pays init kept exactly as-is (working correctly)
  - create_embedding_indexes() kept as-is (working correctly)
"""

import os
import re
from datetime import datetime
from typing import Dict, List, Optional

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import select, text, inspect
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from models import (
    Base,
    DexProductNormalized,
    Fournisseur,
    Pays,
    ProduitMatches,
    Produit,
    ScrapedProductNormalized,
    ScrapingRun,
    Zone,
)

load_dotenv()


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

from config import config


# ──────────────────────────────────────────────────────────────
# DatabaseManager
# ──────────────────────────────────────────────────────────────

class DatabaseManager:

    # ── Junk filter (unchanged — working correctly) ──────────
    JUNK_TITLES = {
        "error page", "página de error", "pagina di errore",
        "email protection", "activities", "inspiration",
        "national parks", "budget travel lists",
        "the best day to buy airline tickets",
    }
    JUNK_TITLE_PATTERNS = [
        r"^the \d+ best",
        r"^top \d+",
        r"things to do in \d{4}",
        r"^budget travel",
    ]

    # ── FIX 4: Junk ville values to reject ───────────────────
    # These come from _city_from_url() on navigation URLs
    JUNK_VILLES = {
        "home", "accueil", "index", "about", "contact",
        "tours", "activities", "excursions", "transfers",
        "tickets", "experiences", "products", "packages",
        "things", "visit", "trips", "blog", "news",
        "sitemap", "search", "booking", "reservation",
        # ADDED — seen in logs + from product_scraper:
        "product", "sitio", "page", "post", "item",
        "safari", "destinations", "archive", "popi act",
        "sector", "weddings", "ladies only",
    }

    def __init__(self):
        # Use the URL from config.py but force psycopg (more stable on Windows)
        db_url = config.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://")

        self.engine = create_async_engine(
            db_url,
            echo=False,
            pool_size=8,
            max_overflow=12,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=60,
        )
        self.AsyncSessionLocal = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    def _sanitize_for_postgres(self, text: str) -> str:
        """Strip NUL bytes and control chars PostgreSQL rejects."""
        if not text:
            return text
        text = text.replace("\x00", "")
        text = re.sub(r"[\x01-\x08\x0b\x0c\x0e-\x1f]", "", text)
        return text

    # ═══════════════════════════════════════════════════════════
    # TABLES & INDEXES
    # ═══════════════════════════════════════════════════════════

    async def create_tables(self):
        """Create tables + intelligently add missing columns (safe for development)."""
        logger.info("🔄 Mise à jour du schéma de la base de données...")

        async with self.engine.begin() as conn:
            # 1. Create all missing tables
            await conn.run_sync(Base.metadata.create_all)
            logger.info("✅ Tables créées / vérifiées")

            # 2. Add missing columns (with proper async handling)
            await self._add_missing_columns(conn)

        logger.info("✅ Schéma mis à jour avec succès")

    async def _add_missing_columns(self, conn):
        """Add missing columns safely using run_sync."""
        logger.info("🔍 Vérification des colonnes manquantes...")

        def sync_add_missing_columns(sync_conn):
            inspector = inspect(sync_conn)
            added = 0

            for table in Base.metadata.tables.values():
                table_name = table.name

                try:
                    existing_columns = {col['name'] for col in inspector.get_columns(table_name)}
                except Exception:
                    continue  # Table doesn't exist or other issue

                for col in table.columns:
                    if col.name not in existing_columns:
                        try:
                            col_type = col.type.compile(dialect=sync_conn.dialect)
                            sql = f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{col.name}" {col_type}'

                            if not col.nullable:
                                sql += " NOT NULL"

                            # Simple default handling
                            if col.default is not None and hasattr(col.default, 'arg'):
                                default = col.default.arg
                                if isinstance(default, (int, float, bool)):
                                    sql += f" DEFAULT {default}"
                                elif isinstance(default, str):
                                    sql += f" DEFAULT '{default}'"

                            sync_conn.execute(text(sql))
                            logger.info(f"   ➕ Ajout de colonne : {table_name}.{col.name} ({col_type})")
                            added += 1
                        except Exception as e:
                            logger.warning(f"   ⚠️ Échec ajout {table_name}.{col.name} : {e}")

            return added

        # Execute synchronously inside async context
        try:
            added_count = await conn.run_sync(sync_add_missing_columns)
            if added_count > 0:
                logger.success(f"✅ {added_count} nouvelle(s) colonne(s) ajoutée(s)")
            else:
                logger.info("✅ Aucune nouvelle colonne nécessaire")
        except Exception as e:
            logger.warning(f"⚠️ Erreur pendant la mise à jour des colonnes: {e}")

    async def create_embedding_indexes(self):
        """
        Create IVFFlat vector indexes for cosine similarity search.
        Unchanged — working correctly.
        """
        indexes = [
            """
            CREATE INDEX IF NOT EXISTS idx_scraped_embedding
                ON scraped_products_normalized
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_dex_embedding
                ON dex_products_normalized
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
            """,
        ]
        async with self.engine.begin() as conn:
            for sql in indexes:
                try:
                    await conn.execute(text(sql))
                    logger.info("✅ Vector index created")
                except Exception as e:
                    logger.warning(f"Vector index (may already exist): {e}")

    async def table_exists(self, table_name: str) -> bool:
        async with self.engine.connect() as conn:
            result = await conn.execute(text(
                "SELECT EXISTS(SELECT FROM information_schema.tables "
                "WHERE table_name = :t)"
            ), {"t": table_name})
            return result.scalar()

    # ═══════════════════════════════════════════════════════════
    # ZONES + PAYS  (unchanged — working correctly)
    # ═══════════════════════════════════════════════════════════

    async def init_zones_and_countries(self):
        """Seed zones and all countries on first run."""
        async with self.AsyncSessionLocal() as session:
            existing_zones = (await session.execute(select(Zone))).scalars().all()
            if not existing_zones:
                for z in [
                    Zone(id=1, nom="Italie",       description="Italie uniquement"),
                    Zone(id=2, nom="Europe",        description="Europe sauf Italie"),
                    Zone(id=3, nom="Amérique",      description="Amérique N+S"),
                    Zone(id=4, nom="Afrique",       description="Afrique"),
                    Zone(id=5, nom="Asie/Océanie",  description="Asie et Océanie"),
                ]:
                    session.add(z)
                await session.commit()

            existing_pays = (await session.execute(select(Pays))).scalars().all()
            if not existing_pays:
                pays_map = {
                    1: ["Italy"],
                    2: [
                        "France","Spain","Germany","United Kingdom","Portugal","Greece",
                        "Netherlands","Belgium","Switzerland","Austria","Sweden","Norway",
                        "Denmark","Finland","Ireland","Poland","Czech Republic","Hungary",
                        "Romania","Bulgaria","Croatia","Slovakia","Slovenia","Estonia",
                        "Latvia","Lithuania","Luxembourg","Malta","Iceland","Cyprus",
                        "Albania","Bosnia and Herzegovina","Montenegro","North Macedonia",
                        "Serbia","Monaco","Andorra","Liechtenstein","Vatican City",
                        "San Marino","Belarus","Moldova","Ukraine","Russia",
                    ],
                    3: [
                        "United States","Canada","Mexico","Brazil","Argentina","Chile",
                        "Peru","Colombia","Venezuela","Ecuador","Bolivia","Paraguay",
                        "Uruguay","Guyana","Suriname","French Guiana","Belize",
                        "Costa Rica","El Salvador","Guatemala","Honduras","Nicaragua",
                        "Panama","Cuba","Dominican Republic","Haiti","Jamaica",
                        "Puerto Rico","Trinidad and Tobago","Bahamas","Barbados",
                        "Grenada","Saint Lucia","Saint Kitts and Nevis",
                        "Saint Vincent and the Grenadines",
                    ],
                    4: [
                        "Morocco","Tunisia","Egypt","Algeria","Libya","Sudan",
                        "South Sudan","Mauritania","Kenya","South Africa","Nigeria",
                        "Ghana","Senegal","Ivory Coast","Cameroon","Uganda","Tanzania",
                        "Rwanda","Ethiopia","Angola","Mozambique","Zambia","Zimbabwe",
                        "Botswana","Namibia","Mauritius","Seychelles","Madagascar",
                        "Congo","DR Congo","Gabon","Benin","Burkina Faso","Malawi",
                        "Mali","Niger","Chad","Central African Republic","Sierra Leone",
                        "Liberia","Togo","Somalia","Eritrea","Djibouti","Comoros",
                        "Cabo Verde","Sao Tome and Principe","Equatorial Guinea",
                        "Eswatini","Lesotho","Burundi","Guinea","Guinea-Bissau","Gambia",
                    ],
                    5: [
                        "Thailand","Indonesia","Vietnam","Japan","India","Singapore",
                        "Malaysia","Philippines","South Korea","China","Taiwan",
                        "Hong Kong","Sri Lanka","Nepal","Cambodia","Laos","Myanmar",
                        "Bangladesh","Pakistan","Afghanistan","Maldives","Brunei",
                        "Mongolia","Australia","New Zealand","Fiji","Papua New Guinea",
                        "Solomon Islands","Vanuatu","Samoa","Tonga","Kiribati",
                        "Micronesia","Marshall Islands","Palau","Nauru","Tuvalu",
                        "New Caledonia","French Polynesia","East Timor","Bhutan",
                        "Kazakhstan","Uzbekistan","Turkmenistan","Kyrgyzstan",
                        "Tajikistan","Azerbaijan","Armenia","Georgia","Turkey",
                        "Iraq","Iran","Kuwait","Saudi Arabia","Yemen","Oman",
                        "United Arab Emirates","Qatar","Bahrain","Jordan","Lebanon",
                        "Syria","Israel","Palestine",
                    ],
                }
                for zone_id, names in pays_map.items():
                    for nom in names:
                        session.add(Pays(nom=nom, zone_id=zone_id))
                await session.commit()
                logger.info("✅ Zones + pays initialisés")

    async def get_zones(self) -> List[Dict]:
        async with self.AsyncSessionLocal() as session:
            zones = (await session.execute(select(Zone))).scalars().all()
            return [{"id": z.id, "nom": z.nom, "description": z.description}
                    for z in zones]

    async def get_countries_by_zone(self, zone_id: int) -> List[Dict]:
        async with self.AsyncSessionLocal() as session:
            pays = (await session.execute(
                select(Pays).where(Pays.zone_id == zone_id)
            )).scalars().all()
            return [{"id": c.id, "nom": c.nom} for c in pays]

    async def get_all_countries(self) -> List[Dict]:
        async with self.AsyncSessionLocal() as session:
            pays = (await session.execute(select(Pays))).scalars().all()
            return [{"id": c.id, "nom": c.nom, "zone_id": c.zone_id} for c in pays]

    async def get_existing_domains(self) -> List[str]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(text(
                "SELECT domain FROM fournisseurs "
                "WHERE domain IS NOT NULL AND domain != ''"
            ))
            return [r[0] for r in rows.fetchall()]

    # ═══════════════════════════════════════════════════════════
    # SCRAPING RUNS
    # ═══════════════════════════════════════════════════════════

    async def save_scraping_run(self, run_data: dict) -> Optional[str]:
        """
        Create a new ScrapingRun from admin form config.
        Returns run_id on success.
        """
        async with self.AsyncSessionLocal() as session:
            allowed = {
                "run_id", "countries", "required_cities", "activity_types",
                "min_suppliers", "max_suppliers", "max_products_per_supplier",
                "recall_old_suppliers",
            }
            clean = {k: v for k, v in run_data.items() if k in allowed}
            if not clean.get("run_id"):
                logger.warning("save_scraping_run: run_id manquant")
                return None
            run = ScrapingRun(**clean)
            session.add(run)
            await session.flush()
            await session.commit()
            logger.info(f"✅ ScrapingRun créé: {clean['run_id']}")
            return clean["run_id"]

    async def update_scraping_run(self, run_id: str, data: dict):
        """Update status/stats on an existing ScrapingRun."""
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(
                select(ScrapingRun).where(ScrapingRun.run_id == run_id)
            )
            run = result.scalar_one_or_none()
            if run:
                allowed = {
                    "status", "finished_at",
                    "total_suppliers_found", "total_products_scraped",
                    "total_normalized", "total_accepted_odoo",
                    "total_rejected_duplicate",
                }
                for key, value in data.items():
                    if key in allowed:
                        setattr(run, key, value)
                await session.commit()

    async def get_scraping_run(self, run_id: str) -> Optional[Dict]:
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(
                select(ScrapingRun).where(ScrapingRun.run_id == run_id)
            )
            run = result.scalar_one_or_none()
            if not run:
                return None
            return {
                "run_id":                   run.run_id,
                "countries":                run.countries,
                "required_cities":          run.required_cities,
                "activity_types":           run.activity_types,
                "min_suppliers":            run.min_suppliers,
                "max_suppliers":            run.max_suppliers,
                "max_products_per_supplier":run.max_products_per_supplier,
                "status":                   run.status,
                "started_at":               run.started_at,
                "finished_at":              run.finished_at,
                "total_suppliers_found":    run.total_suppliers_found,
                "total_products_scraped":   run.total_products_scraped,
                "total_accepted_odoo":      run.total_accepted_odoo,
                "total_rejected_duplicate": run.total_rejected_duplicate,
            }

    # ═══════════════════════════════════════════════════════════
    # FOURNISSEURS
    # ═══════════════════════════════════════════════════════════

    # Allowed fields match the clean Fournisseur model exactly
    _FOURNISSEUR_ALLOWED_FIELDS = {
        "nom", "pays_id", "ville", "domain",
        "adresse", "telephone", "rating", "nb_avis",
        "has_api", "api_name", "has_channel", "channel_name",
        "type_connexion", "score", "status", "run_id",
        "is_marketplace", "marketplace_type",  # ← ADDED
    }

    async def save_fournisseur(self, data: dict) -> Optional[int]:
        """
        FIX 1 — CRITICAL:
        Old code had: if mapping not in valid_mappings → return None
        This silently blocked ALL new suppliers from being saved because
        new fournisseur_scraper.py never sets 'mapping' field.

        New code: no mapping gate. Any supplier with domain + nom is saved.
        Only requirement: must have domain (dedup key).
        """
        async with self.AsyncSessionLocal() as session:
            if not data.get("domain"):
                logger.warning("save_fournisseur: rejeté — domain manquant")
                return None
            if not data.get("nom"):
                logger.warning("save_fournisseur: rejeté — nom manquant")
                return None

            clean = {k: v for k, v in data.items()
                     if k in self._FOURNISSEUR_ALLOWED_FIELDS}
            domain = clean["domain"]

            # Dedup by domain
            existing = (await session.execute(
                select(Fournisseur).where(Fournisseur.domain == domain)
            )).scalar_one_or_none()
            if existing:
                # Update score if new one is higher
                if clean.get("score", 0) > (existing.score or 0):
                    existing.score = clean["score"]
                    existing.updated_at = datetime.utcnow()
                    await session.commit()
                logger.debug(f"  ⏭ Fournisseur exists: {domain}")
                return existing.id

            # Dedup by nom + pays_id
            if clean.get("nom") and clean.get("pays_id"):
                existing = (await session.execute(
                    select(Fournisseur).where(
                        (Fournisseur.nom == clean["nom"]) &
                        (Fournisseur.pays_id == clean["pays_id"])
                    )
                )).scalar_one_or_none()
                if existing:
                    return existing.id
            marketplace_tag = (
                f" [🏪 {clean.get('marketplace_type', '')}]"
                if clean.get("is_marketplace") else ""
            )

            f = Fournisseur(**clean)
            session.add(f)
            await session.flush()
            await session.commit()
            logger.info(
                f"  ✅ Fournisseur saved id={f.id} | "
                f"{clean['nom'][:50]} | score={clean.get('score', 0)} | "
                f"{domain[:60]}"
            )
            return f.id

    async def get_all_fournisseurs(self, limit: int = 5000) -> List[Dict]:
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(select(Fournisseur).limit(limit))
            fournisseurs = result.scalars().all()
            return [
                {
                    "id":             f.id,
                    "nom":            f.nom,
                    "pays_id":        f.pays_id,
                    "ville":          f.ville,
                    "domain":         f.domain,
                    "score":          f.score,
                    "status":         f.status,
                    "has_api":        f.has_api,
                    "has_channel":    f.has_channel,
                    "type_connexion": f.type_connexion,
                }
                for f in fournisseurs
            ]

    async def get_fournisseurs_by_country(
        self, country_name: str, limit: int = 5000
    ) -> List[Dict]:
        """
        Get suppliers by country name (via pays join).
        CHANGED: now uses pays_id → Pays.nom join instead of raw string column.
        """
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(
                select(Fournisseur)
                .join(Pays, Fournisseur.pays_id == Pays.id)
                .where(Pays.nom.ilike(f"%{country_name}%"))
                .limit(limit)
            )
            fournisseurs = result.scalars().all()
            return [
                {
                    "id":             f.id,
                    "nom":            f.nom,
                    "pays_id":        f.pays_id,
                    "domain":         f.domain,
                    "score":          f.score,
                    "status":         f.status,
                    "has_api":        f.has_api,
                    "has_channel":    f.has_channel,
                    "type_connexion": f.type_connexion,
                }
                for f in fournisseurs
            ]

    async def get_fournisseurs_by_run(self, run_id: str) -> List[Dict]:
        """Get all suppliers discovered in a specific run."""
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(
                select(Fournisseur)
                .where(Fournisseur.run_id == run_id)
                .order_by(Fournisseur.score.desc())
            )
            fournisseurs = result.scalars().all()
            return [
                {
                    "id":             f.id,
                    "nom":            f.nom,
                    "pays_id":        f.pays_id,
                    "ville":          f.ville,
                    "domain":         f.domain,
                    "score":          f.score,
                    "status":         f.status,
                    "has_api":        f.has_api,
                    "has_channel":    f.has_channel,
                    "type_connexion": f.type_connexion,
                }
                for f in fournisseurs
            ]

    async def get_fournisseurs_connectables(self, limit: int = 5000) -> List[Dict]:
        """Get suppliers with API or Channel Manager."""
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(
                select(Fournisseur)
                .where(
                    (Fournisseur.has_api == True) |
                    (Fournisseur.has_channel == True)
                )
                .order_by(Fournisseur.score.desc())
                .limit(limit)
            )
            fournisseurs = result.scalars().all()
            return [
                {
                    "id":             f.id,
                    "nom":            f.nom,
                    "pays_id":        f.pays_id,
                    "domain":         f.domain,
                    "score":          f.score,
                    "has_api":        f.has_api,
                    "api_name":       f.api_name,
                    "has_channel":    f.has_channel,
                    "channel_name":   f.channel_name,
                    "type_connexion": f.type_connexion,
                }
                for f in fournisseurs
            ]

    async def update_fournisseur_status(self, fournisseur_id: int, status: str):
        """Update supplier pipeline status."""
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(
                select(Fournisseur).where(Fournisseur.id == fournisseur_id)
            )
            f = result.scalar_one_or_none()
            if f:
                f.status = status
                f.updated_at = datetime.utcnow()
                await session.commit()

    # ═══════════════════════════════════════════════════════════
    # PRODUITS — RAW INGESTION
    # ═══════════════════════════════════════════════════════════

    # Clean allowed fields — matches the clean Produit model exactly
    _PRODUIT_ALLOWED_FIELDS = {
        "fournisseur_id",
        "run_id",
        "nom_produit",
        "description",
        "prix",
        "devise",
        "duree",
        "categorie_raw",
        "ville_raw",
        "pays_raw",
        "source",
        "url_source",
        "booking_url",
        "image_url",
    }

    async def save_produit(self, produit_data: dict) -> dict:
        """
        Validate + clean product dict before batch insert.
        Returns clean dict OR {"status": "rejected", "id": None}.
        No DB call — batch handles insert + commit.
        """
        clean = {
            k: v for k, v in produit_data.items()
            if k in self._PRODUIT_ALLOWED_FIELDS and not k.startswith("_")
        }

        if not clean.get("nom_produit"):
            return {"status": "rejected", "id": None}

        nom = (clean["nom_produit"] or "").strip()
        if len(nom) < 8:
            return {"status": "rejected", "id": None}

        nom_lower = nom.lower().strip()
        if nom_lower in self.JUNK_TITLES:
            return {"status": "rejected", "id": None}
        for pat in self.JUNK_TITLE_PATTERNS:
            if re.search(pat, nom_lower):
                return {"status": "rejected", "id": None}

        # ADDED — single-word titles = category pages not real products
        # "Safari", "Weddings", "Experiences" all rejected here
        if len(nom.split()) < 2:
            return {"status": "rejected", "id": None}

        # Junk ville_raw exact match
        ville_raw = (clean.get("ville_raw") or "").strip().lower()
        if ville_raw in self.JUNK_VILLES:
            clean["ville_raw"] = None

        # ADDED — ville_raw > 4 words = product name not city
        # e.g. "Tsitsikamma National Park Day Tour" → NULL
        if clean.get("ville_raw") and len(str(clean["ville_raw"]).split()) > 4:
            logger.debug(f"  🏙 ville_raw too long: '{clean['ville_raw']}' → NULL")
            clean["ville_raw"] = None

        # ADDED — pays_raw empty string → None (never store "")
        if not clean.get("pays_raw"):
            clean["pays_raw"] = None

        return clean   # Return clean dict for batch to handle

    async def save_produits_batch(self, products: List[Dict]) -> Dict:
        """
        FIX: Each product in its own session + savepoint.
        First DataError (NUL bytes) no longer rolls back entire batch.
        NUL bytes sanitized before insert.
        """
        extracted = len(products)
        saved = skipped = rejected = errors = 0

        logger.info(f"💾 save_produits_batch: {extracted} products")

        # Preload dedup sets ONCE
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                text("SELECT url_source FROM produits WHERE url_source IS NOT NULL")
            )
            existing_urls: set = {r[0] for r in rows.fetchall()}
            rows2 = await conn.execute(
                text("SELECT nom_produit, fournisseur_id FROM produits")
            )
            existing_name_sup: set = {(r[0], r[1]) for r in rows2.fetchall()}

        for i, p in enumerate(products, 1):
            try:
                # Validate + clean
                clean = await self.save_produit(p)
                if isinstance(clean, dict) and clean.get("status") == "rejected":
                    rejected += 1
                    continue

                nom = (clean.get("nom_produit") or "").strip()
                url = clean.get("url_source")
                fid = clean.get("fournisseur_id")

                # Dedup
                if url and url in existing_urls:
                    skipped += 1
                    continue
                if nom and (nom, fid) in existing_name_sup:
                    skipped += 1
                    continue

                # Sanitize ALL string fields — prevent NUL byte errors
                for field in ["nom_produit", "description", "ville_raw", "pays_raw",
                              "categorie_raw", "duree", "url_source", "booking_url", "image_url"]:
                    if clean.get(field):
                        clean[field] = self._sanitize_for_postgres(str(clean[field]))

                # Re-check nom after sanitization
                nom = (clean.get("nom_produit") or "").strip()
                if not nom or len(nom) < 8:
                    rejected += 1
                    continue

                # Each product gets its OWN session — first error cannot cascade
                async with self.AsyncSessionLocal() as session:
                    try:
                        produit = Produit(**clean)
                        session.add(produit)
                        await session.flush()
                        await session.commit()

                        if url: existing_urls.add(url)
                        if nom: existing_name_sup.add((nom, fid))

                        saved += 1
                        logger.info(
                            f"  ✅ id={produit.id} | {nom[:50]} | "
                            f"prix={clean.get('prix')} | "
                            f"ville_raw={clean.get('ville_raw')} | "
                            f"pays_raw={clean.get('pays_raw')}"
                        )

                    except Exception as e:
                        await session.rollback()
                        errors += 1
                        logger.error(f"  ❌ Product #{i} '{nom[:40]}': {e}")

            except Exception as e:
                errors += 1
                logger.error(f"  ❌ Product #{i} outer: {e}")

        logger.info(
            f"📦 Batch done — extracted={extracted} | ✅ saved={saved} | "
            f"⏭ skipped={skipped} | 🚫 rejected={rejected} | ❌ errors={errors}"
        )
        return {
            "extracted": extracted, "saved": saved,
            "skipped": skipped, "rejected": rejected, "errors": errors,
        }

    async def get_produits(self, limit: int = 10000) -> List[Dict]:
        async with self.AsyncSessionLocal() as session:
            rows = (await session.execute(
                select(Produit).limit(limit)
            )).scalars().all()
            return [
                {
                    "id":             p.id,
                    "fournisseur_id": p.fournisseur_id,
                    "run_id":         p.run_id,
                    "nom_produit":    p.nom_produit,
                    "ville_raw":      p.ville_raw,
                    "pays_raw":       p.pays_raw,
                    "categorie_raw":  p.categorie_raw,
                    "prix":           float(p.prix) if p.prix else None,
                    "devise":         p.devise,
                    "source":         p.source,
                    "url_source":     p.url_source,
                    "image_url":      p.image_url,
                    "booking_url":    p.booking_url,
                    "created_at":     p.created_at,
                }
                for p in rows
            ]

    async def get_produits_by_run(self, run_id: str) -> List[Dict]:
        async with self.AsyncSessionLocal() as session:
            rows = (await session.execute(
                select(Produit).where(Produit.run_id == run_id)
            )).scalars().all()
            return [
                {
                    "id":             p.id,
                    "fournisseur_id": p.fournisseur_id,
                    "nom_produit":    p.nom_produit,
                    "ville_raw":      p.ville_raw,
                    "pays_raw":       p.pays_raw,
                    "prix":           float(p.prix) if p.prix else None,
                    "devise":         p.devise,
                    "url_source":     p.url_source,
                }
                for p in rows
            ]

    async def get_product_ids_by_run_id(self, run_id: str) -> List[int]:
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text("SELECT id FROM produits WHERE run_id = :run_id ORDER BY id"),
                {"run_id": run_id}
            )
            return [row[0] for row in result.fetchall()]

    # ═══════════════════════════════════════════════════════════
    # DEX — READ ONLY
    # ═══════════════════════════════════════════════════════════

    async def get_dex_count(self) -> int:
        async with self.engine.connect() as conn:
            try:
                return (await conn.execute(
                    text("SELECT COUNT(*) FROM dex_produit")
                )).scalar() or 0
            except Exception:
                return 0

    async def get_dex_normalized_count(self) -> int:
        async with self.engine.connect() as conn:
            try:
                return (await conn.execute(
                    text("SELECT COUNT(*) FROM dex_products_normalized")
                )).scalar() or 0
            except Exception:
                return 0

    async def get_dex_products_batch(
        self, limit: int = 10000, offset: int = 0,
        only_vendable: bool = False
    ) -> list:
        """
        Read DEX products for normalization pipeline.
        CHANGED: correct table name (dex_produit not produit_dex).
        Added only_vendable filter (2 289 vendable products).
        """
        where = "WHERE vendable = 1" if only_vendable else ""
        async with self.engine.connect() as conn:
            result = await conn.execute(text(
                f"SELECT id, nom, nomen, description, typeprestation, "
                f"prixappel, prixappelachat, devise, actif, vendable, "
                f"idville, idlocalitedepart, idlocalitearrivee, "
                f"idfournisseur, organisateur, dureeheure, dureeminute, "
                f"enpromo, prive, b2c "
                f"FROM dex_produit {where} "
                f"ORDER BY id LIMIT :limit OFFSET :offset"
            ), {"limit": limit, "offset": offset})
            return result.fetchall()

    async def get_dex_localite(self, localite_id: int) -> Optional[Dict]:
        """Resolve a localite id to name + geo coordinates."""
        async with self.engine.connect() as conn:
            result = await conn.execute(text(
                "SELECT id, nom, nomen, typelocalite, latitude, longitude, codeiso "
                "FROM dex_localite WHERE id = :id"
            ), {"id": localite_id})
            row = result.fetchone()
            if not row:
                return None
            return {
                "id":           row[0],
                "nom":          row[1],
                "nomen":        row[2],
                "typelocalite": row[3],
                "latitude":     float(row[4]) if row[4] else None,
                "longitude":    float(row[5]) if row[5] else None,
                "codeiso":      row[6],
            }

    async def get_dex_detail(self, produit_id: int, lang_id: int = 44) -> Optional[Dict]:
        """
        Get editorial content for a DEX product.
        lang_id 44 = French (default), 45 = English.
        Returns chapeau + texte (description longue).
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(
                "SELECT titre, texte, chapeau FROM dex_detail "
                "WHERE idobjet = :pid AND idlangue = :lid "
                "ORDER BY ordre LIMIT 1"
            ), {"pid": produit_id, "lid": lang_id})
            row = result.fetchone()
            if not row:
                return None
            return {
                "titre":   row[0],
                "texte":   row[1],
                "chapeau": row[2],
            }

    # ═══════════════════════════════════════════════════════════
    # STATS
    # ═══════════════════════════════════════════════════════════

    async def get_stats(self) -> dict:
        """Global pipeline stats for dashboard."""
        async with self.engine.connect() as conn:
            f_total = (await conn.execute(
                text("SELECT COUNT(*) FROM fournisseurs")
            )).scalar() or 0

            f_connectable = (await conn.execute(
                text(
                    "SELECT COUNT(*) FROM fournisseurs "
                    "WHERE has_api = true OR has_channel = true"
                )
            )).scalar() or 0

            p_total = (await conn.execute(
                text("SELECT COUNT(*) FROM produits")
            )).scalar() or 0

            try:
                p_norm = (await conn.execute(
                    text("SELECT COUNT(*) FROM scraped_products_normalized")
                )).scalar() or 0
            except Exception:
                p_norm = 0

            try:
                accepted = (await conn.execute(
                    text(
                        "SELECT COUNT(*) FROM produit_matches "
                        "WHERE match_status = 'ACCEPTED'"
                    )
                )).scalar() or 0
            except Exception:
                accepted = 0

            return {
                "fournisseurs": f_total,
                "fournisseurs_connectables": f_connectable,
                "produits_raw": p_total,
                "produits_normalises": p_norm,
                "matches_accepted": accepted,
            }


    async def get_produits_by_fournisseur(
        self, fournisseur_id: int, limit: int = 100
    ) -> list:
        async with self.AsyncSessionLocal() as session:
            rows = (await session.execute(
                select(Produit)
                .where(Produit.fournisseur_id == fournisseur_id)
                .limit(limit)
            )).scalars().all()
            return [
                {
                    "id":            p.id,
                    "nom_produit":   p.nom_produit,
                    "ville_raw":     p.ville_raw,       # ← RENAMED
                    "pays_raw":      p.pays_raw,         # ← RENAMED
                    "categorie_raw": p.categorie_raw,    # ← RENAMED
                    "prix":          float(p.prix) if p.prix else None,
                    "devise":        p.devise,
                    "source":        p.source,
                    "url_source":    p.url_source,
                    "created_at":    p.created_at,
                }
                for p in rows
            ]


    async def get_session(self):
        """Return an async session context manager."""
        return self.AsyncSessionLocal()