"""
models.py — DEX AI Sourcing Agent
Clean version: only the 6 pipeline tables are modified.
All DEX import tables (dex_produit, dex_localite, etc.) are UNTOUCHED.
"""

from datetime import datetime
from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    Column, Integer, String, Float, Text, DateTime,
    Boolean, ForeignKey, Numeric, Date, Time, Index,
    BigInteger, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# ──────────────────────────────────────────────
# GEO LAYER  (unchanged)
# ──────────────────────────────────────────────

class Zone(Base):
    """5 geographic zones for DEX operational structure."""
    __tablename__ = 'zones'

    id          = Column(Integer, primary_key=True)
    nom         = Column(String(100), nullable=False, unique=True)
    description = Column(Text)
    created_at  = Column(DateTime, default=datetime.utcnow)


class Pays(Base):
    """195 countries, each linked to a zone."""
    __tablename__ = 'pays'

    id      = Column(Integer, primary_key=True)
    nom     = Column(String(100), nullable=False, unique=True)
    zone_id = Column(Integer, ForeignKey('zones.id'), index=True)


class Ville(Base):
    """Cities per country — loaded from cities.json."""
    __tablename__ = 'villes'

    id         = Column(BigInteger, primary_key=True, autoincrement=True)
    name       = Column(String(200), nullable=False)
    pays_id    = Column(Integer, ForeignKey('pays.id', ondelete='CASCADE'), nullable=False)
    state_code = Column(String(50))
    latitude   = Column(Float)
    longitude  = Column(Float)
    timezone   = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)


# ──────────────────────────────────────────────
# PIPELINE LAYER 0 — SCRAPING RUN CONTROL
# ──────────────────────────────────────────────

class ScrapingRun(Base):
    """
    One record per sourcing job launched from admin form.
    min/max stored for traceability — they apply PER COUNTRY.
    """
    __tablename__ = 'scraping_runs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), unique=True, index=True, nullable=False)
    countries = Column(JSONB)  # list of country names
    required_cities = Column(JSONB)  # {country: [city, ...]}
    activity_types = Column(JSONB)  # ["excursion","ticket","transfer"]
    min_suppliers = Column(Integer)  # per country
    max_suppliers = Column(Integer)  # per country
    max_products_per_supplier = Column(Integer)
    recall_old_suppliers = Column(Boolean, default=False)
    status = Column(String(20), default='RUNNING')
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    total_suppliers_found = Column(Integer, default=0)
    total_products_scraped = Column(Integer, default=0)
    total_normalized = Column(Integer, default=0)
    total_accepted_odoo = Column(Integer, default=0)
    total_rejected_duplicate = Column(Integer, default=0)


# ─────────────────────────────────────────────────────────────
# FOURNISSEURS
# ─────────────────────────────────────────────────────────────
class Fournisseur(Base):
    """
    Travel suppliers discovered via Scrappa.co + Google Maps.
    One record per unique domain.

    is_marketplace=True → OTA false-supplier (Viator/GYG/Klook page)
    type_connexion: NONE | API | CHANNEL | BOTH | MARKETPLACE
    status: discovered → qualified → scraping_pending → scraped | rejected
    """
    __tablename__ = 'fournisseurs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nom = Column(String(255), nullable=False)
    pays_id = Column(Integer, ForeignKey('pays.id'), index=True)
    ville = Column(String(255), nullable=True)  # where SUPPLIER operates (not products)
    adresse = Column(Text)
    telephone = Column(String(50))
    domain = Column(String(500), unique=True, index=True)
    rating = Column(Numeric(3, 2))
    nb_avis = Column(Integer)
    score = Column(Integer, default=0)
    # CHANGED: 'discovered' is more accurate initial state than 'qualified'
    status = Column(String(30), default='discovered', index=True)
    run_id = Column(String(36), index=True)
    has_api = Column(Boolean, default=False)
    api_name = Column(String(100))
    has_channel = Column(Boolean, default=False)
    channel_name = Column(String(100))
    type_connexion = Column(String(20), default='NONE')
    is_marketplace = Column(Boolean, default=False)
    marketplace_type = Column(String(50))  # "viator"|"getyourguide"|"klook"|None
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    pays_rel = relationship("Pays", backref="fournisseurs")


# ─────────────────────────────────────────────────────────────
# PRODUITS  (raw dirty staging)
# ─────────────────────────────────────────────────────────────
class Produit(Base):
    """
    Raw scraped products — dirty staging table.
    No AI processing here.

    ville_raw: NULL unless verified against villes table (Sprint 3)
    pays_raw:  NULL unless extracted from product URL/title/desc (never from supplier)
    Sprint 4 normalizes both via LLM + DB validation.
    """
    __tablename__ = 'produits'
    id = Column(Integer, primary_key=True, autoincrement=True)
    fournisseur_id = Column(Integer, ForeignKey('fournisseurs.id'), index=True)
    run_id = Column(String(36), index=True)
    nom_produit = Column(String(500), nullable=False)
    description = Column(Text)
    prix = Column(Numeric(10, 2))
    devise = Column(String(10), default='EUR')
    duree = Column(String(50))
    categorie_raw = Column(String(100))  # optional weak signal
    ville_raw = Column(String(150), index=True)  # verified or NULL
    pays_raw = Column(String(100), index=True)  # from content or NULL
    source = Column(String(50))  # crawl4ai|jsonld|bs4|playwright
    url_source = Column(String(1000))
    booking_url = Column(String(1000))
    image_url = Column(String(1000))
    # REMOVED: date_scrap (created_at is sufficient)
    created_at = Column(DateTime, default=datetime.utcnow)
    fournisseur = relationship("Fournisseur", backref="produits")
    __table_args__ = (
        UniqueConstraint('url_source', name='uq_produit_url_source'),
        Index('idx_produit_fournisseur', 'fournisseur_id'),
        Index('idx_produit_url_source', 'url_source'),
        Index('idx_produit_run_pays', 'run_id', 'pays_raw'),
    )


# ─────────────────────────────────────────────────────────────
# SCRAPED PRODUCTS NORMALIZED  (AI clean layer — Sprint 4)
# ─────────────────────────────────────────────────────────────
class ScrapedProductNormalized(Base):
    """
    AI-processed version of a scraped product.

    FINAL decisions vs Doc 15 vs Doc 16:
      + location_confidence + location_sources  ← from Doc 16 (needed)
      + embedding_text                          ← from Doc 16 (debugging)
      + marketplace_source                      ← from Doc 16 (traceability)
      - service_type / service_subtype          ← dropped (adds complexity, low value for PFE)
      - is_private/is_shared/etc.               ← dropped (derivable from activity_tags JSONB)
    """
    __tablename__ = 'scraped_products_normalized'
    id = Column(Integer, primary_key=True, autoincrement=True)
    scraped_product_id = Column(Integer, ForeignKey('produits.id'), unique=True, index=True)
    fournisseur_id = Column(Integer, ForeignKey('fournisseurs.id'), index=True)
    run_id = Column(String(36), index=True)

    # ── Text ──────────────────────────────────────────────────
    clean_title = Column(Text)
    clean_description = Column(Text)

    # ── Geo — validated against villes/pays tables ────────────
    normalized_city = Column(String(150), index=True)
    normalized_country = Column(String(100), index=True)
    geo_lat = Column(Float)
    geo_lng = Column(Float)
    # How city/country was determined and how sure we are
    location_confidence = Column(Float, default=0.0)  # 0.0 → 1.0
    location_sources = Column(JSONB)
    # e.g. ["llm_reasoning", "ville_raw"] or ["title", "url_slug"]

    # ── Business type ─────────────────────────────────────────
    canonical_activity_type = Column(String(20), index=True)
    # EXCURSION | TICKET | TRANSFER — THE main matching filter

    # ── Category ─────────────────────────────────────────────
    normalized_category = Column(String(150), index=True)
    # DEX canonical: CULTUREL|AVENTURE|NATURE|TRANSPORT|etc.

    # ── Transfer routing ─────────────────────────────────────
    origin = Column(String(255))
    destination = Column(String(255))
    route_signature = Column(String(300), index=True)  # "City_A → City_B"

    # ── Price ─────────────────────────────────────────────────
    estimated_price = Column(Numeric(10, 2))  # always EUR
    estimated_currency = Column(String(10), default='EUR')
    price_is_estimated = Column(Boolean, default=False)  # True = AI guessed

    # ── Duration ─────────────────────────────────────────────
    duration_minutes = Column(Integer)

    # ── Tags — replaces all is_* booleans ────────────────────
    activity_tags = Column(JSONB)
    # {"skip_the_line": true, "guided": true, "private": false, ...}

    # ── Embedding + dedup ────────────────────────────────────
    embedding = Column(Vector(1024))
    embedding_text = Column(Text)  # what was embedded (debug)
    matching_signature = Column(String(500), index=True)
    # city|activity_type|title_keywords — fast pre-filter before cosine

    # ── Quality metadata ─────────────────────────────────────
    classification_confidence = Column(Numeric(3, 2), default=0.0)
    normalization_confidence = Column(Float, default=0.0)

    # ── Media + source ────────────────────────────────────────
    image_url = Column(String(1000))
    marketplace_source = Column(String(100))  # "viator"|"getyourguide"|None

    created_at = Column(DateTime, default=datetime.utcnow)
    produit = relationship("Produit", backref="normalized")

    __table_args__ = (
        Index('idx_spn_city_type', 'normalized_city', 'canonical_activity_type'),
        Index('idx_spn_signature', 'matching_signature'),
    )


# ─────────────────────────────────────────────────────────────
# PRODUIT MATCHES  (Sprint 5 — matching engine)
# ─────────────────────────────────────────────────────────────
class ProduitMatches(Base):
    """
    Similarity between scraped normalized product and existing DEX product.

    FINAL decisions:
      - estimated_missing_price REMOVED (belongs in ScrapedProductNormalized)
      - source_url REMOVED (already in Produit.url_source)
      + odoo_product_id ADDED (Sprint 5 — filled after successful Odoo insert)
    """
    __tablename__ = 'produit_matches'
    id = Column(Integer, primary_key=True, autoincrement=True)
    scraped_product_id = Column(Integer, ForeignKey('produits.id'), index=True)
    dex_product_id = Column(Integer, ForeignKey('dex_produit.id'), index=True)
    run_id = Column(String(36), index=True)

    # 5-dimension similarity scores
    similarity_score = Column(Numeric(5, 4), index=True)  # final combined
    title_score = Column(Numeric(5, 4))  # 40% weight
    city_score = Column(Numeric(5, 4))  # 20% weight
    description_score = Column(Numeric(5, 4))  # 15% weight
    category_score = Column(Numeric(5, 4))  # 15% weight
    price_score = Column(Numeric(5, 4))  # 10% weight
    supplier_score = Column(Numeric(5, 4))  # soft signal

    # Decision
    match_status = Column(String(30), index=True)
    # ACCEPTED | REJECTED_DUPLICATE | PENDING_REVIEW | REJECTED_MANUAL
    match_type = Column(String(20))
    # AUTO_ACCEPT | AUTO_REJECT | MANUAL
    ai_reason = Column(Text)  # LLM explanation for jury demo

    # Sprint 5 — Odoo sync result
    odoo_product_id = Column(Integer)  # filled after successful Odoo insert

    created_at = Column(DateTime, default=datetime.utcnow)
    scraped_produit = relationship("Produit", foreign_keys=[scraped_product_id])
    dex_produit = relationship("DexProduit", foreign_keys=[dex_product_id])
# ──────────────────────────────────────────────
# DEX NORMALIZED LAYER  (reference for matching)
# ──────────────────────────────────────────────

class DexProductNormalized(Base):
    """
    Normalized + embedded version of existing DEX products.
    Built by rebuild_dex_normalized.py from:
      dex_produit + dex_localite + dex_detail

    COLUMN DECISIONS (based on Excel null analysis):
      - chapeau         REMOVED (100% null in detail table — dead)
      - service_subtype REMOVED (sousTypePrestation 73% null — too sparse)
      - origin/dest     KEPT but sparse (transfers only, 0.4-0.6% populated)
      - country_iso     REMOVED (codeISO 100% null in localite — dead)
                        → use localite.nomEN for English country name instead
      - city_confidence KEPT — quality signal for matching engine

    Geo source priority (dex_produit.longitude/latitude are 100% DEAD):
      1. dex_localite via idVille → lat/lng + nom/nomEN
      2. adresse field "Italie - Rome" → split + qwen2.5:7b normalization
    """
    __tablename__ = 'dex_products_normalized'

    id             = Column(Integer, primary_key=True, autoincrement=True)
    dex_product_id = Column(
        Integer, ForeignKey('dex_produit.id'),
        unique=True, nullable=False, index=True
    )

    # ── Text — matching dimension 1 ───────────────────────────
    clean_title       = Column(Text)          # from dex_produit.nom (FR)
    clean_title_en    = Column(Text)          # from dex_produit.nomEN
    clean_description = Column(Text)          # from dex_detail.texte (lang=44 FR)
    # NOTE: chapeau REMOVED — confirmed 100% null in all 79999 detail rows

    # ── Geo — matching dimension 2 ────────────────────────────
    # Source: dex_localite via dex_produit.idVille
    # IMPORTANT: dex_produit.longitude/latitude are 100% NULL — never use them
    normalized_city    = Column(String(150), index=True)  # from localite.nomEN or adresse
    normalized_country = Column(String(100), index=True)  # from adresse split + normalize
    geo_lat            = Column(Float)    # from dex_localite.latitude  (alive ~54%)
    geo_lng            = Column(Float)    # from dex_localite.longitude (alive ~51%)
    city_confidence    = Column(Float, default=0.0)
    # 0.9 = localite with lat/lng | 0.5 = localite no coords | 0.3 = adresse only

    # ── Activity type — matching dimension 3 ─────────────────
    # dex_produit.typePrestation: 424=EXCURSION, 429=TRANSFER, 449=TICKET
    canonical_activity_type = Column(String(20), index=True)
    # EXCURSION | TICKET | TRANSFER

    # ── Category — matching dimension 4 ──────────────────────
    # From dex_produit: ProductType, ProductCategory, ProductSubCategory
    # (ProductType 91% alive, ProductCategory 80% alive)
    category    = Column(String(150), index=True)  # ProductCategory (most specific)
    product_type = Column(String(150))              # ProductType (broader grouping)
    service_type = Column(String(50), index=True)   # TOUR | ENTRANCE | TRANSFER

    # Transfer routing (sparse but critical for TRANSFER matching)
    origin          = Column(String(255))    # from idLocaliteDepart → localite.nomEN
    destination     = Column(String(255))    # from idLocaliteArrivee → localite.nomEN
    route_signature = Column(String(300), index=True)  # "Origin → Destination"

    # ── Price — matching dimension 5 ─────────────────────────
    price    = Column(Numeric(10, 2), index=True)  # dex_produit.prixAppel
    currency = Column(String(10), default='EUR')   # from devise: 403=EUR, 404=USD
    is_promo = Column(Boolean, default=False)       # dex_produit.enPromo

    # ── Duration ──────────────────────────────────────────────
    duration_minutes = Column(Integer)  # computed: dureeHeure*60 + dureeMinute

    # ── DEX status flags ──────────────────────────────────────
    is_active   = Column(Boolean, default=True)   # dex_produit.actif = 1
    is_vendable = Column(Boolean, default=False)  # dex_produit.vendable = 1
    # NOTE: only 2,289 / 24,386 products are vendable=1

    # ── Supplier context ──────────────────────────────────────
    supplier_name = Column(String(255))   # dex_produit.organisateur
    supplier_id   = Column(Integer)       # dex_produit.idFournisseur
    odoo_id       = Column(Integer)       # dex_produit.idOdoo (87% populated)

    # ── AI tags — replaces all is_* boolean columns ───────────
    activity_tags = Column(JSONB)
    # {
    #   "is_promo": bool,     from enPromo
    #   "is_private": bool,   from prive
    #   "is_b2c": bool,       from b2c
    #   "themes": [int],      from produitTheme.theme
    #   "languages": [str],   from produitLangue.codeService
    # }

    # ── Embedding — vector matching ───────────────────────────
    # mxbai-embed-large → 1024 dimensions
    # Embedded text: title | city country | activity_type | category | description
    embedding = Column(Vector(1024))

    # ── Dedup / matching acceleration ─────────────────────────
    # Formula: lower(city)|activity_type|first_3_title_words_md5
    matching_signature = Column(String(500), index=True)

    # ── AI metadata ───────────────────────────────────────────
    normalization_confidence = Column(Float, default=0.0)
    # 0.9 = full geo + text | 0.5 = partial | 0.0 = failed

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_dex_norm_city_type',  'normalized_city', 'canonical_activity_type'),
        Index('idx_dex_norm_signature',  'matching_signature'),
        Index('idx_dex_norm_vendable',   'is_active', 'is_vendable'),
        Index('idx_dex_norm_supplier',   'supplier_id'),
        Index('idx_dex_norm_category',   'category'),
    )


# ──────────────────────────────────────────────
# DEX IMPORT TABLES — UNTOUCHED
# These mirror the DEX Odoo export exactly.
# DO NOT MODIFY these — they are read-only references.
# ──────────────────────────────────────────────

class DexLocalite(Base):
    """DEX geographic reference — untouched."""
    __tablename__ = 'dex_localite'

    id                  = Column(Integer, primary_key=True)
    tva                 = Column(Integer)
    criterefiltre       = Column(Integer)
    idparent            = Column(Integer, ForeignKey('dex_localite.id'))
    active              = Column(Integer)
    activepourviator    = Column(Integer)
    typelocalite        = Column(Integer)
    codestourico        = Column(Text)
    codeshotelbeds      = Column(Text)
    nomen               = Column(Text)
    codetype            = Column(Text)
    commentaire         = Column(Text)
    codepicto           = Column(Text)
    codeurlrewriting    = Column(Text)
    codesviator         = Column(Text)
    nompt               = Column(Text)
    adressepostale      = Column(Text)
    longitude           = Column(Text)
    latitude            = Column(Text)
    codebroadwayinbound = Column(Text)
    codecommercial      = Column(Text)
    plan                = Column(Text)
    metadescription     = Column(Text)
    idodoo              = Column(Text)
    codetiqets          = Column(Text)
    codetui             = Column(Text)
    codeventrata        = Column(Text)
    code                = Column(Text)
    nom                 = Column(Text)
    typelocaliteparent  = Column(Text)
    codestravelcube     = Column(Text)
    codeonu             = Column(Text)
    codeiso             = Column(Text)


class DexDetail(Base):
    """DEX product content (descriptions, photos, chapeau) — untouched."""
    __tablename__ = 'dex_detail'

    id         = Column(Integer, primary_key=True)
    idobjet    = Column(Integer, ForeignKey('dex_produit.id'), index=True)
    idlangue   = Column(Integer)
    ordre      = Column(Integer)
    idfamille  = Column(Integer)
    version    = Column(Integer)
    idodoo     = Column(Integer)
    codeobjet  = Column(Text)
    texte      = Column(Text)
    urlphoto   = Column(Text)
    labelphoto = Column(Text)
    chapeau    = Column(Text)
    titre      = Column(Text)


class DexTheme(Base):
    """DEX marketing themes — untouched."""
    __tablename__ = 'dex_theme'

    id            = Column(Integer, primary_key=True)
    criterefiltre = Column(Integer)
    actif         = Column(Integer)
    nomen         = Column(Text)
    idville       = Column(Text)
    idparent      = Column(Text)
    codepicto     = Column(Text)
    nompt         = Column(Text)
    idsville      = Column(Text)
    code          = Column(Text)
    nom           = Column(Text)


class DexTypeServiceActivite(Base):
    """DEX service activity catalogue — untouched."""
    __tablename__ = 'dex_type_service_activite'

    id            = Column(Integer, primary_key=True)
    criterefiltre = Column(Integer)
    nom           = Column(Text)
    nomen         = Column(Text)
    idville       = Column(Text)
    codepicto     = Column(Text)
    nompt         = Column(Text)
    code          = Column(Text)


class DexProduit(Base):
    """
    DEX main product table — UNTOUCHED (read-only import from Odoo).
    All 112 columns preserved exactly as imported.
    Key fields for normalization pipeline:
      - typePrestation: 424=excursion, 429=transfer, 449=ticket
      - idVille → DexLocalite (geo — NOT latitude/longitude which are null)
      - prixAppel: sell price | prixAppelAchat: buy price
      - actif=1: 17 183 active | vendable=1: only 2 289 sellable
    """
    __tablename__ = 'dex_produit'

    id                                       = Column(Integer, primary_key=True)
    nom                                      = Column(Text)
    nomen                                    = Column(Text)
    nominitial                               = Column(Text)
    description                              = Column(Text)
    typeproduit                              = Column(Integer)
    vendable                                 = Column(Integer)
    idfournisseur                            = Column(Integer)
    prixappel                                = Column(Numeric(10, 2))
    prixappelachat                           = Column(Numeric(10, 2))
    tauxtva                                  = Column(Numeric(5, 2))
    prixmini                                 = Column(Numeric(10, 2))
    prixmaxi                                 = Column(Numeric(10, 2))
    devise                                   = Column(Text)
    tauxmargesupplementaireprixclientfinal   = Column(Numeric(5, 2))
    tauxremisefournisseur                    = Column(Numeric(5, 2))
    referenceexterne                         = Column(Text)
    minageenfant1                            = Column(Integer)
    maxageenfant1                            = Column(Integer)
    minageenfant2                            = Column(Integer)
    maxageenfant2                            = Column(Integer)
    nbjour                                   = Column(Integer)
    nbnuit                                   = Column(Integer)
    dureeheure                               = Column(Integer)
    dureeminute                              = Column(Integer)
    adresse                                  = Column(Text)
    idville                                  = Column(Integer, ForeignKey('dex_localite.id'))
    idlocalitedepart                         = Column(Integer, ForeignKey('dex_localite.id'))
    idlocalitearrivee                        = Column(Integer, ForeignKey('dex_localite.id'))
    longitude                                = Column(Numeric(10, 7))   # confirmed NULL
    latitude                                 = Column(Numeric(10, 7))   # confirmed NULL
    codepostal                               = Column(Text)
    gestionstock                             = Column(Integer)
    engagementsurstock                       = Column(Text)
    actif                                    = Column(Integer)
    suspendu                                 = Column(Integer)
    valide                                   = Column(Integer)
    prive                                    = Column(Integer)
    b2c                                      = Column(Integer)
    dispos                                   = Column(Integer)
    enpromo                                  = Column(Integer)
    produitexterne                           = Column(Integer)
    produitouvertatouslesclients             = Column(Integer)
    categorie                                = Column(Text)
    typeprestation                           = Column(Text)   # 424|429|449
    typeprestationinitial                    = Column(Text)
    soustypeprestation                       = Column(Text)
    producttype                              = Column(Text)
    productgroup                             = Column(Text)
    productcategory                          = Column(Text)
    productsubcategory                       = Column(Text)
    pension                                  = Column(Text)
    typeproduitceto                          = Column(Text)
    typerepasceto                            = Column(Text)
    datedebut                                = Column(Date)
    datefin                                  = Column(Date)
    heuredebut                               = Column(Time)
    heurefin                                 = Column(Time)
    datedebutvente                           = Column(Date)
    datefinvente                             = Column(Date)
    jouroperation                            = Column(Text)
    heurerdv                                 = Column(Text)
    lieurdv                                  = Column(Text)
    datecreation                             = Column(DateTime)
    datemodification                         = Column(DateTime)
    datemodificationproduit                  = Column(DateTime)
    datemiseajourdescription                 = Column(DateTime)
    datemiseajourtarif                       = Column(DateTime)
    urlresa                                  = Column(Text)
    urltableauprix                           = Column(Text)
    idorganisateur                           = Column(Integer)
    organisateur                             = Column(Text)
    idresponsable                            = Column(Integer)
    idtcms                                   = Column(Integer)
    idodoo                                   = Column(Integer)
    vol                                      = Column(Text)
    classevol                                = Column(Text)
    idtypevehicule                           = Column(Integer)
    idtypetrajet                             = Column(Integer)
    trajetsimple                             = Column(Integer)
    idtypeservice                            = Column(Integer)
    idgammeservice                           = Column(Integer)
    typeforfait                              = Column(Text)
    groupe                                   = Column(Integer)
    groupe2                                  = Column(Integer)
    interditauxbebes                         = Column(Integer)
    interditauxenfants                       = Column(Integer)
    idtypebillet                             = Column(Integer)
    emissionvoucherimmediate                 = Column(Integer)
    envoivoucher                             = Column(Integer)
    delaiannulationsansfrais                 = Column(Integer)
    cancelifbadweather                       = Column(Integer)
    cancelifinsufficienttravelers            = Column(Integer)
    adultemax                                = Column(Integer)
    adultemin                                = Column(Integer)
    xmlceto                                  = Column(Text)
    referencesexternesquestions              = Column(Text)
    codecomptable                            = Column(Text)
    codeimportancevente                      = Column(Text)
    codequalification                        = Column(Text)
    codepopularite                           = Column(Integer)
    contentsequence                          = Column(Integer)
    nompt                                    = Column(Text)
    commentaire                              = Column(Text)
    niveautraduction                         = Column(Integer)
    niveautraductionexterne                  = Column(Integer)
    allowcustomtravelerpickup                = Column(Text)
    viatorpickupoptiontype                   = Column(Text)
    unite                                    = Column(Text)
    idslocalite                              = Column(Text)
    idstheme                                 = Column(Text)
    idtourcms                                = Column(Text)
    source_sync                              = Column(String(50), default='DEX_NATIVE')
    created_by_ai                            = Column(Boolean, default=False)
    sync_batch_id                            = Column(String(100))
    last_seen_sync                           = Column(DateTime)
    is_active_sync                           = Column(Boolean, default=True)
    external_reference                       = Column(String(500), index=True)


class DexProduitLangue(Base):
    """DEX product languages — untouched."""
    __tablename__ = 'dex_produit_langue'

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    idProduit           = Column(Integer, ForeignKey('dex_produit.id'), nullable=False)
    idproduitCategorie  = Column(Integer, nullable=False)
    idLangue            = Column(Integer)
    codeService         = Column(String, nullable=False)


class DexProduitService(Base):
    """DEX product services (pickup, meals, guides…) — untouched."""
    __tablename__ = 'dex_produit_service'

    id                      = Column(Integer, primary_key=True)
    idproduit               = Column(Integer, ForeignKey('dex_produit.id'))
    idtypeserviceactivite   = Column(Integer, ForeignKey('dex_type_service_activite.id'))
    idgammeserviceactivite  = Column(Integer)
    idlocalite              = Column(Integer, ForeignKey('dex_localite.id'))
    inclut                  = Column(Integer)
    ordre                   = Column(Integer)
    idodoo                  = Column(Integer)
    heure                   = Column(Text)
    duree                   = Column(Text)
    lieuen                  = Column(Text)
    lieupt                  = Column(Text)
    descriptionpt           = Column(Text)
    lieu                    = Column(Text)
    description             = Column(Text)
    descriptionen           = Column(Text)


class DexProduitTheme(Base):
    """DEX product themes — untouched."""
    __tablename__ = 'dex_produit_theme'

    idproduit = Column(Integer, ForeignKey('dex_produit.id'), primary_key=True)
    theme     = Column(Integer, ForeignKey('dex_theme.id'),   primary_key=True)