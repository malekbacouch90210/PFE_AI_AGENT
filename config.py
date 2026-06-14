"""
config.py — DEX AI Sourcing Agent — FINAL

5-Layer Architecture Config:
  Layer 1 — Firecrawl  : FIRECRAWL_API_KEY, FIRECRAWL_URL
  Layer 2 — Groq/Ollama: GROQ_API_KEYS, OLLAMA_* models
  Layer 3 — PostgreSQL : DATABASE_URL
  Layer 4 — Embeddings : OLLAMA_MODEL_EMBED
  Layer 5 — Validation : PRICE_MIN/MAX, confidence thresholds
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:

    # ── Layer 3: PostgreSQL (truth/memory) ────────────────────
    DB_HOST     = os.getenv("DB_HOST",     "localhost")
    DB_PORT     = os.getenv("DB_PORT",     "5432")
    DB_NAME     = os.getenv("DB_NAME",     "Sourcing")
    DB_USER     = os.getenv("DB_USER",     "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "edward_alphanso")

    DATABASE_URL = (
        f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # ── Layer 1: Firecrawl (acquisition) ─────────────────────
    FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
    FIRECRAWL_URL     = os.getenv("FIRECRAWL_URL", "https://api.firecrawl.dev")
    # Cloud: https://api.firecrawl.dev
    # Local Docker (if ever needed): http://localhost:3002

    # ── Layer 2: Groq (reasoning — product extraction) ───────
    # Build pool: filter empty keys
    GROQ_API_KEYS = [
        k for k in [
            os.getenv("GROQ_API_KEY_1", ""),
            os.getenv("GROQ_API_KEY_2", ""),
            os.getenv("GROQ_API_KEY_3", ""),
            os.getenv("GROQ_API_KEY",   ""),   # single key fallback
        ]
        if k and k.startswith("gsk_")
    ]
    # Deduplicate
    GROQ_API_KEYS  = list(dict.fromkeys(GROQ_API_KEYS))
    GROQ_BASE_URL  = "https://api.groq.com/openai/v1"

    # Models — mixtral DECOMMISSIONED, do not use
    GROQ_PRIMARY_MODEL   = "llama-3.3-70b-versatile"  # 12000 TPM
    GROQ_SECONDARY_MODEL = "openai/gpt-oss-20b"           # active fallback
    GROQ_FALLBACK_MODEL  = "llama-3.1-8b-instant"      # 6000 TPM, fastest

    # ── Layer 2: Ollama (reasoning — normalization) ──────────
    OLLAMA_URL              = os.getenv("OLLAMA_URL", "http://localhost:11434")
    OLLAMA_MODEL_CLASSIFY   = "qwen2.5:7b"        # city/country constrained selection
    OLLAMA_MODEL_CLASSIFY_2 = "deepseek-r1:8b"    # used only for complex reasoning
    OLLAMA_MODEL_EMBED      = "mxbai-embed-large"  # 1024-dim embeddings
    OLLAMA_TIMEOUT          = 45

    # ── Layer 1: SerpAPI (supplier discovery — PRIMARY) ──────
    SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")


    # ── Layer 1: Scrappa (supplier discovery) ────────────────
    SCRAPPA_API_KEY = os.getenv("SCRAPPA_API_KEY", "")

    # ── Layer 5: Validation thresholds (safety) ──────────────
    # Price: outside these bounds → NULL, never garbage
    PRICE_MIN_EUR = 5.0
    PRICE_MAX_EUR = 2000.0

    # Location confidence: below this → flag for review
    LOCATION_CONFIDENCE_MIN = 0.4

    # Title: below this length → reject
    TITLE_MIN_CHARS = 8
    TITLE_MIN_WORDS = 2

    # ── Concurrency ───────────────────────────────────────────
    MAX_CONCURRENT_SUPPLIER_CHECKS = 20
    MAX_CONCURRENT_PRODUCT_SCRAPES = 3
    REQUEST_TIMEOUT                = 15
    HOMEPAGE_MAX_BYTES             = 32_000

    # ── Normalization ─────────────────────────────────────────
    NORMALIZATION_BATCH_SIZE  = 100
    NORMALIZATION_CONCURRENCY = 3

    # ── APScheduler ───────────────────────────────────────────
    SCHEDULER_TIMEZONE = "Europe/Paris"

    # ── Helpers ───────────────────────────────────────────────
    @classmethod
    def groq_configured(cls) -> bool:
        return len(cls.GROQ_API_KEYS) > 0

    @classmethod
    def groq_key_count(cls) -> int:
        return len(cls.GROQ_API_KEYS)

    @classmethod
    def firecrawl_configured(cls) -> bool:
        return bool(cls.FIRECRAWL_API_KEY)

    @classmethod
    def scrappa_configured(cls) -> bool:
        return bool(cls.SCRAPPA_API_KEY)

    @classmethod
    def serpapi_configured(cls) -> bool:
        return bool(cls.SERPAPI_KEY)

    # ── Sprint 6: Odoo XML-RPC connection ────────────────────────
    # Odoo 19 Community runs on http://localhost:8069 via Docker
    ODOO_URL = os.getenv("ODOO_URL", "http://localhost:8069")
    ODOO_DB = os.getenv("ODOO_DB", "")
    ODOO_USER = os.getenv("ODOO_USER", "")
    ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")


config = Config()
