"""
DEX — Scraping Tool Comparison Test
=====================================
Pipeline A : Firecrawl  + Groq  (llama-3.3-70b-versatile)
Pipeline B : ScrapingBee + Groq  (llama-3.3-70b-versatile)

Sources tested:
  - 3 travel supplier websites (direct scraping)
  - 1 Viator search URL (marketplace discovery)

Each source → max 10 products per pipeline.
All results saved to a NEW table: scraping_comparison_results
in the existing "Sourcing" PostgreSQL database.

Run:
    python dex_scraping_comparison.py

Requirements (pip install):
    firecrawl-py scrapingbee requests psycopg2-binary beautifulsoup4 python-dotenv
"""

import os
import json
import time
import re
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# CONFIG — fill your keys here or in .env
# ─────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "Sourcing"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "edward_alphanso"),
}

GROQ_API_KEY    = os.getenv("GROQ_API_KEY_1",    "YOUR_GROQ_API_KEY_HERE")
FIRECRAWL_KEY   = os.getenv("FIRECRAWL_API_KEY","YOUR_FIRECRAWL_KEY_HERE")
SCRAPINGBEE_KEY = os.getenv("SCRAPINGBEE_API_KEY","YOUR_SCRAPINGBEE_KEY_HERE")

GROQ_MODEL  = "llama-3.3-70b-versatile"
MAX_PRODUCTS = 10   # max products to extract per URL per pipeline

# ─────────────────────────────────────────────────────────────
# TEST SOURCES
# One Viator URL + real travel supplier websites
# ─────────────────────────────────────────────────────────────

SOURCES = [
    # ── Marketplace ─────────────────────────────────────────
    {
        "url":     "https://www.viator.com/searchResults/all?text=tozeur+tunisia",
        "city":    None,
        "country": None,
        "type":    "marketplace",
        "label":   "Viator — Tozeur Tunisia",
    },
    # ── Travel Supplier Websites ─────────────────────────────
    # No city/country hardcoded — Groq will detect from page content
    {
        "url":     "https://www.royalgatetransport.ae",
        "city":    None,
        "country": None,
        "type":    "supplier",
        "label":   "Royal Gate Transport (AE)",
    },
    {
        "url":     "https://www.carboneratour.com",
        "city":    None,
        "country": None,
        "type":    "supplier",
        "label":   "Carbonera Tour",
    },
    {
        "url":     "https://www.internationaltraveladvisor.com",
        "city":    None,
        "country": None,
        "type":    "supplier",
        "label":   "International Travel Advisor",
    },
    {
        "url":     "https://www.medellindaytrips.com",
        "city":    None,
        "country": None,
        "type":    "supplier",
        "label":   "Medellin Day Trips",
    },
    {
        "url":     "https://www.seeusatours.com",
        "city":    None,
        "country": None,
        "type":    "supplier",
        "label":   "See USA Tours",
    },
    {
        "url":     "https://www.luxurytour.nl",
        "city":    None,
        "country": None,
        "type":    "supplier",
        "label":   "Luxury Tour NL",
    },
]


# ─────────────────────────────────────────────────────────────
# DATABASE — create comparison table
# ─────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scraping_comparison_results (
    id              SERIAL PRIMARY KEY,

    -- Source info
    source_url      TEXT        NOT NULL,
    source_label    TEXT,
    source_type     VARCHAR(20),          -- 'marketplace' or 'supplier'
    city_hint       VARCHAR(100),
    country_hint    VARCHAR(100),

    -- Which pipeline produced this row
    pipeline        VARCHAR(30) NOT NULL, -- 'firecrawl_groq' or 'scrapingbee_groq'
    groq_model      VARCHAR(80),

    -- Extracted product fields
    title           TEXT,
    description     TEXT,
    price_raw       VARCHAR(100),
    category        VARCHAR(150),
    activity_type   VARCHAR(20),          -- EXCURSION / TICKET / TRANSFER
    city_extracted  VARCHAR(100),
    country_extracted VARCHAR(100),
    product_url     TEXT,

    -- Quality / debug
    tokens_used     INTEGER,
    fetch_duration_ms INTEGER,
    groq_duration_ms  INTEGER,
    raw_html_length INTEGER,
    clean_text_length INTEGER,
    extraction_status VARCHAR(20),        -- 'ok' / 'no_products' / 'error'
    error_message   TEXT,

    -- Meta
    run_id          VARCHAR(40),
    scraped_at      TIMESTAMP DEFAULT NOW()
);

-- Index for easy comparison queries
CREATE INDEX IF NOT EXISTS idx_scrcomp_pipeline
    ON scraping_comparison_results (pipeline);
CREATE INDEX IF NOT EXISTS idx_scrcomp_source
    ON scraping_comparison_results (source_label);
CREATE INDEX IF NOT EXISTS idx_scrcomp_run
    ON scraping_comparison_results (run_id);
"""


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_table_exists():
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Table scraping_comparison_results ready")


def save_results(rows: list):
    """Insert a list of result dicts into the comparison table."""
    if not rows:
        return
    conn = get_db_connection()
    cur  = conn.cursor()
    insert_sql = """
        INSERT INTO scraping_comparison_results (
            source_url, source_label, source_type, city_hint, country_hint,
            pipeline, groq_model,
            title, description, price_raw, category, activity_type,
            city_extracted, country_extracted, product_url,
            tokens_used, fetch_duration_ms, groq_duration_ms,
            raw_html_length, clean_text_length,
            extraction_status, error_message, run_id
        ) VALUES (
            %(source_url)s, %(source_label)s, %(source_type)s,
            %(city_hint)s, %(country_hint)s,
            %(pipeline)s, %(groq_model)s,
            %(title)s, %(description)s, %(price_raw)s, %(category)s,
            %(activity_type)s, %(city_extracted)s, %(country_extracted)s,
            %(product_url)s,
            %(tokens_used)s, %(fetch_duration_ms)s, %(groq_duration_ms)s,
            %(raw_html_length)s, %(clean_text_length)s,
            %(extraction_status)s, %(error_message)s, %(run_id)s
        )
    """
    psycopg2.extras.execute_batch(cur, insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"   💾 Saved {len(rows)} rows to DB")


# ─────────────────────────────────────────────────────────────
# HTML CLEANING — same BS4 logic as your main pipeline
# ─────────────────────────────────────────────────────────────

def clean_html(raw_html: str) -> str:
    """Strip noise, return clean text. ~75% token reduction."""
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "aside", "form", "iframe", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s{2,}", " ", text)
    # Limit to 5000 chars to stay well under Groq TPM
    if len(text) > 5000:
        text = text[:5000] + "..."
    return text


# ─────────────────────────────────────────────────────────────
# GROQ EXTRACTION — shared by both pipelines
# ─────────────────────────────────────────────────────────────

GROQ_SYSTEM_PROMPT = """You are a tourism product extractor for Day Experience (DEX),
a travel booking platform like Viator or GetYourGuide.

Your job: read the scraped text from a travel supplier website and extract all
bookable tourism products (tours, excursions, transfers, tickets, activities).

Return ONLY valid JSON — no markdown fences, no explanation, no preamble.

Format:
{
  "supplier_name": "string — detected company name from the page",
  "detected_city": "string — main city this supplier operates in (detect from page)",
  "detected_country": "string — country this supplier operates in (detect from page)",
  "products": [
    {
      "title": "string — exact marketing name of the tour/activity",
      "description": "string — 1-2 sentence summary of what is included",
      "price_raw": "string — raw price text e.g. '45€', 'From $30', 'AED 150' or ''",
      "category": "string — e.g. 'Day Trip', 'City Tour', 'Airport Transfer', 'Desert Safari'",
      "activity_type": "EXCURSION or TICKET or TRANSFER",
      "city": "string — destination city for this specific product",
      "country": "string — destination country for this specific product",
      "product_url": "string — product page URL if visible in the text, else ''"
    }
  ]
}

Rules:
- activity_type MUST be exactly one of: EXCURSION, TICKET, TRANSFER
  EXCURSION = tours, day trips, safaris, guided visits, activities
  TICKET    = entrance tickets, attraction passes, skip-the-line
  TRANSFER  = airport transfers, private drivers, shuttle services
- If price not found leave price_raw as empty string ""
- Detect city and country from the page content (company address, product destinations, etc.)
- Extract at most """ + str(MAX_PRODUCTS) + """ products — prioritize the most bookable ones
- SKIP: navigation links, login buttons, blog posts, contact forms, footer text
- If no clear products found, return empty products array []"""


def call_groq(clean_text: str, city_hint: str | None, country_hint: str | None) -> dict:
    """
    Call Groq API directly via HTTP.
    city_hint and country_hint can be None — Groq will auto-detect from page.
    Returns: {"products": [...], "supplier_name": str, "detected_city": str,
              "detected_country": str, "tokens_used": int, "duration_ms": int}
    """
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }

    # Build the user message — only mention hints if they exist
    hint_line = ""
    if city_hint:
        hint_line += f"City hint: {city_hint}. "
    if country_hint:
        hint_line += f"Country hint: {country_hint}. "
    if not hint_line:
        hint_line = "No city/country hint — detect from page content. "

    user_content = f"{hint_line}\n\nScrapped text:\n{clean_text}"

    payload = {
        "model":       GROQ_MODEL,
        "max_tokens":  1800,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": GROQ_SYSTEM_PROMPT},
            {"role": "user",   "content": user_content},
        ],
    }

    t0 = time.time()
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=45)
        duration_ms = int((time.time() - t0) * 1000)

        if resp.status_code != 200:
            return {
                "error": f"Groq HTTP {resp.status_code}: {resp.text[:200]}",
                "duration_ms": duration_ms,
            }

        data    = resp.json()
        content = data["choices"][0]["message"]["content"]
        tokens  = data.get("usage", {}).get("total_tokens", 0)

        # Strip markdown fences if model added them anyway
        content = re.sub(r"```json|```", "", content).strip()

        parsed = json.loads(content)
        return {
            "products":         parsed.get("products", [])[:MAX_PRODUCTS],
            "supplier_name":    parsed.get("supplier_name", ""),
            "detected_city":    parsed.get("detected_city", ""),
            "detected_country": parsed.get("detected_country", ""),
            "tokens_used":      tokens,
            "duration_ms":      duration_ms,
        }

    except json.JSONDecodeError as e:
        return {"error": f"JSON parse error: {e}", "duration_ms": duration_ms}
    except Exception as e:
        return {"error": str(e), "duration_ms": int((time.time() - t0) * 1000)}


# ─────────────────────────────────────────────────────────────
# PIPELINE A — FIRECRAWL + GROQ
# ─────────────────────────────────────────────────────────────

def pipeline_firecrawl_groq(source: dict, run_id: str) -> list:
    """
    Firecrawl fetches + renders the page → returns markdown/HTML.
    Then Groq extracts products from the cleaned text.
    """
    print(f"\n  🔥 [Firecrawl+Groq] {source['label']}")

    base_row = {
        "source_url":     source["url"],
        "source_label":   source["label"],
        "source_type":    source["type"],
        "city_hint":      source.get("city"),
        "country_hint":   source.get("country"),
        "pipeline":       "firecrawl_groq",
        "groq_model":     GROQ_MODEL,
        "run_id":         run_id,
    }

    # ── Step 1: Firecrawl fetch ──────────────────────────────
    firecrawl_url = "https://api.firecrawl.dev/v1/scrape"
    headers = {
        "Authorization": f"Bearer {FIRECRAWL_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "url":     source["url"],
        "formats": ["markdown", "html"],
        "onlyMainContent": True,
        "waitFor": 3000,   # wait 3s for JS
    }

    t0 = time.time()
    try:
        resp = requests.post(firecrawl_url, headers=headers,
                             json=payload, timeout=60)
        fetch_ms = int((time.time() - t0) * 1000)
        print(f"     Firecrawl HTTP {resp.status_code} — {fetch_ms}ms")

        if resp.status_code != 200:
            return [{
                **base_row,
                "extraction_status": "error",
                "error_message": f"Firecrawl HTTP {resp.status_code}: {resp.text[:300]}",
                "fetch_duration_ms": fetch_ms,
                "groq_duration_ms": 0, "tokens_used": 0,
                "raw_html_length": 0, "clean_text_length": 0,
                "title": None, "description": None, "price_raw": None,
                "category": None, "activity_type": None,
                "city_extracted": None, "country_extracted": None,
                "product_url": None,
            }]

        fc_data   = resp.json()
        # Firecrawl v1 returns data.markdown or data.html
        raw_content = ""
        if fc_data.get("success"):
            page = fc_data.get("data", {})
            raw_content = page.get("markdown") or page.get("html") or ""
        else:
            raw_content = fc_data.get("markdown") or fc_data.get("html") or ""

        raw_len = len(raw_content)
        print(f"     Raw content: {raw_len} chars")

    except Exception as e:
        fetch_ms = int((time.time() - t0) * 1000)
        return [{
            **base_row,
            "extraction_status": "error",
            "error_message": f"Firecrawl exception: {e}",
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms": 0, "tokens_used": 0,
            "raw_html_length": 0, "clean_text_length": 0,
            "title": None, "description": None, "price_raw": None,
            "category": None, "activity_type": None,
            "city_extracted": None, "country_extracted": None,
            "product_url": None,
        }]

    # ── Step 2: Clean text ───────────────────────────────────
    # If firecrawl returned markdown, BS4 isn't needed — it's already clean
    # But we still trim to stay under TPM
    if "<html" in raw_content.lower() or "<body" in raw_content.lower():
        clean_text = clean_html(raw_content)
    else:
        # It's markdown — just trim
        clean_text = raw_content[:5000] + ("..." if len(raw_content) > 5000 else "")
    clean_len = len(clean_text)
    print(f"     Clean text: {clean_len} chars")

    # ── Step 3: Groq extraction ──────────────────────────────
    groq_result = call_groq(clean_text, source.get("city"), source.get("country"))
    print(f"     Groq: {groq_result.get('duration_ms', 0)}ms | "
          f"{groq_result.get('tokens_used', 0)} tokens")

    if "error" in groq_result:
        return [{
            **base_row,
            "extraction_status": "error",
            "error_message": groq_result["error"],
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms": groq_result.get("duration_ms", 0),
            "tokens_used": 0,
            "raw_html_length": raw_len,
            "clean_text_length": clean_len,
            "title": None, "description": None, "price_raw": None,
            "category": None, "activity_type": None,
            "city_extracted": None, "country_extracted": None,
            "product_url": None,
        }]

    products = groq_result.get("products", [])
    detected_city    = groq_result.get("detected_city", "") or source.get("city") or ""
    detected_country = groq_result.get("detected_country", "") or source.get("country") or ""
    print(f"     ✅ {len(products)} products | city={detected_city} | country={detected_country}")

    if not products:
        return [{
            **base_row,
            "extraction_status": "no_products",
            "error_message": "Groq returned empty products array",
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms": groq_result.get("duration_ms", 0),
            "tokens_used": groq_result.get("tokens_used", 0),
            "raw_html_length": raw_len,
            "clean_text_length": clean_len,
            "title": None, "description": None, "price_raw": None,
            "category": None, "activity_type": None,
            "city_extracted": detected_city or None,
            "country_extracted": detected_country or None,
            "product_url": None,
        }]

    rows = []
    for p in products:
        act = (p.get("activity_type") or "EXCURSION").upper().strip()
        if act not in ("EXCURSION", "TICKET", "TRANSFER"):
            act = "EXCURSION"
        p_city    = (p.get("city")    or detected_city    or "")[:100]
        p_country = (p.get("country") or detected_country or "")[:100]
        rows.append({
            **base_row,
            "title":             (p.get("title") or "")[:500],
            "description":       (p.get("description") or "")[:1000],
            "price_raw":         (p.get("price_raw") or "")[:100],
            "category":          (p.get("category") or "")[:150],
            "activity_type":     act,
            "city_extracted":    p_city,
            "country_extracted": p_country,
            "product_url":       (p.get("product_url") or "")[:500],
            "tokens_used":       groq_result.get("tokens_used", 0),
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms":  groq_result.get("duration_ms", 0),
            "raw_html_length":   raw_len,
            "clean_text_length": clean_len,
            "extraction_status": "ok",
            "error_message":     None,
        })
    return rows


# ─────────────────────────────────────────────────────────────
# PIPELINE B — SCRAPINGBEE + GROQ
# ─────────────────────────────────────────────────────────────

def pipeline_scrapingbee_groq(source: dict, run_id: str) -> list:
    """
    ScrapingBee renders JS-heavy pages and bypasses anti-bot.
    Then BS4 cleans the raw HTML, then Groq extracts products.
    """
    print(f"\n  🐝 [ScrapingBee+Groq] {source['label']}")

    base_row = {
        "source_url":     source["url"],
        "source_label":   source["label"],
        "source_type":    source["type"],
        "city_hint":      source.get("city"),
        "country_hint":   source.get("country"),
        "pipeline":       "scrapingbee_groq",
        "groq_model":     GROQ_MODEL,
        "run_id":         run_id,
    }

    # ── Step 1: ScrapingBee fetch ────────────────────────────
    bee_url = "https://app.scrapingbee.com/api/v1/"
    is_marketplace = source["type"] == "marketplace"

    params = {
        "api_key":       SCRAPINGBEE_KEY,
        "url":           source["url"],
        "render_js":     "true",
        "premium_proxy": "true" if is_marketplace else "false",
        "country_code":  "fr",
        "wait":          "4000" if is_marketplace else "2000",
        "block_resources": "false",
    }

    t0 = time.time()
    try:
        resp = requests.get(bee_url, params=params, timeout=90)
        fetch_ms = int((time.time() - t0) * 1000)
        print(f"     ScrapingBee HTTP {resp.status_code} — {fetch_ms}ms")

        if resp.status_code != 200:
            return [{
                **base_row,
                "extraction_status": "error",
                "error_message": f"ScrapingBee HTTP {resp.status_code}: {resp.text[:300]}",
                "fetch_duration_ms": fetch_ms,
                "groq_duration_ms": 0, "tokens_used": 0,
                "raw_html_length": 0, "clean_text_length": 0,
                "title": None, "description": None, "price_raw": None,
                "category": None, "activity_type": None,
                "city_extracted": None, "country_extracted": None,
                "product_url": None,
            }]

        raw_html = resp.text
        raw_len  = len(raw_html)
        print(f"     Raw HTML: {raw_len} chars")

    except Exception as e:
        fetch_ms = int((time.time() - t0) * 1000)
        return [{
            **base_row,
            "extraction_status": "error",
            "error_message": f"ScrapingBee exception: {e}",
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms": 0, "tokens_used": 0,
            "raw_html_length": 0, "clean_text_length": 0,
            "title": None, "description": None, "price_raw": None,
            "category": None, "activity_type": None,
            "city_extracted": None, "country_extracted": None,
            "product_url": None,
        }]

    # ── Step 2: BS4 Clean ────────────────────────────────────
    clean_text = clean_html(raw_html)
    clean_len  = len(clean_text)
    print(f"     Clean text: {clean_len} chars")

    # ── Step 3: Groq extraction ──────────────────────────────
    groq_result = call_groq(clean_text, source.get("city"), source.get("country"))
    print(f"     Groq: {groq_result.get('duration_ms', 0)}ms | "
          f"{groq_result.get('tokens_used', 0)} tokens")

    if "error" in groq_result:
        return [{
            **base_row,
            "extraction_status": "error",
            "error_message": groq_result["error"],
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms": groq_result.get("duration_ms", 0),
            "tokens_used": 0,
            "raw_html_length": raw_len,
            "clean_text_length": clean_len,
            "title": None, "description": None, "price_raw": None,
            "category": None, "activity_type": None,
            "city_extracted": None, "country_extracted": None,
            "product_url": None,
        }]

    products = groq_result.get("products", [])
    detected_city    = groq_result.get("detected_city", "") or source.get("city") or ""
    detected_country = groq_result.get("detected_country", "") or source.get("country") or ""
    print(f"     ✅ {len(products)} products | city={detected_city} | country={detected_country}")

    if not products:
        return [{
            **base_row,
            "extraction_status": "no_products",
            "error_message": "Groq returned empty products array",
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms": groq_result.get("duration_ms", 0),
            "tokens_used": groq_result.get("tokens_used", 0),
            "raw_html_length": raw_len,
            "clean_text_length": clean_len,
            "title": None, "description": None, "price_raw": None,
            "category": None, "activity_type": None,
            "city_extracted": detected_city or None,
            "country_extracted": detected_country or None,
            "product_url": None,
        }]

    rows = []
    for p in products:
        act = (p.get("activity_type") or "EXCURSION").upper().strip()
        if act not in ("EXCURSION", "TICKET", "TRANSFER"):
            act = "EXCURSION"
        p_city    = (p.get("city")    or detected_city    or "")[:100]
        p_country = (p.get("country") or detected_country or "")[:100]
        rows.append({
            **base_row,
            "title":             (p.get("title") or "")[:500],
            "description":       (p.get("description") or "")[:1000],
            "price_raw":         (p.get("price_raw") or "")[:100],
            "category":          (p.get("category") or "")[:150],
            "activity_type":     act,
            "city_extracted":    p_city,
            "country_extracted": p_country,
            "product_url":       (p.get("product_url") or "")[:500],
            "tokens_used":       groq_result.get("tokens_used", 0),
            "fetch_duration_ms": fetch_ms,
            "groq_duration_ms":  groq_result.get("duration_ms", 0),
            "raw_html_length":   raw_len,
            "clean_text_length": clean_len,
            "extraction_status": "ok",
            "error_message":     None,
        })
    return rows


# ─────────────────────────────────────────────────────────────
# SUMMARY REPORT — printed after all runs
# ─────────────────────────────────────────────────────────────

def print_summary(all_rows: list):
    print("\n" + "=" * 60)
    print("  COMPARISON SUMMARY")
    print("=" * 60)

    pipelines = ["firecrawl_groq", "scrapingbee_groq"]
    for pipe in pipelines:
        rows = [r for r in all_rows if r["pipeline"] == pipe]
        ok   = [r for r in rows if r["extraction_status"] == "ok"]
        errs = [r for r in rows if r["extraction_status"] == "error"]
        empty= [r for r in rows if r["extraction_status"] == "no_products"]

        avg_fetch = (sum(r.get("fetch_duration_ms", 0) or 0 for r in rows) /
                     max(len(rows), 1))
        avg_groq  = (sum(r.get("groq_duration_ms", 0) or 0 for r in rows) /
                     max(len(rows), 1))
        avg_tok   = (sum(r.get("tokens_used", 0) or 0 for r in rows) /
                     max(len(rows), 1))

        label = "🔥 Firecrawl + Groq" if pipe == "firecrawl_groq" else "🐝 ScrapingBee + Groq"
        print(f"\n{label}")
        print(f"  Total rows saved : {len(rows)}")
        print(f"  Products OK      : {len(ok)}")
        print(f"  No products      : {len(empty)}")
        print(f"  Errors           : {len(errs)}")
        print(f"  Avg fetch time   : {avg_fetch:.0f}ms")
        print(f"  Avg Groq time    : {avg_groq:.0f}ms")
        print(f"  Avg tokens/call  : {avg_tok:.0f}")

        if ok:
            activity_counts = {}
            for r in ok:
                act = r.get("activity_type", "?")
                activity_counts[act] = activity_counts.get(act, 0) + 1
            print(f"  Activity types   : {activity_counts}")

    print("\n  QUERY TO VIEW RESULTS IN POSTGRES:")
    print("  SELECT pipeline, source_label, activity_type, title, price_raw")
    print("  FROM scraping_comparison_results")
    print("  ORDER BY pipeline, source_label")
    print("  LIMIT 50;")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    run_id = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    print(f"\n{'=' * 60}")
    print(f"  DEX Scraping Comparison Test")
    print(f"  Run ID  : {run_id}")
    print(f"  Sources : {len(SOURCES)} URLs")
    print(f"  Max products per URL per pipeline : {MAX_PRODUCTS}")
    print(f"  Pipelines: Firecrawl+Groq | ScrapingBee+Groq")
    print(f"{'=' * 60}")

    # Check keys
    missing = []
    if "YOUR_GROQ_API_KEY"     in GROQ_API_KEY:    missing.append("GROQ_API_KEY")
    if "YOUR_FIRECRAWL_KEY"    in FIRECRAWL_KEY:   missing.append("FIRECRAWL_API_KEY")
    if "YOUR_SCRAPINGBEE_KEY"  in SCRAPINGBEE_KEY: missing.append("SCRAPINGBEE_API_KEY")
    if missing:
        print(f"\n⚠️  Missing API keys: {missing}")
        print("   Set them in .env or directly in this file at the top.")
        print("   Continuing — affected pipelines will return errors.\n")

    # Ensure table exists
    try:
        ensure_table_exists()
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        print("   Check DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        return

    all_rows = []

    for i, source in enumerate(SOURCES, 1):
        print(f"\n{'─' * 60}")
        print(f"[{i}/{len(SOURCES)}] {source['label']}")
        print(f"  URL: {source['url'][:70]}")

        # Pipeline A — Firecrawl + Groq
        try:
            rows_a = pipeline_firecrawl_groq(source, run_id)
            save_results(rows_a)
            all_rows.extend(rows_a)
        except Exception as e:
            print(f"  ❌ Firecrawl pipeline exception: {e}")

        # Small delay between pipelines to be respectful to Groq
        time.sleep(2)

        # Pipeline B — ScrapingBee + Groq
        try:
            rows_b = pipeline_scrapingbee_groq(source, run_id)
            save_results(rows_b)
            all_rows.extend(rows_b)
        except Exception as e:
            print(f"  ❌ ScrapingBee pipeline exception: {e}")

        # Delay between sources — avoid Groq rate limits
        if i < len(SOURCES):
            print(f"\n  ⏳ Waiting 3s before next source...")
            time.sleep(3)

    print_summary(all_rows)
    print(f"\n✅ Done. Run ID: {run_id}")
    print(f"   Total rows in DB: {len(all_rows)}")


if __name__ == "__main__":
    main()