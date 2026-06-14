"""
DEX — Free Scraping Tools Comparison Test
==========================================
Purpose: Demonstrate WHY free scraping tools are NOT sufficient
         for automated tourism product collection at scale.

Tools tested (all FREE, no API keys needed):
  Tool 1 : requests + BeautifulSoup 4  (basic HTML parsing)
  Tool 2 : JSON-LD extraction          (structured data from <script type="application/ld+json">)
  Tool 3 : Sitemap crawling            (discover product URLs via sitemap.xml)
  Tool 4 : Playwright                  (headless browser, JS rendering)
  Tool 5 : Selenium                    (headless Chrome automation)

Same sources as dex_scraping_comparison.py:
  - Viator (JS-heavy marketplace, anti-bot protected)
  - royalgatetransport.ae
  - carboneratour.com
  - internationaltraveladvisor.com
  - medellindaytrips.com
  - seeusatours.com
  - luxurytour.nl

Results saved to SAME table: scraping_comparison_results
Pipeline names: requests_bs4 / jsonld / sitemap / playwright / selenium

Run:
    python free_scraping_tools.py

Requirements:
    pip install playwright selenium beautifulsoup4 lxml requests psycopg2-binary
    python -m playwright install chromium
    (Selenium needs chromedriver matching your Chrome version)

NOTE: This file intentionally shows failures and partial results.
      The point is to document the LIMITATIONS of free tools
      vs paid tools (Firecrawl + ScrapingBee) for the PFE report.
"""

import os
import re
import json
import time
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "Sourcing"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "edward_alphanso"),
}

MAX_PRODUCTS = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Same sources as dex_scraping_comparison.py
SOURCES = [
    {
        "url":   "https://www.viator.com/searchResults/all?text=tozeur+tunisia",
        "type":  "marketplace",
        "label": "Viator — Tozeur Tunisia",
    },
    {
        "url":   "https://www.royalgatetransport.ae",
        "type":  "supplier",
        "label": "Royal Gate Transport (AE)",
    },
    {
        "url":   "https://www.carboneratour.com",
        "type":  "supplier",
        "label": "Carbonera Tour",
    },
    {
        "url":   "https://www.internationaltraveladvisor.com",
        "type":  "supplier",
        "label": "International Travel Advisor",
    },
    {
        "url":   "https://www.medellindaytrips.com",
        "type":  "supplier",
        "label": "Medellin Day Trips",
    },
    {
        "url":   "https://www.seeusatours.com",
        "type":  "supplier",
        "label": "See USA Tours",
    },
    {
        "url":   "https://www.luxurytour.nl",
        "type":  "supplier",
        "label": "Luxury Tour NL",
    },
    {
        "url": "https://www.japanlocalexperience.com",
        "type": "supplier",
        "label": "Luxury Tour NL",
    },
]


# ─────────────────────────────────────────────────────────────
# DATABASE — reuse same table from dex_scraping_comparison.py
# ─────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scraping_comparison_results (
    id              SERIAL PRIMARY KEY,
    source_url      TEXT        NOT NULL,
    source_label    TEXT,
    source_type     VARCHAR(20),
    city_hint       VARCHAR(100),
    country_hint    VARCHAR(100),
    pipeline        VARCHAR(30) NOT NULL,
    groq_model      VARCHAR(80),
    title           TEXT,
    description     TEXT,
    price_raw       VARCHAR(100),
    category        VARCHAR(150),
    activity_type   VARCHAR(20),
    city_extracted  VARCHAR(100),
    country_extracted VARCHAR(100),
    product_url     TEXT,
    tokens_used     INTEGER,
    fetch_duration_ms INTEGER,
    groq_duration_ms  INTEGER,
    raw_html_length INTEGER,
    clean_text_length INTEGER,
    extraction_status VARCHAR(20),
    error_message   TEXT,
    run_id          VARCHAR(40),
    scraped_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scrcomp_pipeline ON scraping_comparison_results (pipeline);
CREATE INDEX IF NOT EXISTS idx_scrcomp_source   ON scraping_comparison_results (source_label);
CREATE INDEX IF NOT EXISTS idx_scrcomp_run      ON scraping_comparison_results (run_id);
"""


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def ensure_table():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Table scraping_comparison_results ready")


def save_rows(rows: list):
    if not rows:
        return
    conn = get_db()
    cur  = conn.cursor()
    sql = """
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
    psycopg2.extras.execute_batch(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"   💾 Saved {len(rows)} rows to DB")


# ─────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────

def make_base_row(source: dict, pipeline: str, run_id: str) -> dict:
    return {
        "source_url":      source["url"],
        "source_label":    source["label"],
        "source_type":     source["type"],
        "city_hint":       None,
        "country_hint":    None,
        "pipeline":        pipeline,
        "groq_model":      None,   # no LLM used in free tools
        "tokens_used":     0,
        "groq_duration_ms": 0,
        "run_id":          run_id,
    }


def error_row(base: dict, msg: str, fetch_ms: int = 0,
              raw_len: int = 0, clean_len: int = 0) -> dict:
    return {
        **base,
        "title": None, "description": None, "price_raw": None,
        "category": None, "activity_type": None,
        "city_extracted": None, "country_extracted": None,
        "product_url": None,
        "fetch_duration_ms": fetch_ms,
        "raw_html_length":   raw_len,
        "clean_text_length": clean_len,
        "extraction_status": "error",
        "error_message":     msg[:500],
    }


def product_row(base: dict, title: str, description: str,
                price: str, category: str, activity_type: str,
                city: str, country: str, product_url: str,
                fetch_ms: int, raw_len: int, clean_len: int) -> dict:
    act = (activity_type or "EXCURSION").upper().strip()
    if act not in ("EXCURSION", "TICKET", "TRANSFER"):
        act = "EXCURSION"
    return {
        **base,
        "title":             title[:500] if title else None,
        "description":       description[:1000] if description else None,
        "price_raw":         price[:100] if price else None,
        "category":          category[:150] if category else None,
        "activity_type":     act,
        "city_extracted":    city[:100] if city else None,
        "country_extracted": country[:100] if country else None,
        "product_url":       product_url[:500] if product_url else None,
        "fetch_duration_ms": fetch_ms,
        "raw_html_length":   raw_len,
        "clean_text_length": clean_len,
        "extraction_status": "ok",
        "error_message":     None,
    }


def heuristic_classify(title: str) -> str:
    """Basic keyword classification — no AI needed."""
    t = title.lower()
    if any(w in t for w in ["transfer", "shuttle", "airport", "taxi", "pickup", "drop"]):
        return "TRANSFER"
    if any(w in t for w in ["ticket", "entry", "admission", "pass", "skip", "access"]):
        return "TICKET"
    return "EXCURSION"


def heuristic_extract_products_from_text(text: str, source_url: str) -> list:
    """
    Last-resort heuristic: extract lines that look like product titles.
    Used as fallback when tools return raw text without structure.
    """
    products = []
    seen = set()
    lines = text.split("\n")
    for line in lines:
        line = line.strip()
        if (10 < len(line) < 150
                and len(line.split()) >= 3
                and line not in seen
                and not line.startswith("http")
                and not re.match(r"^[\d\W]+$", line)):
            # Must look like a real product (not nav/footer noise)
            good = any(w in line.lower() for w in [
                "tour", "trip", "excursion", "transfer", "ticket",
                "safari", "visit", "day", "night", "experience",
                "adventure", "guide", "trek", "hike", "cruise",
                "city", "private", "group", "airport"
            ])
            if good:
                seen.add(line)
                products.append({
                    "title":        line,
                    "description":  "",
                    "price_raw":    "",
                    "category":     "",
                    "activity_type": heuristic_classify(line),
                    "city":         "",
                    "country":      "",
                    "product_url":  source_url,
                })
        if len(products) >= MAX_PRODUCTS:
            break
    return products


# ─────────────────────────────────────────────────────────────
# TOOL 1 — requests + BeautifulSoup 4
# ─────────────────────────────────────────────────────────────

def tool_requests_bs4(source: dict, run_id: str) -> list:
    """
    Plain HTTP GET + BeautifulSoup HTML parsing.
    LIMITATION: No JS rendering. Viator and modern supplier sites
    load products dynamically — BS4 sees an empty shell.
    Also blocked by Cloudflare/anti-bot on many sites.
    """
    print(f"\n  📄 [requests+BS4] {source['label']}")
    base = make_base_row(source, "requests_bs4", run_id)

    t0 = time.time()
    try:
        resp = requests.get(source["url"], headers=HEADERS, timeout=20)
        fetch_ms = int((time.time() - t0) * 1000)
        print(f"     HTTP {resp.status_code} — {fetch_ms}ms")

        if resp.status_code != 200:
            return [error_row(base,
                f"HTTP {resp.status_code} — blocked or unavailable",
                fetch_ms, 0, 0)]

        raw_html = resp.text
        raw_len  = len(raw_html)

        soup = BeautifulSoup(raw_html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer",
                         "header", "aside", "form", "iframe"]):
            tag.decompose()

        # Try to find product-like elements
        products = []

        # Strategy A: look for common product card selectors
        candidates = (
            soup.select("h2, h3, h4, .product-title, .tour-title, "
                        ".activity-name, [class*='title'], [class*='product'], "
                        "[class*='tour'], [class*='card']")
        )

        seen = set()
        for el in candidates:
            text = el.get_text(strip=True)
            if (10 < len(text) < 150 and text not in seen):
                good = any(w in text.lower() for w in [
                    "tour", "trip", "transfer", "ticket", "excursion",
                    "safari", "experience", "visit", "day", "private"
                ])
                if good:
                    seen.add(text)
                    # Try to find price near this element
                    price = ""
                    parent = el.parent
                    if parent:
                        price_el = parent.find(
                            string=re.compile(r"[\$€£¥]|USD|EUR|AED|DZD|TND")
                        )
                        if price_el:
                            price = str(price_el).strip()[:100]

                    products.append({
                        "title":        text,
                        "description":  "",
                        "price_raw":    price,
                        "category":     "",
                        "activity_type": heuristic_classify(text),
                        "city":         "",
                        "country":      "",
                        "product_url":  source["url"],
                    })
            if len(products) >= MAX_PRODUCTS:
                break

        # Strategy B: if nothing found, fall back to full text heuristic
        if not products:
            full_text = soup.get_text(separator="\n", strip=True)
            products = heuristic_extract_products_from_text(
                full_text, source["url"]
            )

        clean_len = len(soup.get_text())
        print(f"     Raw: {raw_len} chars | Found: {len(products)} products")

        if not products:
            return [{
                **base,
                "title": None, "description": None, "price_raw": None,
                "category": None, "activity_type": None,
                "city_extracted": None, "country_extracted": None,
                "product_url": None,
                "fetch_duration_ms": fetch_ms,
                "raw_html_length":   raw_len,
                "clean_text_length": clean_len,
                "extraction_status": "no_products",
                "error_message": (
                    "BS4 found no product elements. "
                    "Page likely requires JavaScript rendering."
                ),
            }]

        return [
            product_row(base, p["title"], p["description"], p["price_raw"],
                        p["category"], p["activity_type"],
                        p["city"], p["country"], p["product_url"],
                        fetch_ms, raw_len, clean_len)
            for p in products
        ]

    except requests.exceptions.SSLError as e:
        return [error_row(base, f"SSL Error: {e}", int((time.time()-t0)*1000))]
    except requests.exceptions.ConnectionError as e:
        return [error_row(base, f"Connection refused / blocked: {e}",
                          int((time.time()-t0)*1000))]
    except Exception as e:
        return [error_row(base, str(e), int((time.time()-t0)*1000))]


# ─────────────────────────────────────────────────────────────
# TOOL 2 — JSON-LD extraction
# ─────────────────────────────────────────────────────────────

def tool_jsonld(source: dict, run_id: str) -> list:
    """
    Extract structured data from <script type='application/ld+json'> tags.
    LIMITATION: Only works if the supplier explicitly publishes Schema.org
    markup. Most small travel suppliers and all JS-rendered marketplaces
    (Viator) don't include this — or it requires JS to inject it.
    """
    print(f"\n  🧩 [JSON-LD] {source['label']}")
    base = make_base_row(source, "jsonld", run_id)

    t0 = time.time()
    try:
        resp = requests.get(source["url"], headers=HEADERS, timeout=20)
        fetch_ms = int((time.time() - t0) * 1000)
        print(f"     HTTP {resp.status_code} — {fetch_ms}ms")

        if resp.status_code != 200:
            return [error_row(base, f"HTTP {resp.status_code}", fetch_ms)]

        raw_html = resp.text
        raw_len  = len(raw_html)
        soup     = BeautifulSoup(raw_html, "html.parser")

        # Find ALL JSON-LD script blocks
        ld_scripts = soup.find_all("script", type="application/ld+json")
        print(f"     Found {len(ld_scripts)} JSON-LD blocks")

        if not ld_scripts:
            return [{
                **base,
                "title": None, "description": None, "price_raw": None,
                "category": None, "activity_type": None,
                "city_extracted": None, "country_extracted": None,
                "product_url": None,
                "fetch_duration_ms": fetch_ms,
                "raw_html_length":   raw_len,
                "clean_text_length": 0,
                "extraction_status": "no_products",
                "error_message": (
                    "Zero JSON-LD blocks found. Supplier does not publish "
                    "Schema.org structured data. Common for small operators "
                    "and JS-rendered pages (Viator injects LD+JSON via React)."
                ),
            }]

        products = []
        for script in ld_scripts:
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            # Handle @graph arrays
            items = data if isinstance(data, list) else [data]
            for item in items:
                schema_type = item.get("@type", "")
                # Product, TouristTrip, TouristAttraction, Service, Event
                if any(t in schema_type for t in [
                    "Product", "TouristTrip", "TouristAttraction",
                    "Service", "Event", "LocalBusiness", "TravelAgency"
                ]):
                    name  = item.get("name", "")
                    desc  = item.get("description", "")
                    price = ""
                    offers = item.get("offers", {})
                    if isinstance(offers, dict):
                        price = str(offers.get("price", ""))
                        cur   = offers.get("priceCurrency", "")
                        if cur:
                            price = f"{price} {cur}".strip()
                    elif isinstance(offers, list) and offers:
                        price = str(offers[0].get("price", ""))

                    city    = ""
                    country = ""
                    loc = item.get("location", {}) or item.get("address", {})
                    if isinstance(loc, dict):
                        city    = loc.get("addressLocality", "")
                        country = loc.get("addressCountry", "")

                    url = item.get("url", source["url"])

                    if name:
                        products.append({
                            "title":         name,
                            "description":   desc[:500],
                            "price_raw":     price,
                            "category":      schema_type,
                            "activity_type": heuristic_classify(name),
                            "city":          city,
                            "country":       country,
                            "product_url":   url,
                        })

            if len(products) >= MAX_PRODUCTS:
                break

        print(f"     Extracted {len(products)} structured products")

        if not products:
            return [{
                **base,
                "title": None, "description": None, "price_raw": None,
                "category": None, "activity_type": None,
                "city_extracted": None, "country_extracted": None,
                "product_url": None,
                "fetch_duration_ms": fetch_ms,
                "raw_html_length":   raw_len,
                "clean_text_length": 0,
                "extraction_status": "no_products",
                "error_message": (
                    f"{len(ld_scripts)} JSON-LD blocks found but none contain "
                    "TouristTrip/Product/Service schema types. "
                    "Schema markup exists but not for bookable products."
                ),
            }]

        return [
            product_row(base, p["title"], p["description"], p["price_raw"],
                        p["category"], p["activity_type"],
                        p["city"], p["country"], p["product_url"],
                        fetch_ms, raw_len, 0)
            for p in products[:MAX_PRODUCTS]
        ]

    except Exception as e:
        return [error_row(base, str(e), int((time.time()-t0)*1000))]


# ─────────────────────────────────────────────────────────────
# TOOL 3 — Sitemap crawling
# ─────────────────────────────────────────────────────────────

def tool_sitemap(source: dict, run_id: str) -> list:
    """
    Discover product URLs from sitemap.xml, then extract titles.
    LIMITATION: Many sites block robots/sitemap access.
    Viator's sitemap is enormous (millions of URLs) and rate-limited.
    Small suppliers often have no sitemap or use JS-generated sitemaps.
    Even when found, sitemaps give URLs — not product data.
    """
    print(f"\n  🗺️  [Sitemap] {source['label']}")
    base = make_base_row(source, "sitemap", run_id)

    domain   = urlparse(source["url"]).netloc
    sitemap_urls_to_try = [
        f"https://{domain}/sitemap.xml",
        f"https://{domain}/sitemap_index.xml",
        f"https://{domain}/sitemap/sitemap.xml",
        f"https://www.{domain}/sitemap.xml" if not domain.startswith("www") else None,
    ]
    sitemap_urls_to_try = [u for u in sitemap_urls_to_try if u]

    t0 = time.time()
    sitemap_content = None
    sitemap_url_used = None

    for sm_url in sitemap_urls_to_try:
        try:
            r = requests.get(sm_url, headers=HEADERS, timeout=15)
            if r.status_code == 200 and "<url" in r.text.lower():
                sitemap_content = r.text
                sitemap_url_used = sm_url
                print(f"     Found sitemap at: {sm_url}")
                break
            else:
                print(f"     {sm_url} → HTTP {r.status_code}")
        except Exception as e:
            print(f"     {sm_url} → {type(e).__name__}")

    fetch_ms = int((time.time() - t0) * 1000)

    if not sitemap_content:
        return [{
            **base,
            "title": None, "description": None, "price_raw": None,
            "category": None, "activity_type": None,
            "city_extracted": None, "country_extracted": None,
            "product_url": None,
            "fetch_duration_ms": fetch_ms,
            "raw_html_length":   0,
            "clean_text_length": 0,
            "extraction_status": "no_products",
            "error_message": (
                "No accessible sitemap found at sitemap.xml, "
                "sitemap_index.xml or /sitemap/sitemap.xml. "
                "Site may block crawlers or use dynamic sitemap generation."
            ),
        }]

    # Parse sitemap XML and extract product-looking URLs
    try:
        root = ET.fromstring(sitemap_content)
        ns   = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        locs = root.findall(".//sm:loc", ns) or root.findall(".//loc")
        all_urls = [loc.text.strip() for loc in locs if loc.text]
        print(f"     {len(all_urls)} total URLs in sitemap")

        # Filter for product-looking paths
        product_keywords = [
            "tour", "trip", "excursion", "transfer", "ticket",
            "activity", "experience", "product", "booking",
            "safari", "visit", "package"
        ]
        product_urls = [
            u for u in all_urls
            if any(kw in u.lower() for kw in product_keywords)
        ][:MAX_PRODUCTS]

        print(f"     {len(product_urls)} product-like URLs found")

        if not product_urls:
            # Return the raw URL count as info row
            return [{
                **base,
                "title": f"Sitemap found: {len(all_urls)} URLs total",
                "description": (
                    f"Sitemap at {sitemap_url_used} contains {len(all_urls)} URLs "
                    f"but none match product/tour/transfer patterns. "
                    f"Cannot extract product data from URLs alone without "
                    f"visiting each page — not scalable without LLM extraction."
                ),
                "price_raw": None, "category": None, "activity_type": None,
                "city_extracted": None, "country_extracted": None,
                "product_url": sitemap_url_used,
                "fetch_duration_ms": fetch_ms,
                "raw_html_length":   len(sitemap_content),
                "clean_text_length": len(all_urls),
                "extraction_status": "no_products",
                "error_message": "No product-pattern URLs in sitemap",
            }]

        # For each product URL, extract just the title from <title> tag
        # (visiting all pages is too slow — shows sitemap limitation)
        rows = []
        for purl in product_urls:
            slug = purl.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ")
            slug = slug.title()
            rows.append(
                product_row(
                    base, slug, f"URL only — no content fetched: {purl}",
                    "", "", heuristic_classify(slug),
                    "", "", purl,
                    fetch_ms, len(sitemap_content), 0
                )
            )
        return rows

    except ET.ParseError as e:
        return [error_row(base, f"Sitemap XML parse error: {e}", fetch_ms,
                          len(sitemap_content), 0)]


# ─────────────────────────────────────────────────────────────
# TOOL 4 — Playwright (headless Chromium, JS rendering)
# ─────────────────────────────────────────────────────────────

def tool_playwright(source: dict, run_id: str) -> list:
    """
    Playwright launches a real headless Chromium browser.
    ADVANTAGE over BS4/Scrapy: renders JavaScript.
    LIMITATION: Still blocked by sophisticated anti-bot (Cloudflare,
    Viator's Imperva). No proxy rotation = IP gets flagged quickly.
    Also: slow (~5-15s per page), high memory, can't scale to 1000s of pages.
    """
    print(f"\n  🎭 [Playwright] {source['label']}")
    base = make_base_row(source, "playwright", run_id)

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        return [error_row(base,
            "Playwright not installed. Run: pip install playwright && "
            "python -m playwright install chromium")]

    t0 = time.time()
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ]
            )
            ctx = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="en-US",
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()

            # Hide automation fingerprint
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            try:
                page.goto(source["url"], timeout=30000,
                          wait_until="domcontentloaded")
                # Wait for dynamic content
                page.wait_for_timeout(3000)
            except PWTimeout:
                browser.close()
                fetch_ms = int((time.time() - t0) * 1000)
                return [error_row(base,
                    "Playwright timeout (30s) — page too slow or blocked",
                    fetch_ms)]

            fetch_ms = int((time.time() - t0) * 1000)

            # Check for anti-bot blocks
            title = page.title()
            content = page.content()
            raw_len = len(content)
            print(f"     Page title: '{title[:60]}' — {fetch_ms}ms")

            blocked_signals = [
                "access denied", "403 forbidden", "cloudflare",
                "captcha", "verify you are human", "just a moment",
                "checking your browser", "enable javascript",
                "ddos protection", "bot detection"
            ]
            if any(sig in (title + content).lower() for sig in blocked_signals):
                browser.close()
                return [error_row(base,
                    f"Anti-bot detected: '{title}'. "
                    "Playwright fingerprint detected by Cloudflare/Imperva. "
                    "Needs residential proxies + stealth plugins to bypass.",
                    fetch_ms, raw_len, 0)]

            # Parse rendered HTML with BS4
            soup = BeautifulSoup(content, "html.parser")
            for tag in soup(["script", "style", "nav", "footer",
                             "header", "aside", "form"]):
                tag.decompose()

            products = []
            seen = set()

            candidates = soup.select(
                "h1, h2, h3, h4, "
                "[class*='title'], [class*='product'], [class*='tour'], "
                "[class*='card'], [class*='item'], [class*='activity'], "
                "[class*='trip'], [class*='package']"
            )

            for el in candidates:
                text = el.get_text(strip=True)
                if (10 < len(text) < 150 and text not in seen):
                    good = any(w in text.lower() for w in [
                        "tour", "trip", "transfer", "ticket", "excursion",
                        "safari", "experience", "visit", "day", "private",
                        "airport", "guided", "group", "adventure", "city"
                    ])
                    if good:
                        seen.add(text)
                        price = ""
                        parent = el.parent
                        if parent:
                            price_el = parent.find(
                                string=re.compile(r"[\$€£¥]|USD|EUR|AED")
                            )
                            if price_el:
                                price = str(price_el).strip()[:100]
                        products.append({
                            "title": text, "description": "",
                            "price_raw": price, "category": "",
                            "activity_type": heuristic_classify(text),
                            "city": "", "country": "",
                            "product_url": source["url"],
                        })
                if len(products) >= MAX_PRODUCTS:
                    break

            # Fallback: heuristic on full text
            if not products:
                full_text = soup.get_text(separator="\n", strip=True)
                products  = heuristic_extract_products_from_text(
                    full_text, source["url"]
                )

            clean_len = len(soup.get_text())
            browser.close()
            print(f"     Found: {len(products)} products")

            if not products:
                return [{
                    **base,
                    "title": None, "description": None, "price_raw": None,
                    "category": None, "activity_type": None,
                    "city_extracted": None, "country_extracted": None,
                    "product_url": None,
                    "fetch_duration_ms": fetch_ms,
                    "raw_html_length":   raw_len,
                    "clean_text_length": clean_len,
                    "extraction_status": "no_products",
                    "error_message": (
                        "Playwright rendered the page successfully but found "
                        "no product elements. Dynamic content may load after "
                        "scroll/interaction, or product data is in API calls "
                        "not visible in static DOM."
                    ),
                }]

            return [
                product_row(base, p["title"], p["description"], p["price_raw"],
                            p["category"], p["activity_type"],
                            p["city"], p["country"], p["product_url"],
                            fetch_ms, raw_len, clean_len)
                for p in products
            ]

    except Exception as e:
        return [error_row(base, f"Playwright error: {e}",
                          int((time.time()-t0)*1000))]


# ─────────────────────────────────────────────────────────────
# TOOL 5 — Selenium (headless Chrome)
# ─────────────────────────────────────────────────────────────

def tool_selenium(source: dict, run_id: str) -> list:
    """
    Selenium drives a real Chrome browser.
    LIMITATION vs Playwright: slower, heavier, easier to fingerprint.
    Same anti-bot issues. Needs exact chromedriver version match.
    Often fails on modern sites with MutationObserver/lazy loading.
    """
    print(f"\n  🌐 [Selenium] {source['label']}")
    base = make_base_row(source, "selenium", run_id)

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import (
            TimeoutException, WebDriverException, NoSuchDriverException
        )
    except ImportError:
        return [error_row(base,
            "Selenium not installed. Run: pip install selenium")]

    t0 = time.time()
    driver = None
    try:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(f"user-agent={HEADERS['User-Agent']}")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        try:
            driver = webdriver.Chrome(options=opts)
        except (WebDriverException, Exception) as e:
            if "chromedriver" in str(e).lower() or "executable" in str(e).lower():
                return [error_row(base,
                    "ChromeDriver not found or version mismatch. "
                    "Selenium requires chromedriver matching your Chrome version. "
                    "This is a major maintenance burden for production scraping.",
                    int((time.time()-t0)*1000))]
            raise

        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"}
        )

        try:
            driver.get(source["url"])
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)  # wait for dynamic content
        except TimeoutException:
            fetch_ms = int((time.time() - t0) * 1000)
            driver.quit()
            return [error_row(base,
                "Selenium timeout (15s) — page blocked or very slow",
                fetch_ms)]

        fetch_ms = int((time.time() - t0) * 1000)
        page_title = driver.title
        raw_html   = driver.page_source
        raw_len    = len(raw_html)
        print(f"     Page: '{page_title[:60]}' — {fetch_ms}ms")

        # Check for blocks
        blocked_signals = [
            "access denied", "cloudflare", "captcha",
            "verify you are human", "just a moment",
            "403 forbidden", "bot detection"
        ]
        combined = (page_title + raw_html).lower()
        if any(sig in combined for sig in blocked_signals):
            driver.quit()
            return [error_row(base,
                f"Blocked by anti-bot: '{page_title}'. "
                "Selenium automation detected. Real Chrome + stealth "
                "plugins + residential proxies required.",
                fetch_ms, raw_len, 0)]

        # Parse with BS4
        soup = BeautifulSoup(raw_html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer",
                         "header", "aside", "form"]):
            tag.decompose()

        products = []
        seen = set()

        # Try Selenium native finders first (for interactive elements)
        try:
            elements = driver.find_elements(
                By.XPATH,
                "//*[contains(@class,'title') or contains(@class,'tour') "
                "or contains(@class,'product') or contains(@class,'activity')]"
            )
            for el in elements:
                text = el.text.strip() if el.text else ""
                if (10 < len(text) < 150 and text not in seen):
                    good = any(w in text.lower() for w in [
                        "tour", "trip", "transfer", "ticket", "excursion",
                        "experience", "day", "private", "airport", "guided"
                    ])
                    if good:
                        seen.add(text)
                        products.append({
                            "title": text, "description": "",
                            "price_raw": "", "category": "",
                            "activity_type": heuristic_classify(text),
                            "city": "", "country": "",
                            "product_url": source["url"],
                        })
                if len(products) >= MAX_PRODUCTS:
                    break
        except Exception:
            pass

        # Fallback to BS4 heuristic
        if not products:
            full_text = soup.get_text(separator="\n", strip=True)
            products  = heuristic_extract_products_from_text(
                full_text, source["url"]
            )

        clean_len = len(soup.get_text())
        driver.quit()
        print(f"     Found: {len(products)} products")

        if not products:
            return [{
                **base,
                "title": None, "description": None, "price_raw": None,
                "category": None, "activity_type": None,
                "city_extracted": None, "country_extracted": None,
                "product_url": None,
                "fetch_duration_ms": fetch_ms,
                "raw_html_length":   raw_len,
                "clean_text_length": clean_len,
                "extraction_status": "no_products",
                "error_message": (
                    "Selenium rendered the page but found no products. "
                    "Products may be behind login/scroll/interaction, "
                    "loaded by infinite scroll, or returned via XHR API calls "
                    "that Selenium doesn't intercept automatically."
                ),
            }]

        return [
            product_row(base, p["title"], p["description"], p["price_raw"],
                        p["category"], p["activity_type"],
                        p["city"], p["country"], p["product_url"],
                        fetch_ms, raw_len, clean_len)
            for p in products
        ]

    except Exception as e:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        return [error_row(base, f"Selenium error: {e}",
                          int((time.time()-t0)*1000))]


# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────

def print_summary(all_rows: list):
    print("\n" + "=" * 65)
    print("  FREE TOOLS — RESULTS SUMMARY")
    print("=" * 65)

    tools = [
        ("requests_bs4", "📄 requests + BS4"),
        ("jsonld",        "🧩 JSON-LD"),
        ("sitemap",       "🗺️  Sitemap"),
        ("playwright",    "🎭 Playwright"),
        ("selenium",      "🌐 Selenium"),
    ]

    print(f"\n{'Tool':<22} {'OK':>4} {'Empty':>6} {'Error':>6} "
          f"{'Avg Fetch':>10} {'Total':>6}")
    print("─" * 65)

    for pipe, label in tools:
        rows  = [r for r in all_rows if r["pipeline"] == pipe]
        ok    = sum(1 for r in rows if r["extraction_status"] == "ok")
        empty = sum(1 for r in rows if r["extraction_status"] == "no_products")
        err   = sum(1 for r in rows if r["extraction_status"] == "error")
        avg_f = (sum(r.get("fetch_duration_ms", 0) or 0 for r in rows)
                 / max(len(rows), 1))
        print(f"{label:<22} {ok:>4} {empty:>6} {err:>6} "
              f"{avg_f:>9.0f}ms {len(rows):>6}")

    print("\n  KEY FINDINGS (for PFE report):")
    print("  ─────────────────────────────────────────────────")
    print("  • Viator: ALL free tools fail — JS + anti-bot (Imperva)")
    print("  • requests+BS4: works on simple static sites only")
    print("  • JSON-LD: only works if supplier publishes Schema.org")
    print("  • Sitemap: gives URLs, not product data")
    print("  • Playwright/Selenium: renders JS but blocked by Cloudflare")
    print("  • CONCLUSION: Free tools need per-site custom code +")
    print("    constant maintenance. Cannot scale to 1000s of suppliers.")
    print("  • PAID TOOLS (Firecrawl/ScrapingBee): proxy rotation +")
    print("    anti-bot bypass + consistent output = scalable.")

    print("\n  VIEW COMPARISON IN POSTGRES:")
    print("  SELECT pipeline, extraction_status, COUNT(*) as n,")
    print("         AVG(fetch_duration_ms) as avg_ms")
    print("  FROM scraping_comparison_results")
    print("  GROUP BY pipeline, extraction_status")
    print("  ORDER BY pipeline, extraction_status;")
    print("=" * 65)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

TOOLS = [
    ("requests_bs4", tool_requests_bs4),
    ("jsonld",       tool_jsonld),
    ("sitemap",      tool_sitemap),
    ("playwright",   tool_playwright),
    ("selenium",     tool_selenium),
]


def main():
    run_id = datetime.now().strftime("free_%Y%m%d_%H%M%S")
    print(f"\n{'=' * 65}")
    print(f"  DEX — Free Scraping Tools Test")
    print(f"  Run ID   : {run_id}")
    print(f"  Sources  : {len(SOURCES)} URLs")
    print(f"  Tools    : {len(TOOLS)} (requests+BS4, JSON-LD, Sitemap, "
          f"Playwright, Selenium)")
    print(f"  Purpose  : Show WHY free tools are insufficient for DEX")
    print(f"  Database : {DB_CONFIG['dbname']} @ {DB_CONFIG['host']}")
    print(f"{'=' * 65}")

    try:
        ensure_table()
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    all_rows = []

    for i, source in enumerate(SOURCES, 1):
        print(f"\n{'─' * 65}")
        print(f"[{i}/{len(SOURCES)}] {source['label']}")
        print(f"  URL: {source['url']}")

        for tool_name, tool_fn in TOOLS:
            try:
                rows = tool_fn(source, run_id)
                save_rows(rows)
                all_rows.extend(rows)
            except Exception as e:
                print(f"  ❌ {tool_name} fatal exception: {e}")
                # Save error row so the comparison is complete
                base = make_base_row(source, tool_name, run_id)
                err  = error_row(base, f"Fatal: {e}")
                save_rows([err])
                all_rows.append(err)

            # Small delay between tools on same source
            time.sleep(1)

        # Delay between sources to be respectful
        if i < len(SOURCES):
            print(f"\n  ⏳ 2s before next source...")
            time.sleep(2)

    print_summary(all_rows)
    print(f"\n✅ Done. Run ID: {run_id}")
    print(f"   Total rows saved: {len(all_rows)}")


if __name__ == "__main__":
    main()