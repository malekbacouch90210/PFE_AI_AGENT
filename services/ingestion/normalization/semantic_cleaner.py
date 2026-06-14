"""
services/ingestion/normalization/semantic_cleaner.py
PRE-NORMALIZATION CLEANING LAYER — Sprint 4

This is the most important new addition in Sprint 4.
It runs BEFORE AI normalization to fix the GARBAGE IN → GARBAGE OUT problem.

Problems it solves (from document):
  ❌ "De Se", "En Se"  → navigation/selector pollution
  ❌ "Check out our availability..." → booking UI noise
  ❌ "Golf in Djerba" → NAUTIQUE (wrong — golf is SPORT)
  ❌ Stockholm supplier → Norway detected (wrong country)
  ❌ Footer contamination
  ❌ SEO text contamination
  ❌ Mixed language titles
  ❌ HTML/JS remnants in extracted text

Architecture:
  URL/HTML
    → Crawl4AI AsyncWebCrawler (fit_markdown)
    → SemanticCleaner.clean_product()
    → CleanedProduct dict
    → NormalizationPipeline

Crawl4AI vs BS4:
  BS4 = HTML parser → gets everything including nav/footer/booking UI
  Crawl4AI = AI-aware markdown → strips boilerplate, keeps semantic content

Install:
  pip install -U crawl4ai
  crawl4ai-setup
  crawl4ai-doctor
"""

import asyncio
import re
from dataclasses import dataclass, field
from typing import Optional

from loguru import logger

from async_utils import proactor_worker
# ─────────────────────────────────────────────────────────────
# CleanedProduct — output of the cleaning layer
# ─────────────────────────────────────────────────────────────

@dataclass
class CleanedProduct:
    """
    Semantically cleaned product ready for AI normalization.
    All fields are clean strings — no HTML, no noise, no navigation.
    """
    clean_title:       str            = ""
    clean_description: str            = ""
    price_text:        str            = ""      # raw price string for parser
    duration_text:     str            = ""      # raw duration string for parser
    city_mentions:     list           = field(default_factory=list)
    country_mentions:  list           = field(default_factory=list)
    activity_keywords: list           = field(default_factory=list)
    image_urls:        list           = field(default_factory=list)
    markdown:          str            = ""      # full fit_markdown output
    source:            str            = "bs4"   # "crawl4ai" | "bs4" | "cached"
    confidence:        float          = 0.0
    was_cleaned:       bool           = False


# ─────────────────────────────────────────────────────────────
# Noise patterns to strip from extracted text
# ─────────────────────────────────────────────────────────────

# Booking UI noise — these appear in descriptions but are NOT content
BOOKING_NOISE_PATTERNS = [
    r"check\s+out\s+our\s+availability.*",
    r"book\s+now.*",
    r"reserve\s+now.*",
    r"add\s+to\s+cart.*",
    r"select\s+date.*",
    r"check\s+availability.*",
    r"from\s+\$[\d.]+\s+per\s+person",
    r"starting\s+from\s+[\d.]+",
    r"free\s+cancellation.*",
    r"instant\s+confirmation.*",
    r"reserve\s+your\s+spot.*",
    r"why\s+book\s+with\s+us.*",
    r"customer\s+reviews.*",
    r"what\s+to\s+expect.*",
    r"what['']s\s+included.*",
    r"what['']s\s+not\s+included.*",
    r"departure\s+point.*",
    r"return\s+details.*",
    r"accessibility.*",
    r"additional\s+info.*",
    r"gift\s+this\s+experience.*",
]

# Navigation contamination — path-like strings that slip into titles
NAVIGATION_NOISE = [
    r"^de\s+se\b",
    r"^en\s+se\b",
    r"^se\s+",
    r"^\d+\s*results?$",
    r"^home\s*[>\|]",
    r"^skip\s+to\s+(main|content|nav)",
    r"^toggle\s+",
    r"^menu\b",
    r"^navigation\b",
    r"language\s+selector",
    r"^cookie\s+",
    r"^gdpr\b",
    r"^\s*[\|>»]\s*",
    r"^share\s+(this|on|via)",
    r"^back\s+to\s+",
]

# SEO contamination
SEO_NOISE_PATTERNS = [
    r"best\s+\w+\s+in\s+\w+\s+\d{4}",
    r"top\s+\d+\s+things\s+to\s+do",
    r"ultimate\s+guide\s+to\s+",
    r"everything\s+you\s+need\s+to\s+know",
    r"updated\s+\w+\s+\d{4}",
    r"last\s+updated:",
]

# Title minimum quality — reject if shorter than this
MIN_TITLE_LEN = 10
MAX_TITLE_LEN = 300

# Description minimum quality
MIN_DESC_LEN = 30


# ─────────────────────────────────────────────────────────────
# SemanticCleaner
# ─────────────────────────────────────────────────────────────

class SemanticCleaner:
    """
    Pre-normalization cleaning layer using Crawl4AI.

    Two modes:
      MODE 1 — URL available: Crawl4AI fetches + cleans via fit_markdown
      MODE 2 — HTML/text available: rule-based cleaning only (fallback)

    Crawl4AI is NEVER a replacement for the scraper.
    It is a CLEANING LAYER that runs before AI normalization.
    """

    def __init__(self):
        self._crawler = None    # lazy init — Crawl4AI browser is heavy

    # ──────────────────────────────────────────────────────────
    # Main entry point
    # ──────────────────────────────────────────────────────────

    async def clean_product(
        self,
        url: Optional[str] = None,
        raw_title: Optional[str] = None,
        raw_description: Optional[str] = None,
        raw_html: Optional[str] = None,
    ) -> CleanedProduct:
        """
        Clean a product's content before AI normalization.

        Priority:
          1. URL → Crawl4AI fit_markdown (best quality)
          2. HTML → rule-based cleaning (fallback)
          3. text → rule-based cleaning (last resort)
        """
        # Try Crawl4AI if URL is available
        if url:
            result = await self._clean_via_crawl4ai(url)
            if result and result.was_cleaned:
                return result

        # Fallback to rule-based cleaning
        return self._clean_via_rules(
            raw_title       = raw_title or "",
            raw_description = raw_description or "",
            raw_html        = raw_html or "",
            url             = url or "",
        )

    # ──────────────────────────────────────────────────────────
    # Mode 1 — Crawl4AI semantic extraction
    # ──────────────────────────────────────────────────────────

    async def _clean_via_crawl4ai(self, url: str) -> Optional[CleanedProduct]:
        """
        Use Crawl4AI to extract clean semantic markdown from product URL.
        Executes entirely inside the background Proactor worker thread.
        """
        try:
            from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
            from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
            from crawl4ai.content_filter_strategy import BM25ContentFilter

            # BM25 filter — keeps tourism-relevant content only
            bm25_filter = BM25ContentFilter(
                user_query=(
                    "tour excursion activity description price duration "
                    "transfer ticket museum included"
                ),
                bm25_threshold=1.2,
            )

            md_generator = DefaultMarkdownGenerator(
                content_filter=bm25_filter
            )

            browser_cfg = BrowserConfig(
                headless=True,
                verbose=False,
            )
            run_cfg = CrawlerRunConfig(
                markdown_generator=md_generator,
                word_count_threshold=10,
                exclude_external_links=True,
                remove_overlay_elements=True,
                process_iframes=False,
                wait_until="domcontentloaded",
                page_timeout=15000,
            )

            # Define a completely isolated worker coroutine
            async def worker_task():
                async with AsyncWebCrawler(config=browser_cfg) as crawler:
                    return await crawler.arun(url=url, config=run_cfg)

            # Ship the entire execution payload over to the Proactor thread
            result = await asyncio.get_running_loop().run_in_executor(
                None,
                proactor_worker.run_coro,
                worker_task()
            )

            if not result or not result.success:
                error_msg = result.error_message if result else "No result payload"
                logger.debug(f"    Crawl4AI failed for {url}: {error_msg}")
                return None

            # Prefer fit_markdown (noise-filtered) over raw_markdown
            markdown = (
                result.markdown.fit_markdown
                if hasattr(result.markdown, "fit_markdown") and result.markdown.fit_markdown
                else result.markdown.raw_markdown
                if hasattr(result.markdown, "raw_markdown")
                else str(result.markdown)
            )

            if not markdown or len(markdown) < 50:
                return None

            return self._parse_markdown_to_product(markdown, url)

        except ImportError:
            logger.warning(
                "⚠️ Crawl4AI not installed. Run: pip install -U crawl4ai && crawl4ai-setup"
            )
            return None
        except Exception as e:
            logger.debug(f"    Crawl4AI exception {url}: {e}")
            return None

    def _parse_markdown_to_product(
        self, markdown: str, url: str
    ) -> CleanedProduct:
        """
        Parse Crawl4AI fit_markdown output into CleanedProduct.
        Markdown is already clean — just extract structured fields.
        """
        lines = markdown.split("\n")

        # Title: first H1 or H2 heading
        title = ""
        desc_lines = []
        in_desc = False

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # H1 heading → title candidate
            if line.startswith("# ") and not title:
                candidate = line[2:].strip()
                if self._is_valid_title(candidate):
                    title = candidate
                    in_desc = True
                continue

            # H2 → secondary title fallback
            if line.startswith("## ") and not title:
                candidate = line[3:].strip()
                if self._is_valid_title(candidate):
                    title = candidate
                    in_desc = True
                continue

            # Description lines — skip navigation noise
            if in_desc and len(desc_lines) < 20:
                if self._is_valid_description_line(line):
                    desc_lines.append(line)

        # If no heading found → use first valid paragraph as title
        if not title and desc_lines:
            title = desc_lines.pop(0)

        description = " ".join(desc_lines)

        # Apply noise cleaning to both
        title       = self._clean_title(title)
        description = self._clean_description(description)

        # Extract price mentions
        price_text = self._extract_price_text(markdown)

        # Extract duration mentions
        duration_text = self._extract_duration_text(markdown)

        # Extract geo mentions
        city_mentions    = self._extract_city_mentions(markdown)
        country_mentions = self._extract_country_mentions(markdown)

        # Extract activity keywords
        activity_keywords = self._extract_activity_keywords(markdown)

        # Extract images from markdown links
        image_urls = re.findall(r"!\[.*?\]\((https?://[^\)]+)\)", markdown)

        confidence = self._compute_clean_confidence(
            title, description, price_text, city_mentions
        )

        return CleanedProduct(
            clean_title       = title[:MAX_TITLE_LEN],
            clean_description = description[:2000],
            price_text        = price_text,
            duration_text     = duration_text,
            city_mentions     = city_mentions[:5],
            country_mentions  = country_mentions[:3],
            activity_keywords = activity_keywords[:10],
            image_urls        = image_urls[:5],
            markdown          = markdown[:3000],
            source            = "crawl4ai",
            confidence        = confidence,
            was_cleaned       = True,
        )

    # ──────────────────────────────────────────────────────────
    # Mode 2 — Rule-based cleaning (fallback)
    # ──────────────────────────────────────────────────────────

    def _clean_via_rules(
        self,
        raw_title: str,
        raw_description: str,
        raw_html: str,
        url: str,
    ) -> CleanedProduct:
        """
        Rule-based cleaning when Crawl4AI is unavailable or fails.
        More aggressive noise removal than simple BS4 parsing.
        """
        # Strip HTML
        if raw_html and len(raw_html) > len(raw_description):
            raw_description = _strip_html(raw_html)

        title       = self._clean_title(raw_title)
        description = self._clean_description(raw_description)

        combined = f"{title} {description}"

        return CleanedProduct(
            clean_title       = title[:MAX_TITLE_LEN],
            clean_description = description[:2000],
            price_text        = self._extract_price_text(combined),
            duration_text     = self._extract_duration_text(combined),
            city_mentions     = self._extract_city_mentions(combined),
            country_mentions  = self._extract_country_mentions(combined),
            activity_keywords = self._extract_activity_keywords(combined),
            image_urls        = [],
            markdown          = "",
            source            = "rules",
            confidence        = self._compute_clean_confidence(
                title, description, "", []
            ),
            was_cleaned       = bool(title and description),
        )

    # ──────────────────────────────────────────────────────────
    # Title cleaning
    # ──────────────────────────────────────────────────────────

    def _clean_title(self, title: str) -> str:
        if not title:
            return ""

        title = _strip_html(title).strip()

        # Remove navigation noise patterns
        for pat in NAVIGATION_NOISE:
            if re.search(pat, title, re.IGNORECASE):
                title = re.sub(pat, "", title, flags=re.IGNORECASE).strip()

        # Remove leading/trailing separators
        title = re.sub(r"^[\s\|>\-»]+|[\s\|>\-«]+$", "", title).strip()

        # Fix "De Se" type corruption — strip lone 2-char words at start
        title = re.sub(r"^[A-Z][a-z]\s+[A-Z][a-z]\s+", "", title)

        # Normalize whitespace
        title = re.sub(r"\s+", " ", title).strip()

        if len(title) < MIN_TITLE_LEN:
            return ""

        return title

    # ──────────────────────────────────────────────────────────
    # Description cleaning
    # ──────────────────────────────────────────────────────────

    def _clean_description(self, desc: str) -> str:
        if not desc:
            return ""

        desc = _strip_html(desc).strip()

        # Remove booking UI noise
        for pat in BOOKING_NOISE_PATTERNS:
            desc = re.sub(pat, " ", desc, flags=re.IGNORECASE)

        # Remove SEO contamination
        for pat in SEO_NOISE_PATTERNS:
            desc = re.sub(pat, " ", desc, flags=re.IGNORECASE)

        # Remove very short sentences (often fragment artifacts)
        sentences = desc.split(".")
        good_sentences = [
            s.strip() for s in sentences
            if len(s.strip()) > 20
        ]
        desc = ". ".join(good_sentences)

        # Normalize whitespace
        desc = re.sub(r"\s+", " ", desc).strip()

        if len(desc) < MIN_DESC_LEN:
            return ""

        return desc

    # ──────────────────────────────────────────────────────────
    # Extraction helpers
    # ──────────────────────────────────────────────────────────

    def _extract_price_text(self, text: str) -> str:
        """Extract first price mention from text."""
        patterns = [
            r"(?:from|à partir de|depuis|prix|price|cost|tarif)[\s:]*"
            r"([\d,\.]+\s*(?:EUR|USD|€|\$|£|TND|MAD)?)",
            r"([\d,\.]+\s*(?:EUR|USD|€|\$|£|TND|MAD))",
            r"(?:€|\$|£)([\d,\.]+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return m.group(0).strip()[:50]
        return ""

    def _extract_duration_text(self, text: str) -> str:
        """Extract first duration mention from text."""
        patterns = [
            r"(\d+(?:\.\d+)?)\s*(?:hour[s]?|heure[s]?|hr[s]?|h)\b",
            r"(\d+)\s*(?:minute[s]?|min)\b",
            r"(\d+)\s*(?:day[s]?|jour[s]?|jours?)\b",
            r"(?:duration|durée|dure|lasting)[\s:]*([^\.\n]{3,30})",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return m.group(0).strip()[:30]
        return ""

    def _extract_city_mentions(self, text: str) -> list:
        """Extract city names from product text — light heuristic."""
        # Look for "in [City]", "from [City]", "[City] tour/excursion"
        patterns = [
            r"\bin\s+([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)?)\b",
            r"\bfrom\s+([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)?)\b",
            r"([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)?)\s+(?:tour|excursion|transfer|trip)",
        ]
        cities = []
        seen = set()
        for pat in patterns:
            for m in re.finditer(pat, text):
                city = m.group(1).strip()
                if city and city.lower() not in seen and len(city) > 2:
                    seen.add(city.lower())
                    cities.append(city)
        return cities[:5]

    def _extract_country_mentions(self, text: str) -> list:
        """Light country extraction."""
        from services.ingestion.normalization_pipeline import _FAST_COUNTRY_MAP
        countries = []
        text_lower = text.lower()
        for raw, english in _FAST_COUNTRY_MAP.items():
            if raw in text_lower and english not in countries:
                countries.append(english)
        return countries[:3]

    def _extract_activity_keywords(self, text: str) -> list:
        """Extract activity-relevant keywords."""
        ACTIVITY_KEYWORDS = [
            "tour", "excursion", "transfer", "ticket", "shuttle",
            "museum", "safari", "cruise", "hiking", "diving",
            "snorkeling", "guided", "private", "group", "airport",
            "whale", "dolphin", "boat", "kayak", "cultural",
            "adventure", "cooking", "wine", "food", "historical",
        ]
        text_lower = text.lower()
        return [kw for kw in ACTIVITY_KEYWORDS if kw in text_lower]

    # ──────────────────────────────────────────────────────────
    # Validation helpers
    # ──────────────────────────────────────────────────────────

    def _is_valid_title(self, text: str) -> bool:
        if not text or len(text) < MIN_TITLE_LEN:
            return False
        for pat in NAVIGATION_NOISE:
            if re.search(pat, text, re.IGNORECASE):
                return False
        # Reject if mostly non-alpha (navigation/code artifacts)
        alpha_ratio = sum(c.isalpha() for c in text) / max(len(text), 1)
        return alpha_ratio > 0.5

    def _is_valid_description_line(self, line: str) -> bool:
        if len(line) < 15:
            return False
        for pat in BOOKING_NOISE_PATTERNS:
            if re.search(pat, line, re.IGNORECASE):
                return False
        for pat in NAVIGATION_NOISE:
            if re.search(pat, line, re.IGNORECASE):
                return False
        return True

    def _compute_clean_confidence(
        self,
        title: str,
        description: str,
        price_text: str,
        city_mentions: list,
    ) -> float:
        score = 0.0
        if title and len(title) >= MIN_TITLE_LEN:
            score += 0.35
        if description and len(description) >= MIN_DESC_LEN:
            score += 0.35
        if price_text:
            score += 0.15
        if city_mentions:
            score += 0.15
        return round(min(score, 1.0), 2)

    async def close(self):
        if self._crawler:
            try:
                await self._crawler.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>",  " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = (text
            .replace("&amp;", "&").replace("&lt;", "<")
            .replace("&gt;", ">").replace("&nbsp;", " ")
            .replace("&quot;", '"').replace("&#39;", "'")
            .replace("&apos;", "'"))
    return re.sub(r"\s+", " ", text).strip()


# Singleton
semantic_cleaner = SemanticCleaner()
