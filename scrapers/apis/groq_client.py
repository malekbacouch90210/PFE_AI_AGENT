"""
scrapers/apis/groq_client.py
Groq Cloud API client — OpenAI-compatible, free tier.

Models chosen for product extraction:
  PRIMARY:   llama-3.1-8b-instant  → fast, low TPM usage, high RPM
  FALLBACK:  gemma2-9b-it          → alternative if llama hits limits
  AVOID:     llama-3.3-70b-versatile → too large, hits 12000 TPM limit fast

Free tier limits (per model per minute):
  llama-3.1-8b-instant:  20000 TPM, 30 RPM
  gemma2-9b-it:          15000 TPM, 30 RPM
  llama3-70b-8192:       6000  TPM, 30 RPM  ← avoid

Rate limit strategy:
  - Use 8b model as primary (highest TPM)
  - Chunk content to max 4000 tokens per request
  - 2s delay between requests per supplier
  - Exponential backoff on 429 errors
"""
import asyncio
import json
import os
import re
import time
from typing import Optional

import httpx
from loguru import logger

GROQ_BASE_URL = "https://api.groq.com/openai/v1"

# Model selection — ordered by preference
GROQ_MODELS = [
    "llama-3.1-8b-instant",   # Primary: highest TPM on free tier
    "gemma2-9b-it",           # Fallback 1
    "llama3-8b-8192",         # Fallback 2
]

# Max content chars to send per request (keeps us under TPM limit)
# ~4000 tokens ≈ ~16000 chars but we stay conservative
MAX_CONTENT_CHARS = 8000

# Delay between requests (seconds) — respect RPM=30 limit
REQUEST_DELAY = 2.1


class GroqClient:
    """
    Groq Cloud API client.
    OpenAI-compatible — used with Crawl4AI's LLMExtractionStrategy.
    """

    def __init__(self):
        self.api_key   = os.getenv("GROQ_API_KEY", "")
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time = 0.0

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_key.startswith("gsk_"))

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=GROQ_BASE_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type":  "application/json",
                },
                timeout=httpx.Timeout(60.0),
            )
        return self._client

    async def _rate_limit_wait(self):
        """Ensure minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            await asyncio.sleep(REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()

    async def extract_products(
        self,
        content: str,
        url: str,
        max_retries: int = 3,
    ) -> list:
        """
        Extract structured travel products from page content using Groq LLM.

        Args:
            content:     Clean text/markdown from Crawl4AI fit_markdown
            url:         Source URL (used for context)
            max_retries: Retry on rate limit errors

        Returns:
            List of product dicts matching TravelProductSchema
        """
        if not self.is_configured():
            logger.warning("⚠️ GROQ_API_KEY not set — skipping LLM extraction")
            return []

        # Truncate content to stay under TPM limit
        content_truncated = content[:MAX_CONTENT_CHARS]

        prompt = _build_extraction_prompt(content_truncated, url)
        client = await self._get_client()

        for attempt in range(max_retries):
            await self._rate_limit_wait()

            model = GROQ_MODELS[min(attempt, len(GROQ_MODELS) - 1)]

            try:
                resp = await client.post(
                    "/chat/completions",
                    json={
                        "model":       model,
                        "messages": [
                            {
                                "role":    "system",
                                "content": _SYSTEM_PROMPT,
                            },
                            {
                                "role":    "user",
                                "content": prompt,
                            }
                        ],
                        "temperature":   0.1,
                        "max_tokens":    2000,
                        "response_format": {"type": "json_object"},
                    }
                )

                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("retry-after", 4 ** attempt))
                    logger.warning(
                        f"  ⏳ Groq rate limit (attempt {attempt+1}/{max_retries}) "
                        f"— waiting {retry_after:.1f}s"
                    )
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status_code != 200:
                    logger.warning(f"  Groq {resp.status_code}: {resp.text[:200]}")
                    return []

                data    = resp.json()
                content_str = data["choices"][0]["message"]["content"]
                products    = _parse_groq_response(content_str)

                logger.debug(
                    f"  🤖 Groq [{model}] → {len(products)} products "
                    f"from {url[:50]}"
                )
                return products

            except Exception as e:
                logger.warning(f"  Groq attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        return []

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# ─────────────────────────────────────────────────────────────
# Prompts
# ─────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a travel product data extraction specialist.
Extract structured travel product information from website content.
Always respond with valid JSON only. No markdown, no explanation.
Be precise — only extract real products, not navigation or category pages."""


def _build_extraction_prompt(content: str, url: str) -> str:
    return f"""Extract all travel products from this website content.

SOURCE URL: {url}

WEBSITE CONTENT:
{content}

Extract each distinct travel product and return JSON in this exact format:
{{
  "products": [
    {{
      "nom_produit": "exact product title",
      "description": "brief description of what is included",
      "canonical_activity_type": "EXCURSION or TICKET or TRANSFER",
      "destination": "city or place where activity takes place",
      "pays_raw": "country name where activity takes place",
      "prix_raw_text": "price as shown on page, or null",
      "url_produit": "absolute URL to product page, or null",
      "duree": "duration if mentioned, or null",
      "origin": "pickup point for transfers, or null"
    }}
  ]
}}

RULES:
1. canonical_activity_type must be exactly: EXCURSION, TICKET, or TRANSFER
   - EXCURSION: tours, guided trips, day trips, safaris, activities, experiences
   - TICKET: museum entry, park admission, attraction ticket, skip-the-line
   - TRANSFER: airport transfer, shuttle, taxi, private driver
2. destination = city or specific place (e.g. "Athens", "Acropolis Museum")
3. pays_raw = country (e.g. "Greece", "Italy", "France")
4. Only include real products — skip navigation links, category pages, blog posts
5. If price visible: include it exactly as shown (e.g. "From € 79,90 per person")
6. url_produit: make absolute URL using source domain if relative path given
7. Minimum title length: 8 characters, minimum 2 words
8. Return empty products array [] if no real products found"""


def _parse_groq_response(content_str: str) -> list:
    """Parse Groq JSON response into product list."""
    try:
        # Strip any markdown fences if present
        clean = re.sub(r"```(?:json)?|```", "", content_str).strip()
        data  = json.loads(clean)

        products = data.get("products", [])
        if not isinstance(products, list):
            return []

        # Validate and clean each product
        valid = []
        for p in products:
            if not isinstance(p, dict):
                continue
            nom = (p.get("nom_produit") or "").strip()
            if not nom or len(nom) < 8 or len(nom.split()) < 2:
                continue
            # Skip error objects from Groq
            if p.get("error") is True:
                continue
            valid.append(p)

        return valid

    except json.JSONDecodeError as e:
        logger.debug(f"  Groq JSON parse failed: {e}")
        return []


# Singleton
groq_client = GroqClient()
