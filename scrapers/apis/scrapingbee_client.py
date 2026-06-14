"""
scrapers/apis/scrapingbee_client.py
ScrapingBee API client.
"""

import re
from typing import Optional, List, Dict

import httpx
from loguru import logger
from bs4 import BeautifulSoup

from config import config

SCRAPINGBEE_BASE = "https://app.scrapingbee.com/api/v1"

# Marketplace pages to extract operators from
MARKETPLACE_TEMPLATES = {
    "viator": "https://www.viator.com/searchResults/all?text={city}+{country}",
    "getyourguide": "https://www.getyourguide.com/s/?q={city}+{country}&et=ACTIVITY",
    "klook": "https://www.klook.com/en-US/search/?query={city}",
}


class ScrapingBeeClient:
    def __init__(self):
        self.api_key = config.SCRAPINGBEE_API_KEY
        self._client: Optional[httpx.AsyncClient] = None

    def _is_configured(self) -> bool:
        return bool(self.api_key and self.api_key != "")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(40.0))
        return self._client

    # ──────────────────────────────────────────────────────────
    # CASE A — Marketplace city discovery (Main Use)
    # ──────────────────────────────────────────────────────────

    async def discover_operators_from_marketplace(
        self,
        city: str,
        country: str,
        marketplace: str = "viator",
    ) -> List[Dict]:
        if not self._is_configured():
            logger.warning("⚠️ SCRAPINGBEE_API_KEY not configured")
            return []

        template = MARKETPLACE_TEMPLATES.get(marketplace.lower())
        if not template:
            logger.warning(f"Unknown marketplace: {marketplace}")
            return []

        url = template.format(
            city=city.replace(" ", "+"),
            country=country.replace(" ", "+")
        )

        # Stronger settings for Viator (most difficult)
        wait_time = 6000 if marketplace == "viator" else 4000
        use_premium = marketplace == "viator"

        html = await self._fetch(
            url,
            render_js=True,
            wait_ms=wait_time,
            premium_proxy=use_premium
        )

        if not html or len(html) < 1000:
            logger.warning(f"  {marketplace} returned very small HTML")
            return []

        operators = _extract_operators_from_html(html, marketplace)

        logger.info(f"  🏪 {marketplace} {city}: {len(operators)} operators found")
        return operators

    # ──────────────────────────────────────────────────────────
    # CASE B — Playwright fallback
    # ──────────────────────────────────────────────────────────

    async def render_page(
        self,
        url: str,
        wait_ms: int = 2000,
    ) -> Optional[str]:
        if not self._is_configured():
            return None
        return await self._fetch(url, render_js=True, wait_ms=wait_ms)

    # ──────────────────────────────────────────────────────────
    # Core fetch
    # ──────────────────────────────────────────────────────────

    async def _fetch(
        self,
        url: str,
        render_js: bool = True,
        wait_ms: int = 2000,
        premium_proxy: bool = False,
    ) -> Optional[str]:
        client = await self._get_client()
        params = {
            "api_key": self.api_key,
            "url": url,
            "render_js": "true" if render_js else "false",
            "wait": str(wait_ms),
            "block_ads": "true",
            "block_resources": "false",
        }
        if premium_proxy:
            params["premium_proxy"] = "true"

        try:
            resp = await client.get(SCRAPINGBEE_BASE, params=params)
            if resp.status_code == 200:
                return resp.text
            else:
                logger.warning(f"ScrapingBee {resp.status_code} for {url[:70]}")
                logger.warning(f"Response: {resp.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"ScrapingBee fetch error {url[:70]}: {e}")
            return None

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# ─────────────────────────────────────────────────────────────
# Improved Operator Extraction
# ─────────────────────────────────────────────────────────────

def _extract_operators_from_html(html: str, marketplace: str) -> List[Dict]:
    operators: List[Dict] = []
    seen: set = set()

    soup = BeautifulSoup(html, "html.parser")

    if marketplace == "viator":
        # === Improved Viator Extraction 2026 ===

        # 1. Look for supplier names in common patterns
        patterns = [
            "[class*='supplier']",
            "[class*='provider']",
            "[class*='operator']",
            "[class*='merchant']",
            ".activity-supplier",
            "[data-test*='supplier']",
            "[data-supplier]",
        ]

        for pattern in patterns:
            for el in soup.select(pattern):
                text = el.get_text(strip=True)
                if _is_valid_operator_name(text, seen):
                    seen.add(text)
                    operators.append({"nom": text, "domain": "", "source": "viator"})

        # 2. Text patterns like "by Company Name", "Supplied by ..."
        supplied_matches = re.findall(
            r'(?:Supplied by|by|Operator:|Presented by)\s*([A-Z][A-Za-z0-9&\s\.,\'-]+?)(?=\s{2,}|\.|,|$|<|Reviews|from|\d)',
            html, re.IGNORECASE
        )
        for match in supplied_matches:
            name = match.strip()
            if _is_valid_operator_name(name, seen):
                seen.add(name)
                operators.append({"nom": name, "domain": "", "source": "viator"})

        # 3. JSON-LD fallback
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "{}")
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict):
                        provider = item.get("provider") or item.get("organizer") or item.get("@graph", [{}])[0]
                        if isinstance(provider, dict):
                            name = provider.get("name", "").strip()
                            if _is_valid_operator_name(name, seen):
                                seen.add(name)
                                domain = _domain_from_url(provider.get("url", ""))
                                operators.append({"nom": name, "domain": domain, "source": "viator"})
            except:
                continue

    elif marketplace == "getyourguide":
        for el in soup.select("[class*='supplier'], [class*='provider'], .merchant-name, [data-test*='supplier']"):
            text = el.get_text(strip=True)
            if _is_valid_operator_name(text, seen):
                seen.add(text)
                operators.append({"nom": text, "domain": "", "source": "getyourguide"})

    elif marketplace == "klook":
        for el in soup.select("[class*='merchant'], [class*='provider'], [class*='operator']"):
            text = el.get_text(strip=True)
            if _is_valid_operator_name(text, seen):
                seen.add(text)
                operators.append({"nom": text, "domain": "", "source": "klook"})

    return operators[:25]  # Limit results


def _is_valid_operator_name(name: str, seen: set) -> bool:
    if not name or name in seen:
        return False
    name = name.strip()
    if len(name) < 4 or len(name) > 85:
        return False
    # Filter out noise
    noise = ["viator", "tripadvisor", "getyourguide", "klook", "book now", "from ", "€", "$", "reviews", "sold out"]
    return not any(n in name.lower() for n in noise)


def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    url = re.sub(r"https?://", "", url.strip().lower())
    url = re.sub(r"^www\.", "", url)
    return url.split("/")[0]