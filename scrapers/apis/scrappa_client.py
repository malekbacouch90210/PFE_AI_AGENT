# scrapers/apis/scrappa_client.py
"""
Scrappa.co API client
Base URL  : https://scrappa.co/api
Auth      : X-API-KEY header

Endpoints:
  Maps Advanced Search : GET /maps/advanced-search?query=...&zoom=13
  Google Search        : GET /search?query=...&hl=en
  Brave Search         : GET /brave/search?query=...
  YouTube Search       : GET /youtube/search?query=...
  Pinterest Search     : GET /pinterest/search?query=...&limit=25
  Web Scraper          : GET /web-scraper?url=...&include_html=0
"""

import aiohttp
import re
from typing import List, Dict, Optional
from loguru import logger
from config import config

BASE_URL = "https://scrappa.co/api"

# Domains to skip — social / aggregators / useless
SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "youtu.be", "pinterest.com",
     "yelp.com", "google.com", "maps.google.com",
    "trustpilot.com", "booking.com", "airbnb.com", "wikipedia.org",
    "wikimedia.org", "wikitravel.org", "lonelyplanet.com", "expedia.com",
}

# Category keywords that mean SaaS / not a real supplier
SKIP_CATEGORIES = {
    "software company", "software", "it company", "technology company",
    "internet company", "logiciel", "saas", "tech startup",
}


class ScrappaClient:

    def __init__(self):
        self.api_key = config.SCRAPPA_API_KEY
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _h(self) -> dict:
        return {"X-API-KEY": self.api_key, "Accept": "application/json"}

    # ── 1. Google Maps Advanced Search ───────────────────────
    async def search_google_maps(
        self, query: str, location: str = "", limit: int = 20, zoom: int = 13,
    ) -> List[Dict]:
        if not self.api_key:
            logger.error("SCRAPPA_API_KEY missing")
            return []

        session    = await self._get_session()
        full_query = f"{query} {location}".strip() if location else query
        params     = {"query": full_query, "zoom": str(zoom), "limit": str(limit)}

        try:
            async with session.get(
                f"{BASE_URL}/maps/advanced-search", params=params, headers=self._h(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Maps {resp.status}: {(await resp.text())[:200]}")
                    return []

                data  = await resp.json()
                items = (
                    data.get("items") or data.get("results") or
                    data.get("data") or (data if isinstance(data, list) else [])
                )

                results = [r for r in (self._parse_map_item(i) for i in items) if r]
                logger.info(f"Maps '{full_query}': {len(results)} results")
                return results

        except Exception as e:
            logger.error(f"Maps exception: {e}")
            return []

    def _parse_map_item(self, item: dict) -> Optional[Dict]:
        if not item or not isinstance(item, dict):
            return None

        nom = (item.get("name") or item.get("title") or "").strip()
        if not nom:
            return None

        # Skip SaaS
        cat = (item.get("type") or item.get("category") or "").lower()
        if any(s in cat for s in SKIP_CATEGORIES):
            return None

        site   = item.get("website") or item.get("site") or item.get("url") or ""
        domain = self._extract_domain(site)
        if domain and any(s in domain for s in SKIP_DOMAINS):
            return None

        phones = item.get("phone_numbers") or item.get("phones") or []
        phone  = phones[0] if isinstance(phones, list) and phones else item.get("phone", "")

        return {
            "nom":             nom,
            "domain":          domain,
            "adresse":         item.get("full_address") or item.get("address") or "",
            "telephone":       phone,
            "rating":          item.get("rating"),
            "nb_avis":         item.get("review_count") or item.get("reviews") or 0,
            "categorie":       item.get("type") or item.get("category") or "",
            "google_maps_url": item.get("maps_url") or item.get("url") or "",
            "source":          "scrappa_maps",
        }

    # ── 2. Google Search ─────────────────────────────────────
    async def search_google(self, query: str, limit: int = 15) -> List[Dict]:
        if not self.api_key:
            return []

        session = await self._get_session()
        params  = {"query": query, "hl": "en", "safe": "off", "num": str(limit)}

        try:
            async with session.get(
                f"{BASE_URL}/search", params=params, headers=self._h(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Search {resp.status}: {(await resp.text())[:200]}")
                    return []

                data  = await resp.json()
                items = (
                    data.get("organic_results") or data.get("results") or
                    data.get("items") or (data if isinstance(data, list) else [])
                )

                results = []
                for item in items:
                    link   = item.get("link") or item.get("url") or ""
                    title  = (item.get("title") or "").strip()
                    domain = self._extract_domain(link)
                    if not title or not link:
                        continue
                    if domain and any(s in domain for s in SKIP_DOMAINS):
                        continue
                    results.append({
                        "title":   title,
                        "link":    link,
                        "snippet": item.get("snippet") or "",
                        "domain":  domain,
                        "source":  "scrappa_search",
                    })

                logger.info(f"Search '{query}': {len(results)} results")
                return results

        except Exception as e:
            logger.error(f"Search exception: {e}")
            return []

    # ── 3. Brave Search ──────────────────────────────────────
    async def search_brave(self, query: str, limit: int = 10) -> List[Dict]:
        if not self.api_key:
            return []

        session = await self._get_session()
        params  = {"query": query, "num": str(limit)}

        try:
            async with session.get(
                f"{BASE_URL}/brave/search", params=params, headers=self._h(),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    return []

                data  = await resp.json()
                items = data.get("results") or data.get("web", {}).get("results") or []

                results = []
                for item in items:
                    link   = item.get("url") or item.get("link") or ""
                    title  = (item.get("title") or "").strip()
                    domain = self._extract_domain(link)
                    if not title or not link:
                        continue
                    if domain and any(s in domain for s in SKIP_DOMAINS):
                        continue
                    results.append({
                        "title":   title,
                        "link":    link,
                        "snippet": item.get("description") or item.get("snippet") or "",
                        "domain":  domain,
                        "source":  "scrappa_brave",
                    })

                logger.info(f"Brave '{query}': {len(results)} results")
                return results

        except Exception as e:
            logger.error(f"Brave exception: {e}")
            return []

    # ── 4. YouTube Search ────────────────────────────────────
    async def search_youtube(self, query: str, limit: int = 10) -> List[Dict]:
        if not self.api_key:
            return []

        session = await self._get_session()
        params  = {"query": query, "limit": str(limit)}

        try:
            async with session.get(
                f"{BASE_URL}/youtube/search", params=params, headers=self._h(),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    return []

                data  = await resp.json()
                items = data.get("results") or data.get("items") or []

                return [{
                    "title":   (item.get("title") or "").strip(),
                    "link":    item.get("link") or item.get("url") or "",
                    "snippet": item.get("description") or "",
                    "source":  "scrappa_youtube",
                } for item in items if item.get("title")]

        except Exception as e:
            logger.error(f"YouTube exception: {e}")
            return []

    # ── 5. Pinterest Search ──────────────────────────────────
    async def search_pinterest(self, query: str, limit: int = 10) -> List[Dict]:
        if not self.api_key:
            return []

        session = await self._get_session()
        params  = {"query": query, "limit": str(limit)}

        try:
            async with session.get(
                f"{BASE_URL}/pinterest/search", params=params, headers=self._h(),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    return []

                data  = await resp.json()
                items = data.get("results") or data.get("items") or []

                results = []
                for item in items:
                    link   = item.get("link") or item.get("url") or ""
                    domain = self._extract_domain(link)
                    if "pinterest.com" in domain:
                        continue
                    results.append({
                        "title":   (item.get("title") or "").strip(),
                        "link":    link,
                        "snippet": item.get("description") or "",
                        "domain":  domain,
                        "source":  "scrappa_pinterest",
                    })

                logger.info(f"Pinterest '{query}': {len(results)} results")
                return results

        except Exception as e:
            logger.error(f"Pinterest exception: {e}")
            return []

    # ── 6. Web Scraper ───────────────────────────────────────
    async def scrape_url(self, url: str, include_html: bool = False) -> Dict:
        """
        Scrappa Web Scraper — used for connectivity detection.
        Returns body_text, links, title, meta_description, status_code.
        """
        if not self.api_key:
            return {}

        session = await self._get_session()
        params  = {"url": url, "include_html": "1" if include_html else "0"}

        try:
            async with session.get(
                f"{BASE_URL}/web-scraper", params=params, headers=self._h(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return {"error": resp.status}
                return await resp.json()

        except Exception as e:
            logger.error(f"WebScraper exception {url}: {e}")
            return {}

    # ── Helper ───────────────────────────────────────────────
    @staticmethod
    def _extract_domain(url: str) -> str:
        if not url:
            return ""
        url = url.strip().lower()
        url = re.sub(r"https?://", "", url)
        url = re.sub(r"^www\.", "", url)
        return url.split("/")[0].split("?")[0].split("#")[0]

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()