# scrapers/apis/openstreetmap.py
# FIX: Use address.city / address.town / address.village fields
# NEVER split display_name — that caused "City In" bug

import aiohttp
import asyncio
import time
from typing import List
from loguru import logger

# Words that indicate a bad parse result (not a real city name)
BAD_NAME_WORDS = {
    "city", "in", "town", "village", "municipality",
    "commune", "district", "region", "province",
}


class NominatimClient:
    """Nominatim (OpenStreetMap) — FREE, 1 req/sec limit."""

    BASE_URL   = "https://nominatim.openstreetmap.org"
    USER_AGENT = "DEX-AI-Sourcing-Agent/1.0"

    def __init__(self):
        self._last_req = 0.0

    async def _throttle(self):
        """Respect Nominatim's 1 request/second rate limit."""
        elapsed = time.time() - self._last_req
        if elapsed < 1.1:
            await asyncio.sleep(1.1 - elapsed)
        self._last_req = time.time()

    async def get_all_cities(self, country: str, limit: int = 100) -> List[str]:
        """
        Return list of real city names for a country.

        FIX: Uses address.city / address.town / address.village fields.
        NEVER uses display_name.split(',')[0] — that returned 'City In'.
        """
        headers = {"User-Agent": self.USER_AGENT}

        # Step 1: get the ISO country code
        country_code = await self._get_country_code(country, headers)

        # Step 2: search for cities using countrycodes filter
        await self._throttle()
        params = {
            "format":         "json",
            "addressdetails": "1",
            "limit":          str(limit),
            "q":              "city",
        }
        if country_code:
            params["countrycodes"] = country_code.lower()
        else:
            # Fallback if we couldn't get the code
            params["q"] = f"cities {country}"

        results = await self._get(params, headers)
        cities  = self._extract_city_names(results)

        # If we got less than 3 cities, try a direct name-based search
        if len(cities) < 3:
            await self._throttle()
            fallback_params = {
                "q":              f"city {country}",
                "format":         "json",
                "addressdetails": "1",
                "limit":          str(limit),
            }
            results2 = await self._get(fallback_params, headers)
            cities   = self._extract_city_names(results2)

        logger.info(f"Nominatim: {len(cities)} cities for {country}")
        return cities[:limit]

    async def _get_country_code(self, country: str, headers: dict) -> str:
        """Get ISO 2-letter country code for a country name."""
        await self._throttle()
        params = {
            "q":              country,
            "format":         "json",
            "limit":          "1",
            "addressdetails": "1",
        }
        results = await self._get(params, headers)
        if results:
            addr = results[0].get("address", {})
            return addr.get("country_code", "").upper()
        return ""

    def _extract_city_names(self, results: list) -> List[str]:
        """
        Extract clean city names from Nominatim results.

        Priority order:
          1. address.city          ← most reliable
          2. address.town
          3. address.village
          4. address.municipality
          5. item["name"]          ← fallback

        NEVER: display_name.split(',')[0]  ← this caused "City In" bug
        """
        cities = []
        seen   = set()

        for item in results:
            if not isinstance(item, dict):
                continue

            addr = item.get("address", {})

            name = (
                addr.get("city")         or
                addr.get("town")         or
                addr.get("village")      or
                addr.get("municipality") or
                addr.get("suburb")       or
                item.get("name")         or
                ""
            ).strip()

            if not name:
                continue

            # Skip generic / bad words
            if name.lower() in BAD_NAME_WORDS:
                continue

            # Skip if it looks like a query artifact
            for bad in ["city in", "town in", "village in", "cities "]:
                if bad in name.lower():
                    name = ""
                    break
            if not name:
                continue

            # Skip very short or very long
            if len(name) < 2 or len(name) > 80:
                continue

            key = name.lower()
            if key not in seen:
                seen.add(key)
                cities.append(name)

        return cities

    async def _get(self, params: dict, headers: dict) -> list:
        """Make a GET request to Nominatim and return JSON list."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.BASE_URL}/search",
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    logger.warning(f"Nominatim HTTP {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"Nominatim error: {e}")
            return []