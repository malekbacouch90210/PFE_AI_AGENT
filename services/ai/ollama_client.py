"""
services/ai/ollama_client.py — v2
Updated prompts for normalization geographic reasoning.

New/updated methods:
  extract_geography()     → full geo reasoning (new — replaces normalize_city/country)
  suggest_city()          → when only country known, suggest logical city for activity
  classify_activity()     → unchanged
  estimate_price()        → unchanged
  embed()                 → unchanged
"""
import json
import re
import asyncio
from typing import Optional, Tuple
import httpx
from loguru import logger
from config import config


class OllamaClient:
    """
    Async Ollama client — all models local, zero API cost.

    Models:
      qwen2.5:7b        → geo reasoning, classification, city normalization
      deepseek-r1:8b    → price estimation (chain-of-thought reasoning)
      mxbai-embed-large → 1024-dim embeddings
    """

    CLASSIFY_MODEL = "qwen2.5:7b"
    ESTIMATE_MODEL = "deepseek-r1:8b"
    EMBED_MODEL    = "mxbai-embed-large"
    TIMEOUT        = config.OLLAMA_TIMEOUT   # 45s

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def _get(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=config.OLLAMA_URL,
                timeout=httpx.Timeout(self.TIMEOUT),
            )
        return self._client

    # ──────────────────────────────────────────────────────────
    # Health check
    # ──────────────────────────────────────────────────────────

    async def health_check(self) -> bool:
        try:
            client = await self._get()
            resp   = await client.get("/api/tags", timeout=5)
            models = [m["name"] for m in resp.json().get("models", [])]
            ok_c = any(self.CLASSIFY_MODEL.split(":")[0] in m for m in models)
            ok_e = any(self.ESTIMATE_MODEL.split(":")[0] in m for m in models)
            ok_b = any(self.EMBED_MODEL.split(":")[0]    in m for m in models)
            if not ok_c: logger.error(f"❌ Missing: {self.CLASSIFY_MODEL}")
            if not ok_e: logger.error(f"❌ Missing: {self.ESTIMATE_MODEL}")
            if not ok_b: logger.error(f"❌ Missing: {self.EMBED_MODEL}")
            return ok_c and ok_e and ok_b
        except Exception as e:
            logger.error(f"❌ Ollama unreachable: {e}")
            return False

    # ──────────────────────────────────────────────────────────
    # Core generate
    # ──────────────────────────────────────────────────────────

    async def generate(self, prompt: str, model: str, max_retries: int = 2) -> str:
        client = await self._get()
        for attempt in range(max_retries + 1):
            try:
                resp = await client.post(
                    "/api/generate",
                    json={"model": model, "prompt": prompt, "stream": False},
                )
                resp.raise_for_status()
                return resp.json().get("response", "").strip()
            except Exception as e:
                if attempt < max_retries:
                    await asyncio.sleep(1)
                else:
                    logger.warning(f"Ollama generate failed ({model}): {e}")
                    return ""
        return ""

    # ──────────────────────────────────────────────────────────
    # GEOGRAPHIC REASONING — new primary geo method
    # ──────────────────────────────────────────────────────────

    async def extract_geography(
        self,
        title: str,
        description: str,
        url_slug: str = "",
        supplier_country_hint: str = "",
        ville_raw_hint: str = "",
        pays_raw_hint: str = "",
    ) -> dict:
        """
        Full geographic reasoning for a tourism product.
        Uses qwen2.5:7b.

        CRITICAL RULES given to the model:
        - Destination ≠ supplier location (discoverafrica.com sells Botswana tours)
        - If only country found → reason logically about typical cities for that activity
          Example: "Safari Botswana" → Botswana safaris happen in Maun or Kasane
          → pick ONE most likely city
        - Return structured JSON with confidence

        Returns:
          {
            "detected_country": str or null,
            "detected_city":    str or null,
            "confidence":       float 0.0-1.0,
            "reasoning":        str (short explanation)
          }
        """
        # Build context string
        context_parts = []
        if supplier_country_hint:
            context_parts.append(
                f"The supplier/operator is based in {supplier_country_hint} "
                f"(this is NOT necessarily the product destination)."
            )
        if ville_raw_hint:
            context_parts.append(
                f"A raw city hint was scraped (may be wrong/dirty): '{ville_raw_hint}'"
            )
        if pays_raw_hint:
            context_parts.append(
                f"A raw country hint was found in the URL/content: '{pays_raw_hint}'"
            )
        if url_slug:
            context_parts.append(f"URL slug: {url_slug}")

        context_str = "\n".join(context_parts) if context_parts else "No additional context."

        prompt = f"""You are a travel geography expert specializing in tourism product analysis.

Your task: identify WHERE this tourism activity/tour takes place.

---
PRODUCT TITLE: {title}
DESCRIPTION: {description[:500]}
CONTEXT: {context_str}
---

CRITICAL RULES:
1. The destination is WHERE the ACTIVITY takes place, NOT where the company is based.
   Example: A South African company selling "Botswana Safari" → destination is BOTSWANA.
   Example: A UK company selling "Rome Food Tour" → destination is ROME, ITALY.

2. If you only know the country, reason about the most logical city for this activity type:
   - "Safari Botswana" → Botswana safaris concentrate in MAUN (Okavango) or KASANE (Chobe)
   - "Serengeti Safari" → city is ARUSHA (main gateway to Serengeti, Tanzania)
   - "Petra Tour" → city is AQABA or WADI MUSA (gateway to Petra, Jordan)
   - "Sahara Desert Tour Tunisia" → city is TOZEUR or DOUZ
   - "Angkor Wat Tour" → city is SIEM REAP, Cambodia

3. Pick ONE city only — the most specific and logical destination.
   Do NOT pick the capital unless it is genuinely the destination.

4. If truly unclear → set detected_city to null but still try country.

5. Confidence scoring:
   - 0.9+ : city + country explicit in title
   - 0.7-0.9 : city or country clear from title, other inferred
   - 0.5-0.7 : inferred from description/URL
   - 0.3-0.5 : logical reasoning only
   - 0.1-0.3 : very uncertain
   - 0.0 : cannot determine

Respond with ONLY valid JSON, no explanation outside it:
{{
  "detected_country": "country name in English" or null,
  "detected_city": "city name in English" or null,
  "confidence": 0.0 to 1.0,
  "reasoning": "one sentence why"
}}"""

        response = await self.generate(prompt, self.CLASSIFY_MODEL)
        return _parse_geo_json(response)

    # ──────────────────────────────────────────────────────────
    # CITY SUGGESTION — when country known, activity type known
    # ──────────────────────────────────────────────────────────

    async def suggest_city(
        self,
        country: str,
        activity_type: str,
        title: str,
        description: str,
        candidate_cities: list = None,
    ) -> Tuple[str, float]:
        """
        When we know the country but not the city, ask the LLM
        to reason about the most likely city for this specific activity.

        Used in Sprint 4 when:
          - extract_geography() returns country but no city
          - We have a list of candidate cities from villes DB

        Args:
          country:          verified country name
          activity_type:    EXCURSION | TICKET | TRANSFER
          title:            clean product title
          description:      clean product description
          candidate_cities: optional list from villes DB (max 20)

        Returns: (city_name, confidence)
        """
        cities_str = ""
        if candidate_cities:
            top = candidate_cities[:20]
            cities_str = (
                f"\nKnown cities in {country} (from database):\n"
                + ", ".join(top)
                + "\nPick from this list if possible."
            )

        activity_examples = {
            "EXCURSION": "tours, safaris, day trips, boat tours, hiking, cultural visits",
            "TICKET":    "museum entries, park admissions, attraction tickets",
            "TRANSFER":  "airport transfers, shuttle services, private drivers",
        }
        activity_desc = activity_examples.get(activity_type, "tourism activity")

        prompt = f"""You are a travel geography expert.

A tourism product is sold in {country}.
Activity type: {activity_type} ({activity_desc})
Product title: {title}
Description: {description[:300]}
{cities_str}

Question: What is the most likely CITY or DESTINATION in {country} where this activity takes place?

Rules:
- Pick the most logical city based on the activity and any geographic clues
- For safaris: choose near wildlife parks/reserves
- For cultural tours: choose major historic/cultural cities
- For transfers: identify the airport city or main hub
- ONE city only
- If genuinely unclear, return the capital or main tourist city of {country}

Respond with ONLY this JSON:
{{
  "city": "city name" or null,
  "confidence": 0.0 to 1.0,
  "reason": "one sentence"
}}"""

        response = await self.generate(prompt, self.CLASSIFY_MODEL)
        result   = _parse_simple_json(response)
        city     = result.get("city") or ""
        conf     = float(result.get("confidence", 0.3))
        if city and city.lower() == "null":
            city = ""
        return city.strip(), conf

    # ──────────────────────────────────────────────────────────
    # CLASSIFY ACTIVITY (unchanged logic, better prompt)
    # ──────────────────────────────────────────────────────────

    async def classify_activity(self, name: str, description: str) -> str:
        """
        Classify product as EXCURSION | TICKET | TRANSFER.
        qwen2.5:7b — fast, single-word answer.
        """
        prompt = (
            "You are a tourism product classifier.\n"
            "Classify this product into exactly ONE of: EXCURSION, TICKET, TRANSFER\n\n"
            f"Product name: {name[:200]}\n"
            f"Description: {description[:300]}\n\n"
            "Definitions:\n"
            "- EXCURSION: guided tours, safaris, day trips, activities, experiences, "
            "boat tours, hiking, cooking classes, cultural visits\n"
            "- TICKET: museum entry, attraction admission, park ticket, "
            "skip-the-line, show entry, fast track\n"
            "- TRANSFER: airport transfer, shuttle, taxi service, "
            "private driver, minibus, port transfer\n\n"
            "Respond with ONLY ONE WORD: EXCURSION or TICKET or TRANSFER"
        )
        result = (await self.generate(prompt, self.CLASSIFY_MODEL)).upper().strip()
        for word in result.split():
            if word in {"EXCURSION", "TICKET", "TRANSFER"}:
                return word
        return ""

    # ──────────────────────────────────────────────────────────
    # NORMALIZE CITY — light clean (used as fallback)
    # ──────────────────────────────────────────────────────────

    async def normalize_city(self, city_raw: str, country_hint: str = "") -> str:
        """
        Convert dirty/foreign city name to standard English.
        Redis cached by caller. Used as light fallback only.
        Main geo extraction is done by extract_geography().
        """
        country_str = f" in {country_hint}" if country_hint else ""
        prompt = (
            f"Convert this city name to its standard English spelling{country_str}.\n"
            f"City: '{city_raw}'\n"
            "Respond with ONLY the city name, nothing else. "
            "If already correct English, return unchanged. "
            "Examples: 'Le Caire' → 'Cairo', 'Bruxelles' → 'Brussels', "
            "'Barcelone' → 'Barcelona', 'Tunis' → 'Tunis'"
        )
        result = (await self.generate(prompt, self.CLASSIFY_MODEL)).strip().strip('"').strip("'")
        if not result or len(result) > 100:
            return city_raw.strip()
        return result

    async def normalize_country(self, country_raw: str) -> str:
        """Convert country name to standard English."""
        prompt = (
            f"Convert this country name to its standard English name.\n"
            f"Country: '{country_raw}'\n"
            "Respond with ONLY the country name. No explanation.\n"
            "Examples: 'Tunisie' → 'Tunisia', 'Allemagne' → 'Germany', "
            "'Brésil' → 'Brazil', 'Maroc' → 'Morocco'"
        )
        result = (await self.generate(prompt, self.CLASSIFY_MODEL)).strip().strip('"').strip("'")
        if not result or len(result) > 80:
            return country_raw.strip()
        return result

    # ──────────────────────────────────────────────────────────
    # PRICE ESTIMATION (deepseek-r1:8b)
    # ──────────────────────────────────────────────────────────

    async def estimate_price(
        self,
        activity_type: str,
        city: str,
        country: str,
        category: str,
        description: str,
    ) -> Optional[float]:
        """
        Estimate EUR price using deepseek-r1:8b chain-of-thought reasoning.
        DeepSeek is used here specifically for its reasoning quality.
        """
        prompt = (
            "You are a tourism pricing expert with global market knowledge.\n"
            "Estimate a realistic retail price in EUR for this tourism product.\n\n"
            f"Activity type: {activity_type}\n"
            f"City: {city or 'Unknown'}\n"
            f"Country: {country or 'Unknown'}\n"
            f"Category: {category or 'Tourism activity'}\n"
            f"Description: {description[:300]}\n\n"
            "Pricing factors to consider:\n"
            "- Developing countries (Africa, SE Asia, South America): "
            "excursions typically €15–€80, transfers €10–€50\n"
            "- European cities (Paris, Rome, Barcelona): "
            "excursions €25–€150, tickets €10–€60, transfers €30–€100\n"
            "- Middle East/Gulf (Dubai, Jordan): "
            "excursions €40–€200, transfers €30–€80\n"
            "- Duration matters: half-day < full-day < multi-day\n"
            "- Private tours cost 2–3× group tours\n\n"
            "Respond with ONLY a single number (no €, no text, just digits). "
            "Example: 45 or 120.50"
        )
        result = await self.generate(prompt, self.ESTIMATE_MODEL)
        return _parse_price_from_str(result)

    # ──────────────────────────────────────────────────────────
    # EMBEDDINGS (mxbai-embed-large)
    # ──────────────────────────────────────────────────────────

    async def embed(self, text: str) -> Optional[list]:
        """
        Generate 1024-dim embedding vector.
        Sprint 4: generated and stored.
        Sprint 5: used for cosine similarity matching.
        """
        client = await self._get()
        try:
            resp = await client.post(
                "/api/embeddings",
                json={"model": self.EMBED_MODEL, "prompt": text},
            )
            resp.raise_for_status()
            vec = resp.json().get("embedding")
            if vec and len(vec) == 1024:
                return vec
            logger.warning(f"Unexpected embedding dim: {len(vec) if vec else 0}")
            return None
        except Exception as e:
            logger.warning(f"Embed failed: {e}")
            return None

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# ─────────────────────────────────────────────────────────────
# JSON parsing helpers
# ─────────────────────────────────────────────────────────────

def _parse_geo_json(response: str) -> dict:
    """
    Parse geographic extraction JSON from LLM response.
    Handles deepseek <think> tags and markdown code fences.
    """
    default = {
        "detected_country": None,
        "detected_city":    None,
        "confidence":       0.0,
        "reasoning":        "",
    }
    if not response:
        return default
    try:
        # Strip deepseek think tags
        clean = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL)
        # Strip markdown fences
        clean = re.sub(r"```(?:json)?|```", "", clean).strip()
        # Find JSON block
        m = re.search(r"\{.*\}", clean, re.DOTALL)
        if not m:
            return default
        data = json.loads(m.group())

        city    = data.get("detected_city")    or None
        country = data.get("detected_country") or None
        conf    = float(data.get("confidence", 0.0))
        reason  = data.get("reasoning") or ""

        # Clean null strings
        if city    and city.lower()    in ("null", "none", "unknown", ""): city    = None
        if country and country.lower() in ("null", "none", "unknown", ""): country = None

        return {
            "detected_country": country,
            "detected_city":    city,
            "confidence":       round(min(max(conf, 0.0), 1.0), 2),
            "reasoning":        reason[:200],
        }
    except Exception as e:
        logger.debug(f"_parse_geo_json failed: {e} | response: {response[:100]}")
        return default


def _parse_simple_json(response: str) -> dict:
    """Parse simple JSON from LLM (city suggestion, etc.)."""
    if not response:
        return {}
    try:
        clean = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL)
        clean = re.sub(r"```(?:json)?|```", "", clean).strip()
        m = re.search(r"\{.*\}", clean, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception:
        pass
    return {}


def _parse_price_from_str(text: str) -> Optional[float]:
    """Extract numeric price from LLM response."""
    if not text:
        return None
    # Strip deepseek think tags
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    m = re.search(r"\d+(?:\.\d+)?", text)
    if m:
        try:
            val = float(m.group())
            if 1 <= val <= 10000:
                return round(val, 2)
        except ValueError:
            pass
    return None


# Singleton
ollama_client = OllamaClient()