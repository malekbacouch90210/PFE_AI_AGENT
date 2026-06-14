"""
services/geo/city_fetcher.py
City/country helpers using PostgreSQL villes + pays tables.

New functions added:
  verify_city_in_db()      — used by product_scraper to validate ville_raw
  extract_country_from_url() — helper for pays_raw extraction
"""
from typing import Dict, List, Optional
from loguru import logger
from sqlalchemy import text
from database import DatabaseManager


# ─────────────────────────────────────────────────────────────
# Verify city against villes table (Sprint 3)
# ─────────────────────────────────────────────────────────────

async def verify_city_in_db(
    city_name: str,
    country_hint: Optional[str] = None,
) -> Optional[str]:
    """
    Verify a city name against the villes table.
    Returns the canonical city name from DB if found, None otherwise.

    Used by product_scraper to validate ville_raw before saving.
    If not in DB → ville_raw = NULL (don't store garbage).

    Args:
        city_name:    city name to verify (from JSON-LD or text)
        country_hint: optional country to narrow search

    Returns:
        Canonical city name from DB or None
    """
    if not city_name or len(city_name.strip()) < 2:
        return None

    city_clean = city_name.strip()
    db = DatabaseManager()

    try:
        async with db.engine.connect() as conn:
            if country_hint:
                result = await conn.execute(text("""
                    SELECT v.name FROM villes v
                    JOIN pays p ON v.pays_id = p.id
                    WHERE LOWER(v.name) = LOWER(:city)
                      AND LOWER(p.nom) = LOWER(:country)
                    LIMIT 1
                """), {"city": city_clean, "country": country_hint})
                row = result.fetchone()
                if row:
                    return row[0]

            # Try without country constraint
            result = await conn.execute(text("""
                SELECT v.name FROM villes v
                WHERE LOWER(v.name) = LOWER(:city)
                LIMIT 1
            """), {"city": city_clean})
            row = result.fetchone()
            if row:
                return row[0]

            # Try partial match for slight variations
            result = await conn.execute(text("""
                SELECT v.name FROM villes v
                WHERE LOWER(v.name) LIKE LOWER(:city)
                LIMIT 1
            """), {"city": f"{city_clean}%"})
            row = result.fetchone()
            if row:
                return row[0]

            return None

    except Exception as e:
        logger.warning(f"verify_city_in_db error for '{city_name}': {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Sprint 4 — validate LLM-extracted city against DB
# ─────────────────────────────────────────────────────────────

async def validate_location_against_db(
    city: Optional[str],
    country: Optional[str],
) -> Dict:
    """
    Validate LLM-extracted city + country against villes/pays tables.
    Used in Sprint 4 normalization_pipeline.

    Returns:
        {
          "city_valid": bool,
          "country_valid": bool,
          "canonical_city": str or None,
          "canonical_country": str or None,
          "confidence_boost": float,
        }
    """
    result = {
        "city_valid": False,
        "country_valid": False,
        "canonical_city": None,
        "canonical_country": None,
        "confidence_boost": 0.0,
    }

    if not city and not country:
        return result

    db = DatabaseManager()

    try:
        async with db.engine.connect() as conn:
            # Validate country
            if country:
                r = await conn.execute(text("""
                    SELECT nom FROM pays WHERE LOWER(nom) = LOWER(:c) LIMIT 1
                """), {"c": country.strip()})
                row = r.fetchone()
                if row:
                    result["country_valid"]    = True
                    result["canonical_country"]= row[0]
                    result["confidence_boost"] += 0.2

            # Validate city (with country if available)
            if city:
                if result["canonical_country"]:
                    r = await conn.execute(text("""
                        SELECT v.name FROM villes v
                        JOIN pays p ON v.pays_id = p.id
                        WHERE LOWER(v.name) = LOWER(:city)
                          AND LOWER(p.nom) = LOWER(:country)
                        LIMIT 1
                    """), {"city": city.strip(), "country": result["canonical_country"]})
                else:
                    r = await conn.execute(text("""
                        SELECT v.name FROM villes v
                        WHERE LOWER(v.name) = LOWER(:city)
                        LIMIT 1
                    """), {"city": city.strip()})
                row = r.fetchone()
                if row:
                    result["city_valid"]       = True
                    result["canonical_city"]   = row[0]
                    result["confidence_boost"] += 0.3

    except Exception as e:
        logger.warning(f"validate_location_against_db error: {e}")

    return result


# ─────────────────────────────────────────────────────────────
# Sprint 4 — get candidate cities for a country
# Used when LLM needs to pick a city given only country context
# ─────────────────────────────────────────────────────────────

async def get_cities_for_country(country_name: str) -> List[str]:
    db = DatabaseManager()
    try:
        async with db.engine.connect() as conn:
            result = await conn.execute(text("""
                SELECT DISTINCT v.name FROM villes v
                JOIN pays p ON v.pays_id = p.id
                WHERE LOWER(p.nom) = LOWER(:country)
                ORDER BY v.name
            """), {"country": country_name})
            cities = [row[0] for row in result.fetchall()]
            if cities:
                logger.info(f"✅ {country_name} → {len(cities)} cities")
                return cities
            # Partial match
            result = await conn.execute(text("""
                SELECT DISTINCT v.name FROM villes v
                JOIN pays p ON v.pays_id = p.id
                WHERE LOWER(p.nom) LIKE LOWER(:country)
                ORDER BY v.name
            """), {"country": f"%{country_name}%"})
            cities = [row[0] for row in result.fetchall()]
            return cities or [country_name]
    except Exception as e:
        logger.error(f"get_cities_for_country error for {country_name}: {e}")
        return [country_name]


def extract_country_from_url(url: str) -> Optional[str]:
    """Quick country extraction from URL path — used as pays_raw hint."""
    from scrapers.product_scraper import COUNTRY_SLUG_MAP
    from urllib.parse import urlparse
    parsed = urlparse(url.lower())
    for part in parsed.path.split("/"):
        if part in COUNTRY_SLUG_MAP:
            return COUNTRY_SLUG_MAP[part]
    return None


async def search_cities(query: str, limit: int = 50) -> List[Dict]:
    db = DatabaseManager()
    try:
        async with db.engine.connect() as conn:
            result = await conn.execute(text("""
                SELECT v.name, p.nom FROM villes v
                JOIN pays p ON v.pays_id = p.id
                WHERE LOWER(v.name) LIKE LOWER(:q)
                ORDER BY v.name LIMIT :limit
            """), {"q": f"%{query}%", "limit": limit})
            return [{"city": r[0], "country": r[1]} for r in result.fetchall()]
    except Exception as e:
        logger.error(f"search_cities error: {e}")
        return []


async def get_city_info(city_name: str, country_name: str = None) -> Optional[Dict]:
    db = DatabaseManager()
    try:
        async with db.engine.connect() as conn:
            params = {"city": city_name}
            where  = "WHERE LOWER(v.name) = LOWER(:city)"
            if country_name:
                where += " AND LOWER(p.nom) = LOWER(:country)"
                params["country"] = country_name
            result = await conn.execute(text(f"""
                SELECT v.name, p.nom, v.latitude, v.longitude, v.population
                FROM villes v JOIN pays p ON v.pays_id = p.id
                {where} LIMIT 1
            """), params)
            row = result.fetchone()
            if row:
                return {
                    "name": row[0], "country": row[1],
                    "latitude":  float(row[2]) if row[2] else None,
                    "longitude": float(row[3]) if row[3] else None,
                    "population": row[4],
                }
            return None
    except Exception as e:
        logger.error(f"get_city_info error: {e}")
        return None
