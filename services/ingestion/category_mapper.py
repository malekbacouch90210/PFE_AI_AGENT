"""
services/ingestion/category_mapper.py
Maps raw scraped category strings → DEX canonical categories.
10 categories as defined in Sprint 4 class diagram.
"""

# DEX canonical categories (from Sprint 4 PlantUML)
DEX_CATEGORIES = [
    "CULTUREL",
    "AVENTURE",
    "NATURE",
    "TRANSPORT",
    "GASTRONOMIE",
    "SPORT",
    "BIEN_ETRE",
    "NAUTIQUE",
    "HISTORIQUE",
    "FAMILLE",
]

# Keyword → DEX category mapping
_RULES: list[tuple[list[str], str]] = [
    # TRANSPORT
    (["transfer", "shuttle", "taxi", "airport", "transport", "driver",
      "navette", "chauffeur", "limousine", "bus", "train"],      "TRANSPORT"),
    # HISTORIQUE
    (["museum", "musée", "history", "historical", "archaeological",
      "ruins", "palace", "castle", "monument", "heritage",
      "ancient", "roman", "ruins", "temple"],                    "HISTORIQUE"),
    # CULTUREL
    (["culture", "cultural", "art", "gallery", "theatre", "opera",
      "architecture", "tradition", "local", "village",
      "culturel", "artisan", "craft"],                           "CULTUREL"),
    # NATURE
    (["nature", "wildlife", "safari", "national park", "forest",
      "mountain", "hiking", "trek", "desert", "canyon",
      "volcano", "jungle", "countryside", "rural"],              "NATURE"),
    # NAUTIQUE
    (["boat", "sailing", "cruise", "catamaran", "snorkel", "diving",
      "scuba", "kayak", "paddle", "beach", "island", "nautique",
      "whale", "dolphin", "sea", "ocean", "lake", "river"],      "NAUTIQUE"),
    # AVENTURE
    (["adventure", "zip line", "bungee", "paragliding", "climbing",
      "quad", "4x4", "offroad", "rappel", "extreme", "thrill",
      "jeep", "atv", "rafting"],                                  "AVENTURE"),
    # GASTRONOMIE
    (["food", "cooking", "wine", "culinary", "gastronomy", "restaurant",
      "dinner", "tasting", "market", "farm", "olive oil",
      "cheese", "bread", "street food"],                         "GASTRONOMIE"),
    # SPORT
    (["sport", "golf", "tennis", "ski", "cycling", "biking", "running",
      "yoga", "fitness", "surf", "swimming", "football"],        "SPORT"),
    # BIEN_ETRE
    (["spa", "wellness", "massage", "relax", "meditation", "retreat",
      "hammam", "thermal", "bien-être", "yoga"],                 "BIEN_ETRE"),
    # FAMILLE
    (["family", "kids", "children", "theme park", "zoo", "aquarium",
      "amusement", "water park", "famille", "enfants"],          "FAMILLE"),
]


def map_to_dex_category(
    category_raw: str,
    nom_produit: str = "",
    description: str = "",
    activity_type: str = "",
) -> str:
    """
    Map raw category + product text → DEX canonical category.
    Priority: category_raw → nom_produit → description → activity_type fallback.
    """
    combined = " ".join([
        (category_raw or "").lower(),
        (nom_produit or "")[:200].lower(),
        (description or "")[:300].lower(),
    ])

    for keywords, category in _RULES:
        if any(kw in combined for kw in keywords):
            return category

    # Activity type fallback
    if activity_type == "TRANSFER":
        return "TRANSPORT"
    if activity_type == "TICKET":
        return "CULTUREL"

    return "CULTUREL"   # default — most common tourism product type
