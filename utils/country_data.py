# utils/country_data.py
# 195 pays mappés sur les 5 zones DEX

COUNTRY_ZONE_MAP = {
    # ── Zone 1 : Italie ─────────────────────────────────────
    "Italy": 1,

    # ── Zone 2 : Europe ─────────────────────────────────────
    "Albania": 2, "Andorra": 2, "Austria": 2, "Belarus": 2, "Belgium": 2,
    "Bosnia and Herzegovina": 2, "Bulgaria": 2, "Croatia": 2, "Cyprus": 2,
    "Czech Republic": 2, "Denmark": 2, "Estonia": 2, "Finland": 2, "France": 2,
    "Georgia": 2, "Germany": 2, "Greece": 2, "Hungary": 2, "Iceland": 2,
    "Ireland": 2, "Kosovo": 2, "Latvia": 2, "Liechtenstein": 2, "Lithuania": 2,
    "Luxembourg": 2, "Malta": 2, "Moldova": 2, "Monaco": 2, "Montenegro": 2,
    "Netherlands": 2, "North Macedonia": 2, "Norway": 2, "Poland": 2,
    "Portugal": 2, "Romania": 2, "Russia": 2, "San Marino": 2, "Serbia": 2,
    "Slovakia": 2, "Slovenia": 2, "Spain": 2, "Sweden": 2, "Switzerland": 2,
    "Turkey": 2, "Ukraine": 2, "United Kingdom": 2, "Vatican City": 2,
    "Armenia": 2, "Azerbaijan": 2,

    # ── Zone 3 : Amérique ───────────────────────────────────
    "Antigua and Barbuda": 3, "Argentina": 3, "Bahamas": 3, "Barbados": 3,
    "Belize": 3, "Bolivia": 3, "Brazil": 3, "Canada": 3, "Chile": 3,
    "Colombia": 3, "Comoros": 3, "Costa Rica": 3, "Cuba": 3, "Dominica": 3,
    "Dominican Republic": 3, "Ecuador": 3, "El Salvador": 3, "Grenada": 3,
    "Guatemala": 3, "Guyana": 3, "Haiti": 3, "Honduras": 3, "Jamaica": 3,
    "Mexico": 3, "Nicaragua": 3, "Panama": 3, "Paraguay": 3, "Peru": 3,
    "Puerto Rico": 3, "Saint Kitts and Nevis": 3, "Saint Lucia": 3,
    "Saint Vincent and the Grenadines": 3, "Suriname": 3, "Trinidad and Tobago": 3,
    "United States": 3, "Uruguay": 3, "Venezuela": 3,

    # ── Zone 4 : Afrique ────────────────────────────────────
    "Algeria": 4, "Angola": 4, "Benin": 4, "Botswana": 4, "Burkina Faso": 4,
    "Burundi": 4, "Cabo Verde": 4, "Cameroon": 4, "Central African Republic": 4,
    "Chad": 4, "Congo": 4, "Djibouti": 4, "Egypt": 4, "Equatorial Guinea": 4,
    "Eritrea": 4, "Eswatini": 4, "Ethiopia": 4, "Gabon": 4, "Gambia": 4,
    "Ghana": 4, "Guinea": 4, "Guinea-Bissau": 4, "Ivory Coast": 4, "Kenya": 4,
    "Lesotho": 4, "Liberia": 4, "Libya": 4, "Madagascar": 4, "Malawi": 4,
    "Mali": 4, "Mauritania": 4, "Mauritius": 4, "Morocco": 4, "Mozambique": 4,
    "Namibia": 4, "Niger": 4, "Nigeria": 4, "Rwanda": 4,
    "Sao Tome and Principe": 4, "Senegal": 4, "Seychelles": 4, "Sierra Leone": 4,
    "Somalia": 4, "South Africa": 4, "South Sudan": 4, "Sudan": 4, "Tanzania": 4,
    "Togo": 4, "Tunisia": 4, "Uganda": 4, "Zambia": 4, "Zimbabwe": 4,

    # ── Zone 5 : Asie / Océanie ─────────────────────────────
    "Afghanistan": 5, "Australia": 5, "Bahrain": 5, "Bangladesh": 5,
    "Bhutan": 5, "Brunei": 5, "Cambodia": 5, "China": 5, "Fiji": 5,
    "Hong Kong": 5, "India": 5, "Indonesia": 5, "Iran": 5, "Iraq": 5,
    "Israel": 5, "Japan": 5, "Jordan": 5, "Kazakhstan": 5, "Kiribati": 5,
    "Korea South": 5, "Kuwait": 5, "Kyrgyzstan": 5, "Laos": 5, "Lebanon": 5,
    "Malaysia": 5, "Maldives": 5, "Marshall Islands": 5, "Micronesia": 5,
    "Mongolia": 5, "Myanmar": 5, "Nauru": 5, "Nepal": 5, "New Zealand": 5,
    "Oman": 5, "Pakistan": 5, "Palau": 5, "Palestine": 5, "Papua New Guinea": 5,
    "Philippines": 5, "Qatar": 5, "Samoa": 5, "Saudi Arabia": 5,
    "Singapore": 5, "Solomon Islands": 5, "Sri Lanka": 5, "Syria": 5,
    "Taiwan": 5, "Tajikistan": 5, "Thailand": 5, "Timor-Leste": 5, "Tonga": 5,
    "Turkmenistan": 5, "Tuvalu": 5, "United Arab Emirates": 5, "Uzbekistan": 5,
    "Vanuatu": 5, "Vietnam": 5, "Yemen": 5,
}

ZONE_NAMES = {
    1: "Italie",
    2: "Europe",
    3: "Amérique",
    4: "Afrique",
    5: "Asie/Océanie",
}

ZONE_COLORS = {
    1: "#FF6B6B",
    2: "#4ECDC4",
    3: "#FFD93D",
    4: "#6BCB77",
    5: "#9B59B6",
}

# Grouper les pays par zone
COUNTRIES_BY_ZONE = {}
for country, zone_id in COUNTRY_ZONE_MAP.items():
    COUNTRIES_BY_ZONE.setdefault(zone_id, []).append(country)


def get_zone_id(country: str) -> int:
    return COUNTRY_ZONE_MAP.get(country, 5)


def get_zone_name(country: str) -> str:
    zone_id = get_zone_id(country)
    return ZONE_NAMES.get(zone_id, "Asie/Océanie")


def get_countries_for_zone(zone_id: int):
    return sorted(COUNTRIES_BY_ZONE.get(zone_id, []))


def get_all_countries():
    return sorted(COUNTRY_ZONE_MAP.keys())