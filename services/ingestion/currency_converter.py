"""
services/ingestion/currency_converter.py
Currency conversion with 2026 exchange rates.
Pure math — no external API needed.
"""
import re
from typing import Optional, Tuple

# ─────────────────────────────────────────────────────────────
# Exchange rates to EUR — 2026
# ─────────────────────────────────────────────────────────────
RATES_2026: dict[str, float] = {
    "EUR": 1.0,
    "USD": 0.92,
    "GBP": 1.16,
    "CHF": 1.03,
    "JPY": 0.0059,
    "TND": 0.30,
    "MAD": 0.092,
    "EGP": 0.018,
    "DZD": 0.0069,
    "LYD": 0.19,
    "BRL": 0.17,
    "MXN": 0.045,
    "ARS": 0.00090,
    "COP": 0.00022,
    "PEN": 0.24,
    "CLP": 0.00099,
    "IDR": 0.000056,
    "THB": 0.026,
    "VND": 0.000036,
    "INR": 0.011,
    "CNY": 0.13,
    "KRW": 0.00066,
    "SGD": 0.68,
    "HKD": 0.12,
    "MYR": 0.20,
    "PHP": 0.016,
    "PKR": 0.0032,
    "LKR": 0.0030,
    "NPR": 0.0069,
    "BDT": 0.0079,
    "AUD": 0.60,
    "NZD": 0.56,
    "CAD": 0.67,
    "ZAR": 0.048,
    "KES": 0.0071,
    "NGN": 0.00057,
    "GHS": 0.058,
    "XOF": 0.0015,
    "XAF": 0.0015,
    "SAR": 0.25,
    "AED": 0.25,
    "QAR": 0.25,
    "KWD": 3.02,
    "BHD": 2.44,
    "OMR": 2.39,
    "JOD": 1.30,
    "ILS": 0.25,
    "TRY": 0.026,
    "UAH": 0.023,
    "PLN": 0.23,
    "CZK": 0.040,
    "HUF": 0.0025,
    "RON": 0.20,
    "BGN": 0.51,
    "HRK": 0.13,
    "RSD": 0.0085,
    "MKD": 0.016,
    "ALL": 0.010,
    "BAM": 0.51,
    "NOK": 0.086,
    "SEK": 0.088,
    "DKK": 0.13,
    "ISK": 0.0069,
    "RUB": 0.010,
    "GEL": 0.35,
    "AMD": 0.0024,
    "AZN": 0.54,
    "KZT": 0.0019,
    "UZS": 0.000073,
    "MXN": 0.045,
    "CRC": 0.0017,
    "GTQ": 0.12,
    "HNL": 0.038,
    "NIO": 0.025,
    "PAB": 0.92,
    "CUP": 0.036,
    "JMD": 0.0057,
    "TTD": 0.14,
    "BBD": 0.46,
    "BSD": 0.92,
    "XCD": 0.34,
    "KYD": 1.10,
    "AWG": 0.51,
    "ANG": 0.51,
}

# Currency symbols → ISO code
SYMBOL_MAP = {
    "€":  "EUR",
    "$":  "USD",
    "£":  "GBP",
    "¥":  "JPY",
    "₹":  "INR",
    "₩":  "KRW",
    "₺":  "TRY",
    "₽":  "RUB",
    "฿":  "THB",
    "₫":  "VND",
    "₴":  "UAH",
    "₦":  "NGN",
    "₵":  "GHS",
    "₸":  "KZT",
    "د.ت": "TND",
    "د.م": "MAD",
}

# Eurozone countries — default EUR if currency unclear
EUROZONE = {
    "france", "germany", "italy", "spain", "portugal", "greece",
    "netherlands", "belgium", "austria", "finland", "ireland",
    "luxembourg", "malta", "cyprus", "estonia", "latvia", "lithuania",
    "slovakia", "slovenia",
}


def parse_price_string(price_raw: str) -> Tuple[Optional[float], str]:
    """
    Parse raw price string into (amount, currency_iso).
    Handles: "45€", "From $120", "USD 99.00", "120 TND", etc.
    Returns (None, 'EUR') if unparseable.
    """
    if not price_raw:
        return None, "EUR"

    text = str(price_raw).strip()

    # Detect currency symbol or code
    currency = "EUR"
    for sym, iso in SYMBOL_MAP.items():
        if sym in text:
            currency = iso
            text = text.replace(sym, " ")
            break
    else:
        # Try ISO code detection
        upper = text.upper()
        for iso in RATES_2026:
            if iso in upper and iso != "EUR":
                currency = iso
                upper = upper.replace(iso, " ")
                text = upper
                break

    # Remove non-numeric except . and ,
    text = re.sub(r"[^0-9.,]", " ", text)
    text = text.replace(",", ".")

    # Extract first number
    m = re.search(r"\d+(?:\.\d+)?", text)
    if m:
        try:
            val = float(m.group())
            if 1 <= val <= 99999:
                return round(val, 2), currency
        except ValueError:
            pass

    return None, currency


def convert_to_eur(amount: float, currency: str) -> float:
    """
    Convert amount from currency to EUR.
    Returns same amount if currency is EUR or unknown.
    """
    if not currency or currency.upper() == "EUR":
        return round(amount, 2)
    rate = RATES_2026.get(currency.upper())
    if rate:
        return round(amount * rate, 2)
    # Unknown currency — log and return as-is
    return round(amount, 2)


def get_default_price(activity_type: str) -> float:
    """
    Fallback price if AI estimation fails.
    Based on typical tourism market prices.
    """
    defaults = {
        "EXCURSION": 45.0,
        "TICKET":    25.0,
        "TRANSFER":  35.0,
    }
    return defaults.get(activity_type, 40.0)


def is_eurozone(country: str) -> bool:
    return country.lower().strip() in EUROZONE
