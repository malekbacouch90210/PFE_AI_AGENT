# utils/formatters.py
import re
import unicodedata


def normalize_text(text: str) -> str:
    """Normalize text: lowercase, remove accents, special chars."""
    if not text:
        return ""
    text = text.lower().strip()
    # Remove accents
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    # Remove special chars, keep alphanum + spaces
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def clean_domain(url: str) -> str:
    """Extract clean domain from URL."""
    if not url:
        return ""
    domain = re.sub(r'^https?://(www\.)?', '', url)
    domain = domain.split('/')[0].split('?')[0].lower().strip()
    return domain


def clean_price(value) -> float | None:
    """Parse price from various formats."""
    if value is None:
        return None
    try:
        s = str(value).replace(',', '.').strip()
        s = re.sub(r'[^\d.]', '', s)
        return float(s) if s else None
    except Exception:
        return None


def format_currency(amount: float, currency: str = "EUR") -> str:
    if amount is None:
        return "N/A"
    symbols = {"EUR": "€", "USD": "$", "GBP": "£"}
    sym = symbols.get(currency.upper(), currency)
    return f"{amount:.2f} {sym}"


def truncate(text: str, max_len: int = 100) -> str:
    if not text:
        return ""
    return text[:max_len] + ("..." if len(text) > max_len else "")


def extract_price_from_text(text: str) -> float | None:
    """Extract first price found in text."""
    if not text:
        return None
    patterns = [
        r'(\d+(?:[.,]\d+)?)\s*(?:€|EUR)',
        r'(?:€|EUR)\s*(\d+(?:[.,]\d+)?)',
        r'\$\s*(\d+(?:[.,]\d+)?)',
        r'(\d+(?:[.,]\d+)?)\s*(?:USD)',
        r'from\s+(\d+(?:[.,]\d+)?)',
        r'starting\s+at\s+(\d+(?:[.,]\d+)?)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return clean_price(m.group(1))
    return None


def determine_activity_type(text: str) -> str:
    """Classify activity type from text."""
    t = text.lower()
    if re.search(r'transfer|shuttle|airport|chauffeur|limousine|taxi|navette|pickup|minibus', t):
        return 'transfer'
    if re.search(r'ticket|museum|monument|castle|palace|temple|zoo|aquarium|admission|entry|skip.the.line|attraction', t):
        return 'ticket'
    return 'excursion'