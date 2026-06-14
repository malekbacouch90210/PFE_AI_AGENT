# services/url_classifier.py
"""
URL Classifier — Detects if a URL is a real product page or not.
Uses URL patterns + content signals.
"""

import re
from typing import Optional


class URLClassifier:
    """
    Classifies URLs as product pages or non-product pages.
    """

    # Strong product indicators in URL
    PRODUCT_URL_PATTERNS = [
        r'/tour/', r'/tours/', r'/ticket/', r'/tickets/',
        r'/activity/', r'/activities/', r'/experience/', r'/experiences/',
        r'/transfer/', r'/transfers/', r'/excursion/', r'/excursions/',
        r'/attraction/', r'/attractions/', r'/event/', r'/events/',
        r'-t\d+', r'/d\d+-', r'/product/', r'/booking/',
    ]

    # Definitely NOT product pages
    NON_PRODUCT_URL_PATTERNS = [
        r'/blog/', r'/guide/', r'/things-to-do/?$', r'/search\?',
        r'/tag/', r'/press/', r'/about/', r'/contact/',
        r'/help/', r'/faq/', r'/sitemap', r'/category/',
        r'/destinations/', r'/inspiration/', r'/deals/',
        r'/privacy/', r'/terms/', r'/careers/', r'/login/',
        r'/signup/', r'/register/', r'/account/',
    ]

    # Content signals that indicate a real product page
    PRODUCT_CONTENT_SIGNALS = [
        '"@type": "Product"',
        '"@type": "TouristTrip"',
        '"@type": "Event"',
        '"offers"',
        '"price"',
        '"aggregateRating"',
        'booking',
        'reserve',
        'checkout',
        'add-to-cart',
        'book-now',
        'book_now',
        'from-price',
        'per-person',
        'per_person',
    ]

    @classmethod
    def is_product_url(cls, url: str, html: str = "") -> bool:
        """
        Determine if a URL is likely a product page.
        Returns True if it's a product, False otherwise.
        """
        if not url:
            return False

        url_lower = url.lower()

        # Check non-product patterns first (negative match)
        for pattern in cls.NON_PRODUCT_URL_PATTERNS:
            if re.search(pattern, url_lower):
                return False

        # Check product URL patterns
        url_product = False
        for pattern in cls.PRODUCT_URL_PATTERNS:
            if re.search(pattern, url_lower):
                url_product = True
                break

        # If URL doesn't look like a product, check content signals
        if not url_product and html:
            html_lower = html.lower()[:10000]  # First 10KB
            content_signals = sum(1 for s in cls.PRODUCT_CONTENT_SIGNALS if s.lower() in html_lower)
            return content_signals >= 2  # Need at least 2 signals

        return url_product

    @classmethod
    def classify_page_type(cls, url: str) -> str:
        """Classify the page type."""
        url_lower = url.lower()

        if re.search(r'/blog/', url_lower): return "BLOG"
        if re.search(r'/things-to-do/?$', url_lower): return "CITY_GUIDE"
        if re.search(r'/category/', url_lower): return "CATEGORY"
        if re.search(r'/destinations/', url_lower): return "DESTINATION"
        if re.search(r'/search\?', url_lower): return "SEARCH"

        # Product patterns
        for pattern in cls.PRODUCT_URL_PATTERNS:
            if re.search(pattern, url_lower):
                return "PRODUCT"

        return "UNKNOWN"