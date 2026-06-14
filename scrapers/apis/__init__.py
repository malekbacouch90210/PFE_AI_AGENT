# scrapers/apis/__init__.py
from .scrappa_client import ScrappaClient
from .openstreetmap import NominatimClient

__all__ = ['ScrappaClient', 'NominatimClient']