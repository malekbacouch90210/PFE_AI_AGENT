"""
services/odoo/odoo_client.py — Odoo 19 Community XML-RPC client

Why XML-RPC and not REST:
  Odoo 19 Community does not ship a full REST API.
  XML-RPC is the official external integration method.
  We call two endpoints:
    /web/dataset/call_kw  → JSON-RPC (create, search, write, read)
    /xmlrpc/2/common     → authenticate
    /xmlrpc/2/object     → execute model methods

WHAT THIS DOES:
  - Authenticate against Odoo
  - Create product.template records (ACCEPTED scraped products)
  - Create res.partner records (fournisseurs/suppliers)
  - Check for duplicates before insert (by x_run_id + name)
  - Update supplier stats after product insert
"""
import asyncio
import xmlrpc.client
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from config import config


class OdooClient:
    """
    Odoo 19 XML-RPC client.

    Config (add to .env):
      ODOO_URL=http://localhost:8069
      ODOO_DB=odoo
      ODOO_USER=admin
      ODOO_PASSWORD=admin

    Odoo models we use (from dex_sourcing addon):
      product.template  → products (with x_* DEX fields)
      res.partner       → suppliers (with x_* DEX fields)
    """

    def __init__(self):
        self.url      = config.ODOO_URL
        self.db       = config.ODOO_DB
        self.user     = config.ODOO_USER
        self.password = config.ODOO_PASSWORD
        self.uid: Optional[int] = None
        self._common = None
        self._models = None

    def _get_common(self):
        if not self._common:
            self._common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        return self._common

    def _get_models(self):
        if not self._models:
            self._models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        return self._models

    async def authenticate(self) -> int:
        """Authenticate and return uid."""
        if self.uid:
            return self.uid

        def _auth():
            common = self._get_common()
            return common.authenticate(self.db, self.user, self.password, {})

        loop = asyncio.get_event_loop()
        uid  = await loop.run_in_executor(None, _auth)

        if not uid:
            raise ConnectionError(
                f"❌ Odoo auth failed. Check:\n"
                f"  ODOO_URL={self.url}\n"
                f"  ODOO_DB={self.db}\n"
                f"  ODOO_USER={self.user}\n"
                f"  ODOO_PASSWORD=***"
            )

        self.uid = uid
        logger.info(f"  ✅ Odoo authenticated (uid={uid}) at {self.url}")
        return uid

    async def execute(
        self,
        model:  str,
        method: str,
        args:   list,
        kwargs: dict = None,
    ) -> Any:
        """Execute any Odoo model method via XML-RPC."""
        uid = await self.authenticate()

        def _exec():
            models = self._get_models()
            return models.execute_kw(
                self.db, uid, self.password,
                model, method, args, kwargs or {},
            )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _exec)

    # ── Products ──────────────────────────────────────────────

    async def search_product(
        self,
        name: str,
        run_id: Optional[str] = None,
    ) -> Optional[int]:
        """Check if product already exists in Odoo (dedup)."""
        domain = [("name", "=", name), ("x_ai_agent_source", "=", True)]
        if run_id:
            domain.append(("x_run_id", "=", run_id))

        ids = await self.execute("product.template", "search", [domain])
        return ids[0] if ids else None

    async def create_product(self, vals: Dict) -> Optional[int]:
        """
        Create a product.template in Odoo.
        Returns the new product id, or None on error.
        """
        try:
            product_id = await self.execute("product.template", "create", [vals])
            return product_id
        except Exception as e:
            logger.error(f"  create_product error: {e}")
            return None

    async def search_or_create_product(self, vals: Dict) -> Tuple[int, bool]:
        """
        Returns (odoo_id, created).
        created=True if new, False if already existed.
        """
        existing = await self.search_product(vals.get("name", ""), vals.get("x_run_id"))
        if existing:
            return existing, False
        new_id = await self.create_product(vals)
        return new_id, True

    # ── Suppliers ─────────────────────────────────────────────

    async def search_supplier(self, name: str, domain: str = "") -> Optional[int]:
        """Check if supplier already exists."""
        search_domain = [("name", "=", name), ("x_run_id", "!=", False)]
        if domain:
            search_domain.append(("website", "ilike", domain))
        ids = await self.execute("res.partner", "search", [search_domain])
        return ids[0] if ids else None

    async def create_supplier(self, vals: Dict) -> Optional[int]:
        """Create a res.partner (supplier) in Odoo."""
        try:
            supplier_id = await self.execute("res.partner", "create", [vals])
            return supplier_id
        except Exception as e:
            logger.error(f"  create_supplier error: {e}")
            return None

    async def update_supplier_stats(self, supplier_name: str, accepted: int, scraped: int):
        """Update supplier product counts after batch insert."""
        try:
            ids = await self.execute(
                "res.partner", "search",
                [[("name", "=", supplier_name), ("x_run_id", "!=", False)]]
            )
            if ids:
                await self.execute(
                    "res.partner", "write",
                    [ids, {
                        "x_products_count_accepted": accepted,
                        "x_products_count_scraped":  scraped,
                        "x_supplier_status": "scraped",
                    }]
                )
        except Exception as e:
            logger.debug(f"  update_supplier_stats error: {e}")

    # ── Health check ──────────────────────────────────────────

    async def health_check(self) -> bool:
        try:
            version = self._get_common().version()
            logger.info(f"  ✅ Odoo {version.get('server_version','?')} at {self.url}")
            return True
        except Exception as e:
            logger.error(f"  ❌ Odoo not reachable: {e}")
            return False


# Singleton
odoo_client = OdooClient()
