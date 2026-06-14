"""
controllers/website_controller.py — DEX Experience Website routes

Routes:
  GET /dex              → Homepage
  GET /dex/destinations → All countries list
  GET /dex/country/<c>  → Cities of a country
  GET /dex/city/<c>     → Products in a city (with ?type=excursion|transfer)
  GET /dex/product/<id> → Product detail
  GET /dex/search       → Search results (?q=...)
  GET /dex/type/<t>     → By activity type
"""
import json
from odoo import http
from odoo.http import request


class DexWebsite(http.Controller):

    def _get_products_data(self, domain=None):
        """Helper: get products as list of dicts."""
        base_domain = [('x_ai_agent_source', '=', True)]
        if domain:
            base_domain += domain
        products = request.env['product.template'].sudo().search(base_domain, limit=500)
        result = []
        for p in products:
            result.append({
                'id':          p.id,
                'name':        p.name or '',
                'city':        p.x_ville or '',
                'country':     p.x_country or '',
                'type':        p.x_activity_type or 'EXCURSION',
                'category':    p.x_category_dex or '',
                'price':       int(p.list_price) if p.list_price else 0,
                'description': p.description_sale or '',
                'supplier':    p.x_supplier_name or '',
                'supplier_url':p.x_supplier_domain or '',
                'duration':    p.x_duration_minutes or 0,
            })
        return result

    def _get_countries(self):
        """Get countries with product counts."""
        products = request.env['product.template'].sudo().search(
            [('x_ai_agent_source', '=', True), ('x_country', '!=', False)],
            limit=1000
        )
        counts = {}
        for p in products:
            c = p.x_country or 'Unknown'
            counts[c] = counts.get(c, 0) + 1
        return [{'country': k, 'count': v}
                for k, v in sorted(counts.items(), key=lambda x: -x[1])
                if k and k != 'Unknown']

    # ── HOMEPAGE ────────────────────────────────────────────

    @http.route('/dex', type='http', auth='public', website=True)
    def dex_home(self, **kwargs):
        countries = self._get_countries()[:16]  # top 16 countries
        all_products = self._get_products_data()
        # Featured = first 9 products with price
        featured = [p for p in all_products if p['price'] > 0][:9]
        return request.render('dex_sourcing.dex_homepage', {
            'countries':         countries,
            'featured_products': featured,
        })

    # ── DESTINATIONS (all countries) ───────────────────────

    @http.route('/dex/destinations', type='http', auth='public', website=True)
    def dex_destinations(self, **kwargs):
        countries = self._get_countries()
        total = sum(c['count'] for c in countries)
        return request.render('dex_sourcing.dex_destinations', {
            'countries':   countries,
            'total_count': total,
        })

    # ── COUNTRY PAGE ────────────────────────────────────────

    @http.route('/dex/country/<string:country>', type='http', auth='public', website=True)
    def dex_country(self, country, **kwargs):
        products = request.env['product.template'].sudo().search([
            ('x_ai_agent_source', '=', True),
            ('x_country', '=', country),
            ('x_ville', '!=', False),
        ], limit=500)
        city_counts = {}
        for p in products:
            c = p.x_ville or 'Unknown'
            city_counts[c] = city_counts.get(c, 0) + 1
        cities = [{'city': k, 'count': v}
                  for k, v in sorted(city_counts.items(), key=lambda x: -x[1])]
        return request.render('dex_sourcing.dex_country_page', {
            'country_name': country,
            'cities':       cities,
        })

    # ── CITY PAGE ───────────────────────────────────────────

    @http.route('/dex/city/<string:city>', type='http', auth='public', website=True)
    def dex_city(self, city, type=None, **kwargs):
        base_domain = [('x_ai_agent_source', '=', True), ('x_ville', '=', city)]

        # Count per type
        all_prods = request.env['product.template'].sudo().search(base_domain)
        count_excursion = len([p for p in all_prods
                               if p.x_activity_type in ('EXCURSION', 'TICKET')])
        count_transfer  = len([p for p in all_prods
                               if p.x_activity_type == 'TRANSFER'])

        # Filter by requested type
        active_type = type or 'excursion'
        if active_type == 'transfer':
            filtered = [p for p in all_prods if p.x_activity_type == 'TRANSFER']
        else:
            filtered = [p for p in all_prods if p.x_activity_type in ('EXCURSION', 'TICKET')]

        products = [{
            'id':       p.id,
            'name':     p.name or '',
            'type':     p.x_activity_type or 'EXCURSION',
            'category': p.x_category_dex or '',
            'price':    int(p.list_price) if p.list_price else 0,
            'city':     p.x_ville or '',
            'country':  p.x_country or '',
        } for p in filtered]

        return request.render('dex_sourcing.dex_city_page', {
            'city_name':       city,
            'products':        products,
            'active_type':     active_type,
            'count_excursion': count_excursion,
            'count_transfer':  count_transfer,
        })

    # ── PRODUCT DETAIL ──────────────────────────────────────

    @http.route('/dex/product/<int:product_id>', type='http', auth='public', website=True)
    def dex_product(self, product_id, **kwargs):
        p = request.env['product.template'].sudo().browse(product_id)
        if not p.exists() or not p.x_ai_agent_source:
            return request.redirect('/dex')
        product = {
            'id':          p.id,
            'name':        p.name or '',
            'city':        p.x_ville or '',
            'country':     p.x_country or '',
            'type':        p.x_activity_type or 'EXCURSION',
            'category':    p.x_category_dex or '',
            'price':       int(p.list_price) if p.list_price else 0,
            'description': p.description_sale or '',
            'supplier':    p.x_supplier_name or '',
            'supplier_url':p.x_supplier_domain or '',
            'duration':    p.x_duration_minutes or 0,
        }
        return request.render('dex_sourcing.dex_product_detail', {'product': product})

    # ── SEARCH ──────────────────────────────────────────────

    @http.route('/dex/search', type='http', auth='public', website=True)
    def dex_search(self, q='', **kwargs):
        domain = [
            ('x_ai_agent_source', '=', True),
            '|', '|', '|',
            ('name', 'ilike', q),
            ('x_ville', 'ilike', q),
            ('x_country', 'ilike', q),
            ('x_category_dex', 'ilike', q),
        ]
        prods = request.env['product.template'].sudo().search(domain, limit=100)
        products = [{
            'id':      p.id,
            'name':    p.name or '',
            'city':    p.x_ville or '',
            'country': p.x_country or '',
            'price':   int(p.list_price) if p.list_price else 0,
        } for p in prods]
        return request.render('dex_sourcing.dex_search_results', {
            'query':    q,
            'products': products,
        })

    # ── BY TYPE ─────────────────────────────────────────────

    @http.route('/dex/type/<string:activity_type>', type='http', auth='public', website=True)
    def dex_by_type(self, activity_type, **kwargs):
        type_map = {'excursion': 'EXCURSION', 'ticket': 'TICKET', 'transfer': 'TRANSFER'}
        odoo_type = type_map.get(activity_type.lower(), 'EXCURSION')
        products = self._get_products_data([('x_activity_type', '=', odoo_type)])
        return request.render('dex_sourcing.dex_search_results', {
            'query':    f'Type: {odoo_type}',
            'products': products,
        })
