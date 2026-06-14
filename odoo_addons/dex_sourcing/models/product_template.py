from odoo import models, fields


class DexProductTemplate(models.Model):
    _inherit = 'product.template'

    x_ville = fields.Char(string='Ville', index=True)
    x_country = fields.Char(string='Country', index=True)
    x_activity_type = fields.Selection(
        selection=[
            ('EXCURSION', 'Excursion'),
            ('TICKET', 'Ticket'),
            ('TRANSFER', 'Transfer'),
        ],
        string='Activity Type',
        index=True,
    )
    x_category_dex = fields.Char(string='DEX Category')
    x_duration_minutes = fields.Integer(string='Duration (min)')
    x_supplier_name = fields.Char(string='Supplier Name')
    x_supplier_domain = fields.Char(string='Supplier Website')
    x_price_estimated = fields.Boolean(string='Price Estimated by AI', default=False)
    x_original_price = fields.Float(string='Original Price', digits=(10, 2))
    x_original_currency = fields.Char(string='Original Currency')
    x_ai_agent_source = fields.Boolean(string='Inserted by AI Agent', default=True)
    x_source_url = fields.Char(string='Source URL')
    x_booking_url = fields.Char(string='Booking URL')
    x_scraping_method = fields.Selection(
        selection=[
            ('scrapy', 'Scrapy (static)'),
            ('playwright', 'Playwright (dynamic)'),
            ('jsonld', 'JSON-LD'),
            ('api', 'API'),
        ],
        string='Scraping Method',
    )
    x_run_id = fields.Char(string='Sourcing Run ID', index=True)
    x_similarity_score = fields.Float(string='Similarity Score', digits=(5, 4))
    x_inserted_at = fields.Datetime(string='Inserted by Agent At')
