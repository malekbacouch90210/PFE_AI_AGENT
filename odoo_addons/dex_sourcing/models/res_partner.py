from odoo import models, fields


class DexResPartner(models.Model):
    _inherit = 'res.partner'

    x_supplier_score = fields.Integer(string='Sourcing Score', default=0)
    x_supplier_status = fields.Selection(
        selection=[
            ('discovered', 'Discovered'),
            ('qualified', 'Qualified'),
            ('scraping_pending', 'Scraping Pending'),
            ('scraped', 'Scraped'),
            ('rejected', 'Rejected'),
        ],
        string='Sourcing Status',
        default='discovered',
        index=True,
    )
    x_has_api = fields.Boolean(string='Has API', default=False)
    x_api_name = fields.Char(string='API Name')
    x_has_channel_manager = fields.Boolean(string='Has Channel Manager', default=False)
    x_channel_manager_name = fields.Char(string='Channel Manager Name')
    x_connection_type = fields.Selection(
        selection=[
            ('API', 'API only'),
            ('CHANNEL', 'Channel Manager only'),
            ('BOTH', 'API + Channel Manager'),
            ('NONE', 'No connection'),
        ],
        string='Connection Type',
        default='NONE',
    )
    x_supplier_ville = fields.Char(string='Main City')
    x_zone = fields.Selection(
        selection=[
            ('1', 'Italie'),
            ('2', 'Europe'),
            ('3', 'Amerique'),
            ('4', 'Afrique'),
            ('5', 'Asie / Oceanie'),
        ],
        string='DEX Zone',
    )
    x_run_id = fields.Char(string='Discovery Run ID', index=True)
    x_discovered_at = fields.Datetime(string='Discovered At')
    x_products_count_scraped = fields.Integer(string='Products Scraped', default=0)
    x_products_count_accepted = fields.Integer(string='Products Accepted', default=0)
