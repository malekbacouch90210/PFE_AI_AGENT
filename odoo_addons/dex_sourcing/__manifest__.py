{
    'name': 'DEX Sourcing Agent',
    'version': '19.0.3.0.0',
    'category': 'Tourism/Sourcing',
    'summary': 'AI Sourcing Agent — Travel Products, Suppliers, Analytics, Website',
    'author': 'DEX AI Sourcing Agent PFE',
    'depends': [
        'product',
        'purchase',
        'stock',
        'contacts'
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/product_views.xml',
        'views/supplier_views.xml',
        'views/dashboard_views.xml',
        'views/menu_views.xml',
        'templates/website_dex.xml',
    ],
    # Odoo 19 correct way to register static assets
    'assets': {
        'web.assets_frontend': [
            'dex_sourcing/static/src/css/dex_website.css',
        ],
    },
    'installable': True,
    'application': True,
    'auto_install': False,
    'license': 'LGPL-3',
}
