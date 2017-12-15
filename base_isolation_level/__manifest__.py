# -*- coding: utf-8 -*-
{
    'name' : 'Optimistic concurrency control',
    'author': "wangbuke@gmail.com",
    'version' : '1.0',
    'summary': '',
    'sequence': 10,
    'description': """
# odoo_base_isolation_level

## Set odoo isolation level to READ_COMMITTED
https://www.postgresql.org/docs/10/static/transaction-iso.html

## use write_date for Optimistic concurrency control 

## Avoid odoo exception "Could not serialize access due to concurrent update"

    """,
    'category': 'Hidden',
    'website': '',
    'depends' : ['base'],
    'installable': True,
    'application': True,
    'auto_install': True,

}
