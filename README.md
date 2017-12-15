# odoo_base_isolation_level

Set odoo isolation level to READ_COMMITTED, use write_date for Optimistic concurrency control . 
see: https://www.postgresql.org/docs/10/static/transaction-iso.html


Avoid odoo exception "Could not serialize access due to concurrent update"
