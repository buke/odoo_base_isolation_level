# -*- coding: utf-8 -*-
from odoo.sql_db import Cursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_REPEATABLE_READ

def autocommit(self, on):
    if on:
        isolation_level = ISOLATION_LEVEL_AUTOCOMMIT
    else:
        isolation_level = ISOLATION_LEVEL_READ_COMMITTED
    self._cnx.set_isolation_level(isolation_level)

Cursor.autocommit = autocommit

#  vim:et:si:sta:ts=4:sts=4:sw=4:tw=79:
