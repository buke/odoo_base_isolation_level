# -*- coding: utf-8 -*-
import json
import operator
from odoo.models import BaseModel
from psycopg2 import IntegrityError, OperationalError, errorcodes

class OptLockError(OperationalError):
    pgcode = errorcodes.SERIALIZATION_FAILURE

def _write(self, vals):
    # low-level implementation of write()
    self.check_field_access_rights('write', list(vals))

    cr = self._cr

    # for recomputing new-style fields
    extra_fields = ['write_date', 'write_uid'] if self._log_access else []
    self.modified(list(vals) + extra_fields)

    # for updating parent_left, parent_right
    parents_changed = []
    if self._parent_store and (self._parent_name in vals) and \
            not self._context.get('defer_parent_store_computation'):
        # The parent_left/right computation may take up to 5 seconds. No
        # need to recompute the values if the parent is the same.
        #
        # Note: to respect parent_order, nodes must be processed in
        # order, so ``parents_changed`` must be ordered properly.
        parent_val = vals[self._parent_name]
        if parent_val:
            query = "SELECT id FROM %s WHERE id IN %%s AND (%s != %%s OR %s IS NULL) ORDER BY %s" % \
                            (self._table, self._parent_name, self._parent_name, self._parent_order)
            cr.execute(query, (tuple(self.ids), parent_val))
        else:
            query = "SELECT id FROM %s WHERE id IN %%s AND (%s IS NOT NULL) ORDER BY %s" % \
                            (self._table, self._parent_name, self._parent_order)
            cr.execute(query, (tuple(self.ids),))
        parents_changed = map(operator.itemgetter(0), cr.fetchall())

    updates = []            # list of (column, expr) or (column, pattern, value)
    upd_todo = []           # list of column names to set explicitly
    updend = []             # list of possibly inherited field names
    direct = []             # list of direcly updated columns
    has_trans = self.env.lang and self.env.lang != 'en_US'
    single_lang = len(self.env['res.lang'].get_installed()) <= 1
    for name, val in vals.iteritems():
        field = self._fields[name]
        if field and field.deprecated:
            _logger.warning('Field %s.%s is deprecated: %s', self._name, name, field.deprecated)
        if field.store:
            if hasattr(field, 'selection') and val:
                self._check_selection_field_value(name, val)
            if field.column_type:
                if single_lang or not (has_trans and field.translate is True):
                    # val is not a translation: update the table
                    val = field.convert_to_column(val, self)
                    updates.append((name, field.column_format, val))
                direct.append(name)
            else:
                upd_todo.append(name)
        else:
            updend.append(name)

    if self._log_access:
        updates.append(('write_uid', '%s', self._uid))
        updates.append(('write_date', "(now() at time zone 'UTC')"))
        direct.append('write_uid')
        direct.append('write_date')

    if updates:
        self.check_access_rule('write')

        if not self._log_access:
            query = 'UPDATE "%s" SET %s WHERE id IN %%s' % (
                self._table, ','.join('"%s"=%s' % (u[0], u[1]) for u in updates),
            )
            params = tuple(u[2] for u in updates if len(u) > 2)
            for sub_ids in cr.split_for_in_conditions(set(self.ids)):
                cr.execute(query, params + (sub_ids,))
                if cr.rowcount != len(sub_ids):
                    raise MissingError(_('One of the records you are trying to modify has already been deleted (Document type: %s).') % self._description)
        else: # optimistic lock
            params = tuple(u[2] for u in updates if len(u) > 2)
            for sub_ids in cr.split_for_in_conditions(set(self.ids)):
                sub_params = params
                ver_query = 'SELECT id, write_date FROM %s WHERE id IN %%s' % (self._table,)
                cr.execute(ver_query, (sub_ids,))
                if cr.rowcount != len(sub_ids):
                    raise MissingError(_('One of the records you are trying to modify has already been deleted (Document type: %s).') % self._description)
                rows = cr.fetchall()
                query = 'UPDATE "%s" SET %s WHERE %s RETURNING "id"'  % (
                    self._table, 
                    ','.join('"%s"=%s' % (u[0], u[1]) for u in updates),
                    ' OR '.join('("id" = %s AND "write_date" = %s)' for r in rows)
                )
                for r in rows:
                    sub_params += (r[0], r[1])

                cr.execute(query, sub_params)
                if cr.rowcount != len(sub_ids):
                    raise OptLockError("OPTIMISTIC LOCK NOT AVAILABLE: %s %s %s" % (self._name, str(sub_ids), json.dumps(updates)))

        # TODO: optimize
        for name in direct:
            field = self._fields[name]
            if callable(field.translate):
                # The source value of a field has been modified,
                # synchronize translated terms when possible.
                self.env['ir.translation']._sync_terms_translations(self._fields[name], self)

            elif has_trans and field.translate:
                # The translated value of a field has been modified.
                src_trans = self.read([name])[0][name]
                if not src_trans:
                    # Insert value to DB
                    src_trans = vals[name]
                    self.with_context(lang=None).write({name: src_trans})
                val = field.convert_to_column(vals[name], self)
                tname = "%s,%s" % (self._name, name)
                self.env['ir.translation']._set_ids(
                    tname, 'model', self.env.lang, self.ids, val, src_trans)

    # invalidate and mark new-style fields to recompute; do this before
    # setting other fields, because it can require the value of computed
    # fields, e.g., a one2many checking constraints on records
    self.modified(direct)

    # defaults in context must be removed when call a one2many or many2many
    rel_context = {key: val
                    for key, val in self._context.iteritems()
                    if not key.startswith('default_')}

    # call the 'write' method of fields which are not columns
    for name in upd_todo:
        field = self._fields[name]
        field.write(self.with_context(rel_context), vals[name])

    # for recomputing new-style fields
    self.modified(upd_todo)

    # write inherited fields on the corresponding parent records
    unknown_fields = set(updend)
    for parent_model, parent_field in self._inherits.iteritems():
        parent_ids = []
        for sub_ids in cr.split_for_in_conditions(self.ids):
            query = "SELECT DISTINCT %s FROM %s WHERE id IN %%s" % (parent_field, self._table)
            cr.execute(query, (sub_ids,))
            parent_ids.extend([row[0] for row in cr.fetchall()])

        parent_vals = {}
        for name in updend:
            field = self._fields[name]
            if field.inherited and field.related[0] == parent_field:
                parent_vals[name] = vals[name]
                unknown_fields.discard(name)

        if parent_vals:
            self.env[parent_model].browse(parent_ids).write(parent_vals)

    if unknown_fields:
        _logger.warning('No such field(s) in model %s: %s.', self._name, ', '.join(unknown_fields))

    # check Python constraints
    self._validate_fields(vals)

    # TODO: use _order to set dest at the right position and not first node of parent
    # We can't defer parent_store computation because the stored function
    # fields that are computer may refer (directly or indirectly) to
    # parent_left/right (via a child_of domain)
    if parents_changed:
        if self.pool._init:
            self.pool._init_parent[self._name] = True
        else:
            parent_val = vals[self._parent_name]
            if parent_val:
                clause, params = '%s=%%s' % self._parent_name, (parent_val,)
            else:
                clause, params = '%s IS NULL' % self._parent_name, ()

            for id in parents_changed:
                # determine old parent_left, parent_right of current record
                cr.execute('SELECT parent_left, parent_right FROM %s WHERE id=%%s' % self._table, (id,))
                pleft0, pright0 = cr.fetchone()
                width = pright0 - pleft0 + 1

                # determine new parent_left of current record; it comes
                # right after the parent_right of its closest left sibling
                # (this CANNOT be fetched outside the loop, as it needs to
                # be refreshed after each update, in case several nodes are
                # sequentially inserted one next to the other)
                pleft1 = None
                cr.execute('SELECT id, parent_right FROM %s WHERE %s ORDER BY %s' % \
                            (self._table, clause, self._parent_order), params)
                for (sibling_id, sibling_parent_right) in cr.fetchall():
                    if sibling_id == id:
                        break
                    pleft1 = (sibling_parent_right or 0) + 1
                if not pleft1:
                    # the current record is the first node of the parent
                    if not parent_val:
                        pleft1 = 0          # the first node starts at 0
                    else:
                        cr.execute('SELECT parent_left FROM %s WHERE id=%%s' % self._table, (parent_val,))
                        pleft1 = cr.fetchone()[0] + 1

                if pleft0 < pleft1 <= pright0:
                    raise UserError(_('Recursivity Detected.'))

                # make some room for parent_left and parent_right at the new position
                cr.execute('UPDATE %s SET parent_left=parent_left+%%s WHERE %%s<=parent_left' % self._table, (width, pleft1))
                cr.execute('UPDATE %s SET parent_right=parent_right+%%s WHERE %%s<=parent_right' % self._table, (width, pleft1))
                # slide the subtree of the current record to its new position
                if pleft0 < pleft1:
                    cr.execute('''UPDATE %s SET parent_left=parent_left+%%s, parent_right=parent_right+%%s
                                    WHERE %%s<=parent_left AND parent_left<%%s''' % self._table,
                                (pleft1 - pleft0, pleft1 - pleft0, pleft0, pright0))
                else:
                    cr.execute('''UPDATE %s SET parent_left=parent_left-%%s, parent_right=parent_right-%%s
                                    WHERE %%s<=parent_left AND parent_left<%%s''' % self._table,
                                (pleft0 - pleft1 + width, pleft0 - pleft1 + width, pleft0 + width, pright0 + width))

            self.invalidate_cache(['parent_left', 'parent_right'])

    # recompute new-style fields
    if self.env.recompute and self._context.get('recompute', True):
        self.recompute()

    self.step_workflow()
    return True

BaseModel._write = _write

#  vim:et:si:sta:ts=4:sts=4:sw=4:tw=79:
