#coding: utf-8

#Overwrite ReadDictConnection for return rows with upper case field names.

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursorBase

class RealDictConnectionUpper(_connection):
    """A connection that uses `RealDictCursor` automatically."""
    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', RealDictCursorUpper)
        return super(RealDictConnectionUpper, self).cursor(*args, **kwargs)


class RealDictCursorUpper(DictCursorBase):
    """A cursor that uses a real dict as the base type for rows.

    Note that this cursor is extremely specialized and does not allow
    the normal access (using integer indices) to fetched data. If you need
    to access database rows both as a dictionary and a list, then use
    the generic `DictCursor` instead of `!RealDictCursor`.
    """
    def __init__(self, *args, **kwargs):
        kwargs['row_factory'] = RealDictRowUpper
        super(RealDictCursorUpper, self).__init__(*args, **kwargs)
        self._prefetch = 0

    def execute(self, query, vars=None):
        self.column_mapping = []
        self._query_executed = 1
        return super(RealDictCursorUpper, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.column_mapping = []
        self._query_executed = 1
        return super(RealDictCursorUpper, self).callproc(procname, vars)

    def _build_index(self):
        if self._query_executed == 1 and self.description:
            for i in range(len(self.description)):
                self.column_mapping.append(self.description[i][0].upper())
            self._query_executed = 0

class RealDictRowUpper(dict):
    """A `!dict` subclass representing a data record."""

    __slots__ = ('_column_mapping')

    def __init__(self, cursor):
        dict.__init__(self)
        # Required for named cursors
        if cursor.description and not cursor.column_mapping:
            cursor._build_index()

        self._column_mapping = cursor.column_mapping

    def __setitem__(self, name, value):
        if type(name) == int:
            name = self._column_mapping[name]
        return dict.__setitem__(self, name, value)

    def __getstate__(self):
        return (self.copy(), self._column_mapping[:])

    def __setstate__(self, data):
        self.update(data[0])
        self._column_mapping = data[1]
