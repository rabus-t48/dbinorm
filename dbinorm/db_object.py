#coding: utf-8

import logging
from dbinorm.db_functions import DictWrapper, db_select, OracleTable
from dbinorm.exception import DbException

log = logging.getLogger()

_REQS = {
    "get"   : "select * from {table_name} where {key_field} = %(key_field)s",
    "insert": "insert into {table_name} ({fields}) values ({values})",
    "update": "update {table_name} set {fields} where {key_field} = %(key_field)s",
    "delete": 'delete from {table_name} where {key_field} = %(key_field)s',
    "select": 'select * from {table} where {where}'
}

import pprint

class DbObject(DictWrapper):

    table = None
    key_field = 'ID'
    table_obj = None

    def __init__(self, config = None, connection = None, table = None, **kargs):
        self._config = config
        self._table = table or self.table
        self._deleted = False
        self._connection = connection
        self._table_obj = None
        self._table_obj = self.table_obj

    def __str__(self):
        return pprint.pformat(self._config)

    @classmethod
    def create(cls, obj_id  = None, connection = None, table = None, **kargs):
        if obj_id == None:
            raise Exception("Object id does not set.")
        obj = cls({cls.key_field: obj_id}, table = table or cls.table, connection = connection)
        obj.load()
        return obj

    @classmethod
    def select(cls, config = None, connection = None, table = None, **kargs):
        # TODO: add where parameter realisation when it will be needed
        table = (table if table != None else cls.table)
        if config == None:
            raise Exception("Object config does not set.")
        where = cls.create_where(config, connection)
        if "condition" in kargs:
            #add space if needed
            c = kargs.get("condition")
            where += (c if c.startswith(" ") else " " + c)
        if "condition_args" in kargs:
            config.update(kargs["condition_args"])
        req = _REQS.get('select').format(
            table = table,
            where = where)
        items = []
        for i in connection.uexec_get_list(req, config, extra_params = {
                'with_lob_values': True,
                'blob_fields': cls.cls_get_lob_fields(connection, table)
        }):
            items.append(cls(i, table = table, connection = connection))
        return items

    @classmethod
    def create_where(cls, config, connection):
        where = u""
        delimeter = u""
        for k, v in config.items():
            if k.isupper():
                where += delimeter + u"{field_name} = {field_value}".format(
                    field_name = k, field_value = connection.nbind(k))
                delimeter = u" and "
        return where

    @property
    def data(self):
        return self._config

    def get_table(self):
        return self._table

    def set_connection(self, connection):
        self._connection = connection
        #TODO: if were inserts, updates, table loading or other,
        #      it should be restored to default

    @classmethod
    def cls_get_lob_fields(cls, connection = None, table = None):
        return cls.cls_get_table_obj(connection = connection, table = table).get_lob_fields()

    @classmethod
    def cls_get_table_obj(cls, connection = None, table = None):
        if cls.table_obj == None:
            cls.table_obj = OracleTable(connection, table)
        return cls.table_obj

    def get_table_obj(self):
        if self._table_obj == None:
            self._table_obj = OracleTable(self._connection, self.get_table())
        return self._table_obj

    def get_lob_fields(self):
        t_obj = self.get_table_obj()
        return t_obj.get_lob_fields()

    def get_key_value(self):
        key_field = self.key_field
        if not isinstance(key_field, (list, tuple)):
            return self._config[self.key_field]
        else:
            key = tuple(self._config[i] for i in key_field)
            return key

    def load(self):
        key_field = self.key_field
        if self._config[key_field] == None:
            raise DbException("Missing {1}  {0} for object could be load.".format(self._table, key_field))
        object_data = self._load()
        self._config.update(object_data)
        return self._config

    def _load(self):
        req = _REQS.get("get") % {"key_field": self._connection.nbind("{key_field}")}
        key_field = self.key_field
        request = req.format(table_name = self._table, key_field = key_field)
        #log.info("req = %s", req)
        object_data = self._connection.uexec_get_list(request, {
            key_field: self._config[key_field]
        }, extra_params = {
            'with_lob_values': True,
            'blob_fields': self.get_lob_fields()
        })
        if len(object_data) == 0:
            raise DbException("Missing object in table {0} with {2} = {1}".format(self._table, self._config[key_field], key_field))
        elif len(object_data) > 1:
            log.info("object_data = %s", object_data)
            raise DbException("Multiple objects in table {0} with {2} = {1}".format(self._table, self._config[key_field], key_field))
        #log.debug("object data = %s", object_data[0])
        return object_data[0]

    def insert(self):
        req = _REQS.get("insert") % {"key_field": self._connection.nbind("{key_field}")}
        key_field = self.key_field
        if self._config[key_field] == None:
            #FIX: may be missing for example for gui_form
            self._config[key_field] = self._connection.get_next_val(self._table + "_S")
        nbind = self._connection.nbind
        keys = self._config.keys()
        fields = ", ".join(keys)
        vals = ", ".join(map(lambda x: nbind(x), keys))
        req = req.format(table_name = self._table, fields = fields, values = vals)
        result = self._connection.exec_req(req, self._config, extra_params = {
            'with_lob_values': True,
            'blob_fields': self.get_lob_fields()
        })
        if result != True:
            raise Exception("Error on insert.")
        return self._config

    def actualize(self):
        """ Sync current node state with database.
        """
        kargs = self._config
        tableObj = self._connection.get_table(self._table)
        fields = tableObj.get_field_names()
        for i in fields:
            if not kargs.has_key(i):
                kargs[i] = None
        kargs = kargs.copy()
        del kargs[self.key_field]
        self.update(**kargs)
        return self._config

    def update(self, **kargs):
        req = _REQS.get("update") % {"key_field": self._connection.nbind("{key_field}")}
        key_field = self.key_field
        if self._config[key_field] == None:
            raise Exception("Missing {key_field}, can not perform update.".format(key_field = key_field))
        if kargs.get(key_field) != None:
            raise Exception("Setted {key_field} for update.".format(key_field = key_field))
        if len(kargs) == 0:
            raise Exception("Nothing set for update.")

        fields = ""
        nbind = self._connection.nbind
        delim = ""
        for i, v in kargs.items():
            fields += delim + "{field} = {value}".format(field = i, value = nbind(i))
            delim = ", "

        update = req.format(table_name = self._table, fields = fields, key_field = key_field)
        params = {key_field: self._config[key_field]}
        params.update(kargs)
        result = self._connection.exec_req(update, params, extra_params = {
            'with_lob_values': True,
            'blob_fields': self.get_lob_fields()
        })
        if result != True:
            raise Exception("Error on update.")
        for i, v in kargs.items():
            self._config[i] = v
        return self._config

    def delete(self):
        req = _REQS.get("delete") % {"key_field": self._connection.nbind("{key_field}")}
        key_field = self.key_field
        if self._config[key_field] == None:
            raise Exception("Missing {key_field}, can not perform update.".format(key_field = key_field))
        delete = req.format(table_name = self._table, key_field = key_field)
        result = self._connection.exec_req(delete, {key_field: self._config[key_field]})
        if result != True:
            raise Exception("Error on delete.")
        return self._config

    def check_exists(self):
        """ Check that recod in database exists and there is only 1 record.
        """
        object_data = self._load()
        return object_data

        return False

    def __del__(self):
        self._connection = None
        self._table = None
        self._connection = None

def get_db_object(con, table, obj_id, ExecType = Exception):
    req = _REQS['get_db_object'].format(table = table)
    row = con.uexec_get_list(req, {"ID": obj_id})
    if len(row) != 1:
        raise ExecType("Not 1({0}) action {table} row with id = {1}".format(len(row), obj_id, table = table))
    row = row[0]
    return row

def o_select(con, table, *args, **kargs):
    """ :param con: database connection
        :param table: table for request
        :param *args: list of table keys
        :param **kargs: list of conditions
        :rtype: list of DbObject
    """
    result = db_select(con, table, *args, **kargs)
    result = map(lambda x: DbObject(config = x, table = table), result)
    return result


