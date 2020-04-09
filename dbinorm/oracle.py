#coding: utf-8

import six
import re
import cx_Oracle
import traceback
import base64
import datetime
from string import Formatter
from dbinorm.connection import DbiConnection, DbiException
from dbinorm.tools import evented

DB_PARAM = re.compile("[_a-zA-Z0-9]+")
DB_NAME = re.compile("[_a-zA-Z0-9\\.]+")
formatter = Formatter()
DatabaseError = cx_Oracle.DatabaseError
WARNING_TRESHOLD = 10

T_CLOB = cx_Oracle.CLOB
HALF_VFIELD = 2045

is_str = lambda x: isinstance(x, six.string_types)
strptime = datetime.datetime.strptime

import logging
log = logging.getLogger()

fmt = "%d.%m.%Y %H:%M:%S"
#TODO: add addition formats
def datetime_from_string(datetime_string):
    return strptime(datetime_string, fmt)

def prepare_strings(cursor, args):
    """ Convert strings with length > 2000 to CLOB vars.
        :param cur: cursor to datbase.
        :param args: dictionary with arguments for request.
        For work with unicode NEED setted NLS_LANG.
    """
    n = []
    for k, v in args.items():
        if is_clob(v):
            args[k] = to_clob(v); n.append(k)
    if len(n) > 0:
        cursor.setinputsizes(**dict({(i, T_CLOB) for i in n}))
    return args

is_clob = lambda(v): is_str(v) and len(v) > HALF_VFIELD
to_clob = lambda(cur, v): cur.var(cx_Oracle.CLOB).setvalue(0, v)

def cur_exec(cur, req, params):
    """ Execute request with cursor.
        http://www.dba-oracle.com/sf_ora_01722_invalid_number.htm
        problem with invalid number due to implicit type casting

        :param cur: cursor
        :param request: string with request
        :param params: dictionary with params
    """
    temp_binds = None
    try:
        cur.prepare(req)
        temp_binds = dict((i, params[i]) for i in cur.bindnames())
        prepare_strings(cur, temp_binds)
        cur.execute(req, **temp_binds)
    except Exception as err:
        log.warn('error req = %s', req)
        log.warn('params = %s', params)
        log.warn('temp_binds = %s', temp_binds)
        raise err


def cur_exec_with_blob(cur, req, params, blob_fields, need_raise = True):
    """ Execute request with requests to blob or clob fields.
        :param cur: cursor
        :param req: string with request
        :param params: dictionary with params
        :param blob_fields: list or string with blob fields
        :need_raise: if True exception on error will be raised
    """
    temp_binds = None
    params = params.copy()
    # Code, to set fields, can be placed in _set_variables
    # And use only one function exec_req.
    # Problem to define what need to use BLOB or CLOB that is incompatible
    if isinstance(blob_fields, (str, unicode)):
        blob_fields = [blob_fields]
    try:
        cur.prepare(req)
        _set_blobs(cur, params, blob_fields)
        #if var missing in params, exception will be raised,
        #exception will be not oracle but python
        #only for version >= 2.7
        #temp_binds = {i: params[i] for i in cur.bindnames()}
        temp_binds = dict((i,params[i]) for i in cur.bindnames())
        cur.execute(req, **temp_binds)
        return True
    except Exception as err:
        if need_raise == True:
            log.warn('error req = %s', req)
            c_temp_binds = temp_binds.copy()
            for i in blob_fields:
                del c_temp_binds[i]
            log.warn('temp_binds = %s', c_temp_binds)
            raise err
        error_string = traceback.format_exc(err)
        c_temp_binds = temp_binds.copy()
        for i in blob_fields:
            del c_temp_binds[i]
        log.error("request  = %s, params = %s", req, c_temp_binds)
        log.error(error_string)
    return False

def cur_exec_get_list(cur, req, params):
    """ Execute request and get results with dict_order in each record.
        TODO: dict_order remove.
        :param cur: cursor to database
        :param request: request to database
        :param params: parameters to execute request
    """
    cur_exec(cur, req, params)
    description = map(lambda x: x[0], cur.description[:])
    return [dict(zip(description, i)) for i in cur]

def exec_get_list(cur, req, params = {}):
    """ Execute request and get result.
        :param cur: cursor to database
        :param request: request to database
        :param params: parameters to execute request
    """
    cur_exec(cur, req, params)
    description = map(lambda x: x[0], cur.description[:])
    return [dict(zip(description, i)) for i in cur]

def exec_get_list_ex(cur, req, params = {}, extra_params = {}):
    """ Execute request with update columns names.
        :param cur: cursor to database
        :param request: request to database
        :param params: parameters to execute request
        :param extra_params: dictionary with addition params
        :param extra_params.description_override: override description config
        :param extra_params.row_type: result type
        :param extra_params.encoding: encoding from that results should be decoded
    """
    cur_exec(cur, req, params)
    description = map(lambda x: x[0], cur.description[:])

    if extra_params.has_key("description_override") and \
      isinstance(extra_params["description_override"], dict):
      description_override = extra_params["description_override"]
      description = _update_description(description, description_override)

    row_type = extra_params.get("row_type", "dict")
    if extra_params.get("encoding") is not None:
        enc = extra_params.get("encoding")
        quiet = extra_params.get("decode_quiet")
        if row_type == "dict":
            return [dict(zip(description, [_dec(j, enc, quiet) for j in i])) for i in cur]
        elif row_type == "list":
            return [ [_dec(j, enc, quiet) for j in i]  for i in cur ]
        else:
            raise Exception("Unsupported row type %s for exec_get_list_ex" % (row_type))
    if row_type == "dict":
        return [dict(zip(description, i)) for i in cur]
    elif row_type == "list":
        return [ [j for j in i]  for i in cur ]
    else:
        raise Exception("Unsupported row type %s for exec_get_list_ex" % (row_type))

def exec_get_list_with_lob(cur, req, params = {}, extra_params = {}):
    """ Exec request with access to LOB fields.

        :param cur: cursor to database
        :param request: request to database
        :param params: parameters to execute request
        :param extra_params: dictionary with addition params
        :param extra_params.description_override: override description config
        :param extra_params.encoding: encoding from that results should be decoded
    """
    cur_exec(cur, req, params)
    description = map(lambda x: x[0], cur.description[:])
    if extra_params.has_key("description_override") and \
      isinstance(extra_params["description_override"], dict):
      description_override = extra_params["description_override"]
      description = _update_description(description, description_override)
    if extra_params.get("encoding") is not None:
        enc = extra_params.get("encoding")
        get_val = get_val_or_lob_value_enc
    else:
        enc = None
        get_val = get_val_or_lob_value
    quiet = extra_params.get("decode_quiet")
    #FIX, test version
    result = [dict(zip(description, [get_val(j, enc, quiet) for j in i])) for i in cur]
    return result

def _update_description(description, description_override):
    new_description = [None] * len(description)
    for n, i in enumerate(description):
        if description_override.has_key(i):
            new_description[n] = description_override[i]
        else:
            new_description[n] = i
    return new_description

def get_val_or_lob_value(v, enc = None, quiet = False):
    if isinstance(v, (cx_Oracle.LOB, cx_Oracle.CLOB)):
        val = v.read()
        return val
    return v

def _dec(val, enc, quiet = False):
    if isinstance(val, str):
        try:
            return val.decode(enc)
        except Exception:
            log.exception(val)
            if quiet == True:
                return val.decode(enc, 'replace')
            else:
                raise
    return val

def get_val_or_lob_value_enc(v, enc, quiet = False):
    if isinstance(v, (cx_Oracle.LOB, cx_Oracle.CLOB)):
        val = v.read()
        return val
        # TODO: ??? Does need decoder clob, blob content ???
        # where it is used ?
        # try:
        #     val_dec = _dec(val, enc, False)
        #     return val_dec
        # except Exception:
        #     return val
    return _dec(v, enc, quiet)

def _set_blobs(cur, params, blob_fields):
    """ Set LOB values for parameters.
        :param cur: cursor to database.
        :param params: dictionary with parameters.
        :param blob_fields: config for blob_fields.
    """
    if isinstance(blob_fields, (tuple, list)):
        for bf in blob_fields:
            field_type = None
            if isinstance(bf, dict):
                f = bf.get("field_name")
                field_type = bf.get("field_type", None)
            else:
                f = bf
            val = params.get(f)
            if val == None:
                continue
            if field_type in ["blob", "clob"]:
                field_type = field_type.upper()
            if field_type == None:
                if isinstance(val, (str, unicode)):
                    field_type = "CLOB"
                else:
                    field_type = "BLOB"
            if field_type == "CLOB":
                blobvalue = cur.var(cx_Oracle.CLOB, len(val))
            else:
                blobvalue = cur.var(cx_Oracle.BLOB, len(val))
            blobvalue.setvalue(0, val)
            params[f] = blobvalue
    elif isinstance(blob_fields, dict):
        for f, field_type in blob_fields.items():
            val = params.get(f)
            if val == None:
                continue
            if field_type == 'clob':
                blobvalue = cur.var(cx_Oracle.CLOB, len(val))
            elif field_type == 'blob':
                blobvalue = cur.var(cx_Oracle.BLOB, len(val))
            else:
                raise Exception("Unsupported blob type %s.", field_type)
            blobvalue.setvalue(0, val)
            params[f] = blobvalue

class OraTable(object):

    def __init__(self, con, table_name = None):
        self.table_name = table_name
        self.columns_dict = {}
        self.columns = []
        req = "select * from %s where 1 = 0" % (self.table_name)
        cur = con.cursor()
        cur.execute(req)
        #(name, type, display_size, internal_size, precision, scale, null_ok)
        self.columns = cur.description[:]
        for i in self.columns:
            self.columns_dict[i[0]] = i
        cur.close()
        cur = None

        self.blob_fields = None

    def get_name(self):
        return self.table_name

    def get_field_type(self, field_name):
        desc = self.columns_dict.get(field_name)
        if desc == None:
            return False
        ftype = desc[1]
        if ftype == cx_Oracle.NUMBER:
            return "number"
        elif ftype == cx_Oracle.STRING:
            return "string"
        elif ftype == cx_Oracle.DATETIME:
            return "datetime"
        elif ftype == cx_Oracle.CLOB:
            return "clob"
        elif ftype == cx_Oracle.BLOB:
            return "blob"
        else:
            #other types
            return None

    def is_field_nullable(self, field_name):
        desc = self.columns_dict.get(field_name)
        if desc == None:
            return None
        return desc[6] != 0

    def get_field_names(self):
        """ Return field names.
        """
        return [i[0] for i in self.columns]

    def prepare_data(self, data):
        """ Update data to convert them to types of database.
        """
        for i, v in data.items():
            field_type = self.get_field_type(i)
            #log.info('i = %s, type = %s', i, field_type)
            if field_type == 'datetime' and isinstance(v, (str, unicode)):
                data[i] = datetime_from_string(v)
        return data

    def get_lob_fields(self):
        if self.blob_fields == None:
            blob_fields = {}
            for i in self.get_field_names():
                field_type = self.get_field_type(i)
                if field_type in ['blob', 'clob']:
                    blob_fields[i] = field_type
            self.blob_fields = blob_fields
        return self.blob_fields

    def get(self, field_name):
        field = self.columns_dict.get(field_name)
        return field

def create_connection_string(db_user, base64_pass = None, db_host = None, db_port = None, db_sid = None):
    db_pass = base64.decodestring(base64_pass)
    connection_string = db_user + "/" + db_pass + "@" + db_host + ":" + db_port + "/" + db_sid
    return connection_string

class OraConnection(DbiConnection):
    """ Class wrap standard sql operations, make them simpler.
        This class used to work with Oracle databse for for with
        other methods should be overwritten.
    """

    table_class = OraTable
    db_type = "oracle"

    @staticmethod
    def nbind(val):
        return u":" + val

    @staticmethod
    def unbind(val):
        if isinstance(val, (str, unicode)) and val[0] == ":":
            candidat = val[1:]
            if DB_PARAM.match(candidat):
                return candidat
        return None

    @staticmethod
    def check_name(name):
        if DB_NAME.match(name):
            return name
        else:
            raise DbiException("Invalid name %s" % name)

    @staticmethod
    def normalize_blob_fields(blob_fields, params):
        """ Normalization of blob_fields description.
            After remove other ussage, should be removed.
        """
        new_blob_fields = {}
        if isinstance(blob_fields, (str, unicode)):
            blob_fields = [blob_fields]
        if isinstance(blob_fields, (list, tuple)):
            for bf in blob_fields:
                field_name = None
                field_type = None
                if isinstance(bf, dict):
                    field_name = bf.get("field_name")
                    field_type = bf.get("field_type")
                elif isinstance(bf, (str, unicode)):
                    field_name = bf
                else:
                    raise Exception("Unsupported blob field config type %s." % type(bf))
                if field_type == None:
                    if isinstance(params.get(field_name), (str, unicode)):
                        field_type = "clob"
                    else:
                        field_type = "blob"
                elif isinstance(field_type, (str, unicode)):
                    if field_type in ["BLOB", "CLOB", "blob", "clob"]:
                        field_type = field_type.lower()
                    else:
                        raise Exception("Unsuported lob type %s." % field_type)
                else:
                    raise Exception("Unsuported field_type %s." % type(field_type))
                new_blob_fields[field_name] = field_type
                return new_blob_fields
        elif isinstance(blob_fields, dict):
            return blob_fields
        else:
            raise Exception("Unsuported blob_fields types %s." % type(blob_fields))

    @classmethod
    def create_in(cls, elems, prefix = "IN_ELEM"):
        return create_in(elems, prefix, con = cls)

    @classmethod
    def error_code_and_message(cls, error):
        err, = error.args
        code = err.code
        message = err.message
        return (code, message)

    @classmethod
    def get_field_type(cls, dsc):
        ftype = dsc[1]
        if ftype == cx_Oracle.NUMBER:
            return "number"
        elif ftype == cx_Oracle.STRING:
            return "string"
        elif ftype == cx_Oracle.DATETIME:
            return "datetime"
        elif ftype == cx_Oracle.CLOB:
            return "clob"
        elif ftype == cx_Oracle.BLOB:
            return "blob"
        else:
            #other types
            return None

    @classmethod
    def sequence_name(cls, table_name):
        return table_name.upper() + "_S"

    @classmethod
    def format_binds(cls, request):
        parsed = [fn for _, fn, _, _ in formatter.parse(request) if fn is not None]
        bind = cls.nbind
        if len(parsed) > 0:
            try:
                request = request.format(**dict([(v, bind(v)) for v in parsed]))
            except TypeError as e:
                log.exception(e)
                log.info("%s", request)
                raise e
        return request

    DatabaseError = DatabaseError
    events = None
    escape = lambda x: u"q'[" + (str(x) if not isinstance(x, (str, unicode)) else x) + u"]'"

    def __init__(self, constr = None):
        """ Wrap standard sql operations, make them simpler.
            :param connection_or_string:
        """
        self.con = None
        self._outer_con = False
        self._name = None
        self._password = None
        self._host = None
        self._port = None
        self._sid = None
        self._con_type = None
        self.events = {}
        self.table_map = None
        self.prev_description = None

        if isinstance(constr, DbiConnection):
            self.con = constr.get_raw_con()
            self._con_type = "outer"

        elif isinstance(constr, (str, unicode)):
            self.con = cx_Oracle.Connection(constr)
            self._con_type = "string"

        elif isinstance(constr, dict):
            constr = create_connection_string(constr)
            self.con = cx_Oracle.Connection(constr)
            self._con_type = "dict"

        elif isinstance(constr, cx_Oracle.Connection):
            self.con = constr
            self._con_type = "raw" # не возможности получить pass
            #can't get password
            #and perform some other operations!
            #does need support ?
        else:
            raise DbiException('Unsupported types of DatabaseConnection parameters')
        self._init_con_parts()

    def _init_con_parts(self):
        raise Exception("Not implemented.")

    def get_table(self, table_name):
        """ Get Table object.
            NOTE: tamble_map can be class object,
                  but it will be stored in memory all
                  time of application working.
                  But it may be more efficient if it
                  frequently used.
            :param table_name: name of table object for that should be returned.
        """
        table_name = table_name.upper()
        if self.table_map == None:
            self.table_map = {}
        table = self.table_map.get(table_name)
        if table != None:
            return table
        table = self.table_class(self, table_name)
        self.table_map[table_name] = table
        return table

    def get_db_type(self):
        return self.db_type

    def add_event(self, event_name, handler, params):
        events = self.events.setdefault(event_name, [])
        for i in events:
            if i.get('handler') == handler:
                return None
        event_config = {'handler': handler, 'params': params}
        events.append(event_config)
        return event_config

    def remove_event(self, event_name, handler):
        events = self.events.get(event_name)
        if events == None or len(events) == 0:
            return
        new_events = []
        for i in events:
            if i.get(handler) != handler:
                new_events.append(i)
            else:
                continue
        self.events[event_name] = new_events
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exec_val, exec_tb):
        if self._con_type not in ['outer', 'raw']:
            self.destroy_connection()
        return False

    def __del__(self):
        self.destroy_connection()

    def _get_password(self):
        """ Passwords in cx_Oracle 5.2.1 was removed from connection.
            So, it should be saved manual.
        """
        if self._password != None:
            return self._password
        raise DbiException("Can't get password")

    def get_user_name(self):
        return self._name or (
            self.con.username if self.con != None else None)

    def _get_host(self):
        if self._host != None:
            return self._host
        raise DbiException("Can't get host")

    def _get_port(self):
        if self._port != None:
            return self._port
        raise DbiException("Can't get port")

    def _get_sid(self):
        if self._sid != None:
            return self._sid
        raise DbiException("Can't get sid")

    def cursor(self):
        return self.con.cursor()

    @evented
    def commit(self):
        return self.con.commit()

    @evented
    def rollback(self):
        return self.con.rollback()

    def create_connection(self):
        if self.con != None:
            return self.con
        return None

    @evented
    def destroy_connection(self):
        if self.con != None and not self._con_type in ["raw", "outer"]:
            self.con.close()
            self.con = None

    def exec_req(self, request, params = {}, need_commit = False, extra_params = None):
        """ Execute request  """
        result = False
        con = self.create_connection()
        cur = con.cursor()

        blob_fields = None
        if isinstance(extra_params, dict) and extra_params.get("blob_fields") != None:
            blob_fields = extra_params.get("blob_fields")
            """ **blob_fields**
                It may be:
                  - str, unicode with name of field.
                  - list of str, unicode with name of field.
                  - list of dicts: field_name, field_type
                  - dict: {field_name: field_type, ...}
                It is more complex that need:
                  should keey only one, with help of normalize function.
            """
            blob_fields = self.normalize_blob_fields(blob_fields, params)
        #
        if len(params) > 0:
            self._set_variables(cur, params, blob_fields)
        if extra_params == None:
            result = cur_exec(cur, request, params, need_raise = True, encoding = self.get_encoding())
        elif blob_fields != None:
            result = cur_exec_with_blob(cur, request, params, blob_fields, need_raise = True)
        cur.close()
        if need_commit == True:
            con.commit()
        return result

    def _set_variables(self, cur, params, blob_fields):
        if blob_fields == None:
            blob_fields = {}

        for k, v in params.iteritems():
            if k in blob_fields:
                continue
            if v == cx_Oracle.NUMBER:
                var = cur.var(v)
                params[k] = var
            elif isinstance(v, (str, unicode)) and len(v) > 2000 and \
                not blob_fields.has_key(k):
                var = cur.var(cx_Oracle.CLOB, len(v))
                var.setvalue(0, v)
                params[k] = var

    def create_variables(self, cur, params):
        """ Create variables for cursor """
        variables = {  }
        for name in params:
            param_type = params.get(name)
            if (param_type == "NUMBER"):
                var = cur.var(cx_Oracle.NUMBER)
                variables[name] = var
            else :
                variables[name] = params.get(name)
        return variables

    def get_variables_value(self, variables):
        """ Get variables values """
        for var in variables:
            oracle_var = variables[var]
            if type(oracle_var) == cx_Oracle.NUMBER:
                variables[var] = int(oracle_var.getvalue())
        return variables

    def exec_request_with_returns(self, request, params = {}, commit_needed = False, extra_params = None):
        """ Execute request and return result (returning values) """
        con = self.create_connection()
        cur = con.cursor()
        blob_fields = None
        if isinstance(extra_params, dict) and extra_params.get("blob_fields") != None:
            blob_fields = extra_params.get("blob_fields")
        if len(params) > 0:
            self._set_variables(cur, params, blob_fields)
        variables = (self.create_variables(cur, params) if len(params) > 0 else {})
        if extra_params == None:
            result = cur_exec(cur, request, variables, need_raise = True, encoding = self.get_encoding())
        elif blob_fields != None:
            result = cur_exec_with_blob(cur, request, variables, blob_fields, need_raise = True)
        result = self.get_variables_value(variables)
        cur.close()
        if commit_needed == True:
            con.commit()
        return result

    def uexec_get_list(self, request, params = {}, extra_params = None):
        if extra_params == None:
            extra_params = {}
        if not extra_params.has_key('encoding'):
            self.create_connection()
            extra_params['encoding'] = self.get_encoding()
        return self.exec_get_list(request, params, extra_params)

    def exec_get_list(self, request, params={}, extra_params = None):
        """
        """
        con = self.create_connection()
        cur = con.cursor()
        if extra_params == None:
            result_list = exec_get_list(cur, request, params)
        elif (isinstance(extra_params, dict)):
            if extra_params.get("with_lob_values") == True:
                result_list = exec_get_list_with_lob(cur, request, params, extra_params)
            elif len(extra_params) > 0:
                result_list = exec_get_list_ex(cur, request, params, extra_params)
        self.prev_description = cur.description[:]
        cur.close()
        return result_list

    def exec_get_cur(self, request, params={}, extra_params = None):
        """ Execute request and return cursor.
        """
        con = self.create_connection()
        cur = con.cursor()
        cur_exec(cur, request, params)
        return cur

    def get_prev_desc(self):
        """ Return description from previous request.
            Returned normalilized description.
        """
        desc = []
        if self.prev_description:
            for i in self.prev_description:
                name = i[0]
                ftype = self.get_field_type(i)
                desc.append([name, ftype])
        return desc

    def get_prev_field_names(self):
        """ Return field names from previous request.
        """
        if self.prev_description != None:
            return [i[0] for i in self.prev_description]
        else:
            return None

    def _get_next_val(self, sequence_name):
        r = self.exec_get_list("select {SEQUENCE}.nextval value from dual".format(**{"SEQUENCE": sequence_name}))
        return r[0].get("VALUE")

    def get_next_val(self, sequence_name, length = 1, tuple_result = False):
        """  Return list of ids that length = length for special sequence
        """
        if length == 1:
            return self._get_next_val(sequence_name)

        cur = self.cursor()
        request = u"""
begin
  for i in 1 .. {LENGTH} loop
       :retval := {SEQUENCE}.nextval();
  end loop;

  -- next variant perform commit because has DDL operation
  -- -- First, alter the object so the next increment will jump {LENGTH} instead of just 1.
  -- execute immediate 'alter sequence {SEQUENCE} increment by {LENGTH}';
  -- -- Run a select to actually increment it by {LENGTH}
  -- select {SEQUENCE}.nextval into :retval from dual;
  -- -- Alter the object back to incrementing only 1 at a time
  -- execute immediate 'alter sequence {SEQUENCE} increment by 1';
end;
"""
        request = request.format(**{"SEQUENCE": sequence_name, "LENGTH": length})
        retval = cur.var(cx_Oracle.NUMBER)
        cur.execute(request, retval=retval)
        retval = int(retval.getvalue())
        cur.close()
        #return list of new ids or pair of values that correspond interval
        if tuple_result == True:
            return (retval - length + 1, retval)
        return range(retval - length + 1, retval + 1)

    def get_encoding(self):
        if not self.con: return None
        return self.con.encoding

    def get_connection_string(self):
        if not self.con: return None
        c = self.con
        # return create_connection_string(c.username, base64.encodestring(self._get_password()), self._get_host(), self._get_port(), self._get_sid())

    def call_proc(self, name, args):
        con = self.create_connection()
        cur = con.cursor()
        cur.callproc(name, args)
        cur.close()

    def handle_events(self, event_name):
        events = self.events.get(event_name)
        before = []
        after = []
        new_events = []
        r = False
        for i in events:
            params = i.get('params')
            if params.get('attitude') == 'before':
                before.append(i)
            else:
                after.append(i)
            if params.get('single') == True:
                r = True
            else:
                new_events.append(i)
        if r == True:
            self.events[event_name] = new_events

        return before, after

        if len(before) > 0:
            for i in before:
                handler = i.get('handler')
                handler()

        result = self.con.commit()

        if len(after) > 0:
            for i in after:
                handler = i.get('handler')
                handler()
