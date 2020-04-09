#coding: utf-8

import sqlite3
import logging
import time
import traceback
log = logging.getLogger()

WARNING_TRESHOLD = 10

def get_con(db_path):
    con = sqlite3.connect(db_path,
                          detect_types=sqlite3.PARSE_DECLTYPES |
                          sqlite3.PARSE_COLNAMES)
    return con

def cur_exec(cur, req, params, need_raise = False, encoding = None):
    """ Execute request.
    """
    temp_binds = params or {}
    try:
        cur.prepare(req)
        start = time.time()
        cur.execute(req, **temp_binds)
        finish = time.time()
        if finish - start > WARNING_TRESHOLD:
            log.warn("LONG REQUEST: %s \n params %s", req, params)
        return True
    except Exception as err:
        if need_raise == True:
            log.warn('error req = %s', req)
            log.warn('params = %s', params)
            log.warn('temp_binds = %s', temp_binds)
            raise err

        error_string = traceback.format_exc(err)
        log.error("request  = %s, params = %s", req, temp_binds)
        log.error(error_string)
    return False

def exec_get_list(cur, req, params = {}):
    """ Execute request and return result in
        list of dicts.
    """
    cur_exec(cur, req, params)
    description = map(lambda x: x[0], cur.description[:])
    return [dict(zip(description, i)) for i in cur]

class SqliteConnection(object):

    def __init__(self, constr = None):
        self._outer = False
        if isinstance(constr, (str, unicode)):
            self.con = get_con(constr)
            self._con_type = "string"
        elif isinstance(constr, sqlite3.Connection):
            self.con = constr
            self._con_type = "raw"
        elif isinstance(constr, SqliteConnection):
            self.con = SqliteConnection.con
            self._con_type = "outer"

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exec_val, exec_tb):
        if self._con_type not in ['outer', 'raw']:
            self.destroy_connection()
        return False

    def commit(self):
        self.con.commit()

    def destroy_connection(self):
        if self.con != None and not self._con_type in ["raw", "outer"]:
            self.con.close()
            self.con = None

    def exec_req(self, request, params = {}):
        con = self.con
        cursor = con.cursor()
        cursor.execute(request, params)
        cursor.close()

    def exec_get_list(self, request, params = {}):
        cur = self.con.cursor()
        result = exec_get_list(cur, request, params)
        cur.close()
        return result
