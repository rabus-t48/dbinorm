#coding: utf-8

import pymssql
import logging

import re, base64
from dbinorm.db_exception import DbException

re_conn_string = re.compile(r'(?P<user>[^/@]+)/(?P<password>[^/@]+)@(?P<host>[^/@]+)/(?P<database>[^/@]+)')

log = logging.getLogger(__name__)

def cur_exec(cur, req, params= {}, need_raise = False):
    try:
        cur.execute(req, params)
        return True
    except Exception as e:
        if need_raise == True:
            raise
        log.error("request  = %s, params = %s", req, params)
        try:
            log.exception(e)
        except TypeError:
            log.critical(e, exc_info = 1)
    return False

def exec_get_list(cur, req, params):
    """
    " Execute request, return result as list of dicts.
    """
    cur_exec(cur, req, params)
    description = map(lambda x: x[0], cur.description[:])
    return [dict(zip(description, i)) for i in cur]

class MsSqlConnection(object):

    """ Connect to mssql database.
    """

    def __init__(self, connection_config):
        self.__connection = None
        self._outer = False
        self._destroyed = False
        if isinstance(connection_config, (str, unicode)):
            self._connection_config = self._parse_connection_string(connection_config)
        elif isinstance(connection_config, (dict, MsSqlConnection, pymssql.Connection)):
            self._connection_config = connection_config
        else:
            raise DbException('Invalid type of connection %s' % (type(connection_config)))

    @property
    def connection(self):
        if self.__connection != None:
            return self.__connection

        if self._destroyed == True:
            return None

        value = self._connection_config
        if isinstance(value, dict):
            self.__connection = pymssql.connect(**value)
        elif isinstance(value, pymssql.Connection):
            self.__connection = value
            self._outer = True
        elif isinstance(value, MsSqlConnection):
            self._outer = True
            self.__connection = value.connection # ?
        else:
            raise Exception('Invalid type of connection %s' % (type(value)))
        return self.__connection

    def _parse_connection_string(self, value):
        'user/pass@host/database'
        res = re_conn_string.match(value)
        if not res:
            raise DbException('Incvalid string format should be: user/password@host/database')
        config = {
            'user': res.group('user'),
            'password': base64.decodestring(res.group('password')),
            'host': res.group('host'),
            'database': res.group('database')
        }
        return config

    def __getattr__(self, name):
        """ Не самый лучший способ
        """
        if self.__connection != None:
            return getattr(self.__connection, name)
        else:
            return getattr(object, name)

    def exec_get_list(self, request, params = {}, extra_params = None):
        con = self.connection
        cur = con.cursor()
        result_list = exec_get_list(cur, request, params)
        cur.close()
        return result_list

    def exec_req(self, request, params = {}, need_commit = False, extra_params = None):
        con = self.connection
        cur = con.cursor()
        result_list = exec_get_list(cur, request, params)
        cur.close()
        return result_list

    def destroy_connection(self):
        if self.__connection != None and self._outer == False:
            self.__connection.close()
            self._destroyed = True

    def create_connection(self):
        if self._destroyed == True and self._outer == False:
            self._destroyed = False
            return self.connection
        else:
            return self.connection

    def _is_created(self):
        """ For test only purpose.
        """
        return self.__connection != None

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exec_val, exc_tb):
        self.destroy_connection()
        return False
