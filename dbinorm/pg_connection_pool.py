#coding: utf-8

import time
import logging
import base64
import psycopg2
import psycopg2.extensions
import psycopg2.sql as sql
from psycopg2.extensions import connection as _connection
from rbs_db.pg_factory import RealDictConnectionUpper
from rbs_db.db_exception import DatabaseConnectionException

log = logging.getLogger()

def _get_connection(connection_or_string):
    if connection_or_string is None:
        raise DatabaseConnectionException("ErrorDatabaseConnection connection string is None")
    if isinstance(connection_or_string, dict):
        con = psycopg2.connect(connection_factory = RealDictConnectionUpper, **connection_or_string)
    else:
        con = psycopg2.connect(connection_or_string, connection_factory = RealDictConnectionUpper)
    return con

def create_connection_string(db_user,
                             base64_pass = None,
                             db_host = None,
                             db_port = None,
                             db_name = None):
    if isinstance(db_user, dict):
        conf = db_user
        db_user = conf.get("db_user")
        base64_pass = conf["db_pass"]
        db_host = conf["db_host"]
        db_port = conf["db_port"]
        db_name = conf["db_name"]
    db_pass = base64.decodestring(base64_pass)
    connection_string = "dbname=%s user=%s password=%s host=%s port=%s" % (db_name,db_user,db_pass,db_host,db_port)
    return connection_string


class PgConnectionPool(object):

    def __init__(self, globs, config, no_pool = True,
                       con_conf = None):
        self.app_globals = globs
        self.config = config
        self.con_conf = con_conf
        self.pool = None
        self.no_pool = no_pool
        self.global_list_of_connections = {}
        self.connection_map = {}
        self.hung_previous_remove = time.time()
        self.max_connection_wait_time = 60 * 60
        self.hung_remove_interval = 60 * 15
        self.max_connection_group_number = 0xFFFFFF
        self.connection_group_number = 1
        self.con_id = 0
        self.no_session = self.config.get('no_session', False)

    def __del__(self):
        self.remove_all_connections()

    def acquire(self, id_of_pool_connection = 0):
        try:
            return self._try_get_connection(id_of_pool_connection)
        except Exception as e:
            log.critical("Exception occured")
            log.exception(e)

    def _try_get_connection(self, id_of_pool_connection,
                            start = None):
        connection = self._get_connection()
        self._set_user(connection)
        self.global_save_connection(id_of_pool_connection,
                                    connection)
        return connection

    def _get_connection(self):
        log.info("config = %s", self.con_conf)
        conf = self.con_conf or self.config
        con = _get_connection(conf)
        cur = con.cursor()
        #TODO: add reset params
        cur.close()
        return con

    def _init_params(self, connection):
        pass
        
    def _get_user(self):
        return "TEST_USER"

    def release(self, connection):
        try:
            connection.close()
        except Exception as e:
            log.critical("error when close connection")
            log.exception(e)

    def global_save_connection(self, connection_group, connection):
        return

    def remove_connection_group(self, connection_group):
        return

    def remove_all_connections(self):
        return

    def remove_hung_connections(self):
        return

    def get_next_group_number(self):
        return 0

    def is_empty_group(self, group):
        return True

    def log_connections_info(self):
        return

    def log_connection_info(self):
        return

    def log_connection_info_cfg(self, con, limit = None):
        return

    def remove_con(self, con_id, grp = None):
        return
