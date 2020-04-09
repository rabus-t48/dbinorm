#coding: utf-8

import time
import logging
import cx_Oracle
import base64
import threading
import traceback

log = logging.getLogger()

class ConnectionPool(object):
    def __init__(self, globs, config, no_pool=True,
                con_conf = None):
        '''
        no_pool - not use session pool always new session
        '''
        self.app_globals = globs
        self.config = config
        self.con_conf = con_conf
        self.pool = None
        self.no_pool = no_pool
        self.global_list_of_connections = {}
        self.connection_map = {}
        self.hung_previous_remove = time.time()
        # self.max_connection_wait_time = 60  # 1 HOUR # test version
        # self.hung_remove_interval = 60  # 15 minutes # test version
        self.max_connection_wait_time = 60 * 60 # 1 HOUR
        self.hung_remove_interval = 60 * 15 # 15 minutes
        self.max_connection_group_number = 0xFFFFFF
        self.connection_group_number = 1
        self.conn_id = 0
        self.no_session = self.config.get("no_session", False)

    def __del__(self):
        self.remove_all_connections()

    def acquire(self, id_of_pool_connection = 0):
        """ Give new connection to reqestor.
        """
        start = time.time()
        if start - self.hung_previous_remove > self.hung_remove_interval:
            try:
                self.remove_hung_connections()
            except Exception as e:
                log.exception(e)

        try:
            return self._try_get_connection(id_of_pool_connection, start)
        except Exception as e:
            log.critical("Exception occured")
            log.exception(e)
            # print e, dir(e)
            # print "args = ", e.args
            # print "msg = ", e.message
            # print dir(e.message)

            if type(e) is cx_Oracle.DatabaseError and (str(e.message.code) == "24418" or #ORA-24418: Cannot open further sessions"
                str(e.message.code) == "12519"):  #ORA-12519: TNS:no appropriate service handler found
                raise Exception("The server is overloaded, please try again in action after a while")
            else:
                self._del_pool_if_damaged()
                return self._try_get_connection(id_of_pool_connection, start)


    def _try_get_connection(self, id_of_pool_connection, start = None):
        if not start : start = time.time()
        connection = self._get_connection()
        connection.ping()
        self._init_params(connection)
        self.global_save_connection(id_of_pool_connection, connection)
        # finish = time.time()
        # log.info('acquire %2.4f (total) ' % (finish - start))
        return connection

    def _get_connection(self):
        con = None
        if self.no_pool:
            con = cx_Oracle.Connection(threaded=True, **self._get_con_params() )
        else:
            if not self.pool: self._create_pool()
            con = self.pool.acquire()
        #clear params
        cur = con.cursor()
        cur.callproc('dbms_session.reset_package', tuple())
        cur.close()
        return con

    def _get_con_params(self):
        config = self.con_conf or self.config
        return {
            "user"     : config['user'],
            "password" : base64.b64decode(config['pass']),
            "dsn"      : cx_Oracle.makedsn(config['host'],
                                           config['port'],
                                           config['sid'])
        }


    def _create_pool(self):
        print "POOL CREATION"
        try:
            #config = self.config
            self.pool = cx_Oracle.SessionPool(
                min=1,
                max=50,
                increment=1,
                threaded=True,
                **self._get_con_params()
            )
        except Exception as e:
            log.exception(e)
            self.pool = None
            return None


    def _del_pool_if_damaged(self):
        """ Remove pool if its damaged.
        """
        if self.pool:
            try:
                connection = self.pool.acquire()
                self.pool.release(connection)
            except Exception as e:
                log.exception(e)
                self.remove_all_connections()
                del self.pool
                self.pool = None
                time.sleep(3) # sleep to wait while some connections will be closed

    def _init_params(self, connection):
        pass
        
    def release(self, connection):
        cfg = self.connection_map.get(connection)
        if self.no_pool:
            try:
                connection.close()
                log.info("close connection id: %s", cfg.get("id"))
            except Exception as e:
                log.critical("error when close connection")
                log.exception(e)
            finally:
                self._remove_cfg_from_grp(cfg)
        else:
            if self.pool:
                def release_con(conmgr, connection, cfg):
                    """ Close hung connection
                    """
                    try:
                        connection.cancel()
                        log.debug("connection concel action in thread")
                    except Exception as e:
                        log.exception(e)

                t = threading.Timer(5.0, release_con, [self, connection, cfg])
                t.start()
                #log.info("TRY release connection")
                try:
                    self.pool.release(connection)
                except Exception as e:
                    traceback.print_stack()
                    log.exception(e)
                    try:
                        connection.close()
                    except Exception as e:
                        log.info('close error % s', e.message)
                t.cancel()
                self._remove_cfg_from_grp(cfg)
                if cfg != None:
                    log.info("close connection with id = %s", cfg.get("id"))

    def _remove_cfg_from_grp(self, config):
        if not config:
            return
        grp = config["grp"]
        find = None
        for num, cfg in enumerate(self.global_list_of_connections[grp]):
            if cfg == config:
                find = num
                break
        if find != None:
            self.global_list_of_connections[grp].pop(find)
            if len(self.global_list_of_connections[grp]) == 0:
                del self.global_list_of_connections[grp]

        con = config.get("connection")
        if self.connection_map.has_key(con):
            del self.connection_map[con]


    def global_save_connection(self, connection_group, connection):
        """ Save connection """
        self.conn_id += 1
        log.info("get connection:  id: %s grp: %s", self.conn_id, connection_group)
        con_cfg = {
            "connection": connection,
            "time": time.time(),
            "grp": connection_group,
            "id": str(self.conn_id),
            "path": traceback.format_stack(limit = 15),
            "user": self._get_user()
        }
        self.connection_map[connection] = con_cfg
        if self.global_list_of_connections.get(connection_group) == None:
            self.global_list_of_connections[connection_group] = [con_cfg]
        else:
            self.global_list_of_connections[connection_group].append(con_cfg)

    def _get_user(self):
        return 'TEST_USER'
        
    def remove_connection_group(self, connection_group):
        """ Remove connection group """
        if (str(connection_group) != "0" and
            self.global_list_of_connections.get(connection_group) and
            type(self.global_list_of_connections.get(connection_group)) is list):
            max_num = len(self.global_list_of_connections)
            i = 0
            while not self.is_empty_group(connection_group) and i < max_num:
                self.release(self.global_list_of_connections[connection_group][0]["connection"])
                i += 1

    def remove_all_connections(self):
        """ Remove all connections
        """
        con_number = 0
        try:
            grps = self.global_list_of_connections.keys()
            for grp in grps:
                while not self.is_empty_group(grp):
                    l = len(self.global_list_of_connections[grp])
                    con_cfg = self.global_list_of_connections[grp][0]
                    try:
                        self.log_connection_info(con_cfg["id"])
                        self.release(con_cfg["connection"])
                        con_number += 1
                    except Exception as e:
                        #if remove can't be doing
                        #and it is in group show info
                        log.exception(e)
                        if l == len(self.global_list_of_connections[grp]):
                            log.error("Error on remove connection:")
                            self.log_connection_info_cfg(con_cfg)
                            self.global_list_of_connections[grp].pop(0)
        except Exception as e:
            log.exception(e)
        log.info("REMOVE %s CONNECTIONS", con_number)
        return con_number

    def remove_hung_connections(self):
        log.info("REMOVE HUNG CONNECTIONS")
        cur_time = time.time()
        grps = self.global_list_of_connections.keys()
        for grp in grps:
            i = 0
            while not self.is_empty_group(grp) and i < len(self.global_list_of_connections[grp]):
                con_cfg = self.global_list_of_connections[grp][i]
                if cur_time - con_cfg["time"] > self.max_connection_wait_time:
                    try:
                        self.log_connection_info(con_cfg["id"])
                        self.release(con_cfg["connection"])
                    except Exception as e:
                        log.exception(e)
                else:
                    i += 1

        self.hung_previous_remove = time.time()
        log.info("FINISH REMOVE HUNG CONNECTIONS")


    def get_next_group_number(self):
        self.connection_group_number = self._get_next_group_number()
        i = 1
        while not self.is_empty_group(self.connection_group_number) and i < self.max_connection_group_number:
            self.connection_group_number = self._get_next_group_number()
            i += 1
        if i == self.max_connection_group_number:
            return 0
        else:
            return self.connection_group_number

    def is_empty_group(self, group):
        return not self.global_list_of_connections.has_key(group) or len(self.global_list_of_connections[group]) == 0

    def _get_next_group_number(self):
        if self.connection_group_number < self.max_connection_group_number:
                self.connection_group_number = self.connection_group_number + 1
        else:
                self.connection_group_number = 1
        return self.connection_group_number


    def log_connections_info(self):
        cur_time = time.time()
        log.info( "===========================================")
        total = 0
        for grp in self.global_list_of_connections:
            grp_list = self.global_list_of_connections[grp]
            log.info("grp = %s,  count = %s", str(grp), str(len(grp_list)))
            for num, con_cfg in enumerate(grp_list):
                log.info("    id = %s,  time = %s" , con_cfg["id"], "%2.4f" % (cur_time - con_cfg["time"]))
                total +=1
        log.info( "total = %s", total)
        log.info( "==========================================")
        total = 0
        for key in self.connection_map:
            con = self.connection_map[key]
            log.info("    id = %s, time = %s", con["id"], "%2.4f" % (cur_time - con["time"]))
            total +=1
        log.info( "total = %s", total)
        log.info( "==========================================")


    def log_connection_info(self, con_id, grp = None, limit = None):
        conf = self._get_conf(con_id, grp)
        if not conf:
                log.info("Connection with id = %s, and grp = %s MISSING ", str(con_id), str(grp))
                return
        self.log_connection_info_cfg(conf)

    def log_connection_info_cfg(self, conf, limit = None):
        log.info("CONNECTION:")
        log.info("conn_id = \"%s\"", conf.get("id"))
        log.info("conn_grp = \"%s\"", conf.get("grp"))
        log.info("time = \"%s\"", "%2.4f" % (time.time() - conf.get("time")))
        log.info("user = \"%s\"", conf.get("user"))
        log.info("path =  ")
        path = self._get_path_part(conf.get("path"), limit)
        log.info("".join(path))

    def _get_path_part(self, path,  limit):
        max_len = len(path)
        if type(limit) is str or type(limit) is unicode and limit.isdigit():
            limit = int(limit)
        if type(limit)  is int:
            plen = min(max_len, limit)
            path = path[-plen:]
        return path


    def remove_con(self, con_id, grp = None):
        conf = self._get_conf(con_id, grp)
        if conf:
            con = conf.get("connection")
            self.release(con)


    def _get_conf(self, con_id, grp = None):
        conf = self._get_conf_by_id(con_id)
        if conf: return conf
        if grp:
           return self._get_conf_by_id_and_grp(con_id, grp)
        return None


    def _get_conf_by_id(self, con_id):
        for i in self.connection_map:
            con_cfg = self.connection_map[i]
            if con_cfg.get("id") == con_id:
                return con_cfg
        return None


    def _get_conf_by_id_and_grp(self, con_id, grp):
        for cfg in self.global_list_of_connections[grp]:
            if cfg.get("id") == con_id:
                return cfg
                con = cfg.get("connection")
                return con
        return None
