#coding: utf-8

import logging
from string import Formatter
from textwrap import dedent

formatter = Formatter()
log = logging.getLogger()

class Request(object):

    @staticmethod
    def oracle_nbind(val):
        return u":" + val

    @staticmethod
    def postgres_nbind(val):
        return u"%(" + val + ")s"

    _bind = {
        'oracle': oracle_nbind.__get__(object), # or oracle_nbind.__func__
        'postgres': postgres_nbind.__get__(object)
    }

    def __init__(self, requests):
        self.requests = requests
        self._cashe = {'oracle': {}, 'postgres': {}}

    def get_by_db_type(self, val, db_type):
        """ May be to much.
        """
        #PARTIALLY COPIED TO rbs_db.db_functions.Connection.format_binds
        #find in cache
        db_dict = self._cashe.setdefault(db_type, {})
        db_req = db_dict.get(val)
        if db_req != None:
            return db_req
        #find in spec
        db_req_dict = self.requests.get(db_type)
        if db_req_dict != None:
            db_req = db_req_dict.get(val)
            if db_req != None:
                return db_req
        #prepare request for database and save it in cache
        request = self.requests.get(val)
        if isinstance(request, (str, unicode)):
            parsed = [fn for _, fn, _, _ in formatter.parse(request) if fn is not None]
            bind = self._bind.get(db_type)
            if len(parsed) > 0:
                try:
                    request = request.format(**dict([(v, bind(v)) for v in parsed]))
                except TypeError as e:
                    log.exception(e)
                    log.info("%s, %s", val, request)
                    raise e
            else:
                 request = request.format()
            db_dict[val] = request
            return request
        else:
            raise Exception("Request %s missing implementation for db_type %s." % (val, db_type))

    def get_by_con(self, val, con):
        db_type = con.get_db_type()
        return self.get_by_db_type(val, db_type)

    def select(self, val, con, params = None):
        r = self.get_by_con(val, con)
        res = con.uexec_get_list(r, params)
        return res

def fdedent(string):
    """ Remove start indentation.
        Copy of method rbs_utl.co_pprint.fdedent

        :param string: string for dedent
        :type string: str, unicode
        :return str, unicode:
    """
    string = dedent(string)
    if string and string[0] == '\n':
        string = string[1:]
    return string
