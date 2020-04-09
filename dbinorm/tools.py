#coding: utf-8

def evented(fn):
    def wrapper(self, *args, **kargs):
        event_name = fn.__name__
        events = self.events.get(event_name)
        before = []
        after = []
        new_events = []
        r = False
        if events != None:
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
        if len(before) > 0:
            for i in before:
                handler = i.get('handler')
                args = i.get("args", [])
                kargs = i.get("kargs", {})
                handler(*args, **kargs)

        result = fn(self, *args, **kargs)

        if len(after) > 0:
            for i in after:
                handler = i.get('handler')
                args = i.get("args", [])
                kargs = i.get("kargs", {})
                handler(*args, **kargs)
        return result
    return wrapper

def create_in(elems, prefix = "IN_ELEM", con = None, nbind = None):
    """Create request parts with params for request with in parts.
    """
    if nbind == None:
        if con != None:
            nbind = con.nbind
        else:
            nbind = lambda x: u":" + x

    retval = u""
    delim = u""
    params = {}
    for num, sys_name in enumerate(elems):
        pname = prefix + u"_" + str(num)
        retval += delim + nbind(pname)
        delim = u", "
        params[pname] = sys_name
    return retval, params
