class DictNature(object):
    """Mixin class to allow something with get/put/has_key/delete
       methods to be accessed using [] notation and have a default
       get_multi and put_multi implementation"""

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.put(key, value)

    def __in__(self, key):
        return self.has_key(key)

    def __delitem__(self, key):
        return self.delete(key)


class NotFound(Exception):
    pass


def trace(fn):
    "function decorator to make a function be really verbose"
    def _fn(*a, **kw):
        print fn.__name__, repr(a), repr(kw)
        ret = fn(*a, **kw)
        print fn.__name__, repr(a), repr(kw), '->', repr(ret)
        return ret
    return _fn
        
