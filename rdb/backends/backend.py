from optparse import OptionParser

from .. rdbutil import NotFound, DictNature

class NoneResult(object):
    """stored in caches instead of pickling None itself so that we can
       tell them apart"""
    pass


class NoInclude(object):
    """used as the default param to get_multi to indicate that the
       returned dictionary should not include not-found results"""
    pass


class StorageBackend(DictNature):
    supports_iteration = False # not all backends support retrieving
                               # all of their keys
    def __init__(self, options, args):
        pass

    def get(self, key, default = NotFound):
        """Fetch a value with the given key, returning 'default' if
           it's not found. The special default NotFound will raise a
           NotFound exception if the key is not found. Implementations
           can take advantage of this class's implementation by
           implementing _get and returning None for not-found and a
           NoneResult instance for stored-None."""
        assert isinstance(key, str)

        try:
            ret = self._get(key, None)
        except NotFound:
            if default is NotFound:
                raise NotFound

        if ret is None and default is NotFound:
            raise NotFound
        elif ret is None and default is not NotFound:
            return default
        elif isinstance(ret, NoneResult):
            return None
        else:
            # we found it!
            assert isinstance(ret, str)
            return ret

    def _get(self, key, default = None):
        """On not-found, implementations may return 'default' or raise
           a NotFound exception"""
        raise NotImplementedError

    def put(self, key, value):
        """Create or replace a key/value, storing a pickled NoneResult
           for stored-none values. Implementations can take advantage of
           this class's None handling by implementing _put"""
        assert isinstance(key, str) and isinstance(value, str)

        self._put(key, NoneResult() if value is None else value)

    def _put(self, key, value):
        raise NotImplementedError

    def delete(self, key):
        """Remove a given key/value from the store"""
        return self._delete(str(key))

    def _delete(self, key):
        raise NotImplementedError

    def has_key(self, key):
        """Returns true if the given key exists in the store."""
        raise NotImplementedError

    def keys(self):
        """Returns an iterator defining all known keys. If a backend
           supports this, it should set 'supports_iteration'. Not all
           backends implement this."""
        raise NotImplementedError

    def get_multi(self, keys, default = NoInclude):
        """Retrieve multiple keys from the backend at once, returning
           a dictionary of keys to values. Non-found keys will be set
           to the value of the 'default' parameter, unless it's the
           special value NoInclude, which indicates that those keys
           should not be included at all. NotFound can also be used to
           raise a NotFound exception.
 
           Backends can take advantage of this class's implementation
           of the not-found handling by implementing _get_multi and
           returning values of None for not-found, NoneResult for
           stored-None."""
        keys = set(str(key) for key in keys)

        assert all(isinstance(key, str) for key in keys)

        # some backends (i.e. memcache) insist on returning Nones for
        # non-found keys
        from_server = self._get_multi(keys)
        ret = {}
        for key in keys:
            if from_server.get(key, None) is not None:
                if isinstance(from_server[key], NoneResult):
                    ret[key] = None
                else:
                    ret[key] = from_server[key]
            elif default is NotFound:
                raise NotFound
            elif default is not NoInclude:
                ret[key] = default

        assert all(isinstance(key, str) for key in ret.keys())

        return ret

    def _get_multi(self, keys):
        """On not-found, implementations may not include the key in
           the returned dictionary, or use None. This implementation makes many calls to _get"""
        ret = {}
        for key in keys:
            try:
                ret[key] = self._get(key, default = None)
            except NotFound:
                pass
        return ret

    def put_multi(self, keys):
        """Store multiple values at once, using a dictionary or
           iterator yielding tuples. Implementations can take
           advantage of this class's iterator and None handling by
           implementing _put_multi."""
        if isinstance(keys, dict):
            keys = keys.iteritems()

        keys = dict((str(key), NoneResult if val is None else val)
                    for (key, val)
                    in keys)

        assert all((isinstance(key, str) and isinstance(val, str))
                   for (key, val) in keys.iteritems())

        self._put_multi(keys)

    def _put_multi(self, keys):
        """The default implementation of _put_multi calls _put
           multiple times"""
        for key, value in keys.iteritems():
            self._put(key, value)

    def stats(self):
        """Returns a dictionary describing statistics and status
           information about the backend"""
        return {}

    def items(self):
        """Returns an iterator defining all known keys and their
           values. Not all backends are able to implement
           iteration. If a backend supports this, it should set
           'supports_iteration'. A very naive default implementation
           is provided"""
        for key in self.keys():
            yield key, self.get(key)

    iteritems = items

    def open(self):
        """Open/close will be called between fork() events. both should be
           idempotent, and may be called in __init__"""
        pass

    def close(self):
        """Open/close will be called between fork() events. both should be
           idempotent, and may be called in __init__"""
        pass

    def __del__(self):
        self.close()

    @classmethod
    def parse_arguments(cls, optionparser):
        pass

