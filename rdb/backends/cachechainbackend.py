from backend import StorageBackend

from .. rdbutil import NotFound
from bdbbackend import BDBBackend
from memcachebackend import MemcacheBackend

class CacheChainBackend(StorageBackend):
    """
    Uses a list of backends in sequence. Because they all share
    OptionParser instances, they need to not overlap on usage of
    command-line arguments
    """
    backends = (MemcacheBackend, BDBBackend)

    def __init__(self, options, args):
        self.caches = tuple(backend(options, args)
                            for backend in self.backends)

    def _get(self, key, default = None):
        found_idx = -1
        for i, cache in enumerate(self.caches):
            try:
                ret = cache.get(key, default = NotFound)
                found_idx = i
            except NotFound:
                pass

        if found_idx == -1:
            # we couldn't find it
            return default

        # we found it, let's push it all the way back up the cache
        # chain
        for cache in self.caches[:found_idx]:
            cache.put(key, ret)

        return ret

    def _get_multi(self, keys):
        ret = {}
        pushup = {} # dict(cacheno -> dict(key -> value))
        keys = set(keys)
        find_keys = set(keys)

        for i, cache in enumerate(self.caches):
            # a NoneResult should never be returned from get_multi
            # (because those are converted to Nones before returning),
            # so we'll use that as a stand-in value to detect the
            # not-found state here, even if it looks a little
            # backwards
            subret = cache.get_multi(find_keys)
            find_keys -= set(subret.keys())
            for pushupcache_no in range(i):
                pushup.setdefault(pushupcache_no, {}).update(subret)

            ret.update(subret)

            if not find_keys:
                # we found them all
                break

        # for the ones we did find, push those up the cache-chain
        for i, c_keys in pushup.iteritems():
            self.caches[i].put_multi(c_keys)

        # we've got to convert the Nones into NoneResults here,
        # because our parent class will be expecting that
        return dict((key, NoneResult() if val is None else val)
                    for (key, val) in ret.iteritems())

    def _put(self, key, val):
        for cache in self.caches:
            cache.put(key, val)

    def _put_multi(self, keys):
        for cache in self.caches:
            cache.put_multi(keys)

    def _delete(self, key):
        for cache in self.caches:
            cache.delete(key)

    @classmethod
    def parse_arguments(cls, optparse):
        for backend in cls.backends:
            backend.parse_arguments(optparse)

    def open(self):
        for cache in self.caches:
            cache.open()

    def close(self):
        for cache in getattr(self, 'caches', []):
            cache.close()

    def stats(self):
        return dict((cache.__class__.__name__, cache.stats())
                    for cache in self.caches)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.caches)
