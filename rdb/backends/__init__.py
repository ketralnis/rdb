backends = {}

from memcachebackend import have_memcache
if have_memcache:
    from memcachebackend import MemcacheBackend
    backends['memcache'] = MemcacheBackend

from bdbbackend import have_bdb, BDBBackend
if have_bdb:
    from bdbbackend import BDBBackend
    backends['bdb'] = BDBBackend

if have_bdb and have_memcache:
    from cachechainbackend import CacheChainBackend
    backends['cachechain'] = CacheChainBackend
