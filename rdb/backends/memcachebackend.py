from backend import StorageBackend

try:
    from memcache import Client as MemcacheClient
    have_memcache=True
except ImportError:
    have_memcache=False

class MemcacheBackend(StorageBackend):

    def __init__(self, options, args):
        if not options.servers:
            raise Exception('memcache servers are required')

        self.servers = options.servers.split(',')

        self.open()

    @classmethod
    def parse_arguments(cls, optparse):
        optparse.add_option('-m', '--servers', dest='servers',
                            help='comma-separated list of memcached servers',
                            metavar='SERVERS',
                            default='localhost:11211')

    def _encode_key(self, key):
        return key.encode('base64').rstrip('\n')

    def _decode_key(self, key):
        return key.decode('base64')

    def _get(self, key, default = None):
        return self.mc.get(self._encode_key(key))

    def _get_multi(self, keys):
        keys = map(self._encode_key, keys)
        ret = self.mc.get_multi(keys)
        return dict((self._decode_key(key), value)
                    for (key, value) in ret.iteritems())

    def _put(self, key, val):
        return self.mc.set(self._encode_key(key), val)

    def _put_multi(self, keys):
        keys = dict((self._encode_key(key), value)
                    for (key, value) in keys.iteritems())
        self.mc.set_multi(keys)

    def _delete(self, key):
        self.mc.delete(self._encode_key(key))

    def close(self):
        if getattr(self, 'mc', None):
            self.mc.disconnect_all()
            self.mc = None

    def open(self):
        self.close()
        self.mc = MemcacheClient(self.servers)

    def stats(self):
        return dict(self.mc.get_stats())


