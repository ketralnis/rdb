import cPickle as pickle
import urllib3
import hashlib
import simplejson as json
from itertools import chain
from urllib import quote, urlencode
from contextlib import contextmanager

from rdbutil import DictNature, NotFound
from pool import ThreadPool, Pool

def client_from_spec(spec):
    if ';' in spec:
        servers = []
        for server in spec.split(';'):
            if ',' in server:
                server, weight = server.split(',')
                weight = int(weight)
            else:
                weight = 1
            servers.append((server, weight))
        return RDBMultiClient(servers)
    else:
        # we return a multiclient either way because we need it to be
        # thread-safe
        return RDBMultiClient([(spec,1)])

class ConsistantHasher(object):
    def __init__(self, weights):
        if isinstance(weights, dict):
            weights = sorted(weights.items(), key=lambda x: x[1])

        # now weights =:= [(node_name, int), ...]

        # In practise, the total of all of the weights is probably
        # less than a hundred. So to speed up lookups, we're going to
        # make an map of all possible weights. Then we can just
        # address it with self.nodes[index_for(key)]. The length of
        # this will be the total of all of the weights, so if you
        # start doing silly things like
        # weights==[(node1,100000),(node2,10000000)], this will eat up
        # a load of memory
        self.nodes = []
        self.total = 0
        for node, weight in weights:
            assert isinstance(weight, (int, long))
            for x in range(weight):
                self.nodes.append(node)
            self.total += weight

    def __getitem__(self, key):
        return self.nodes[self.index_for(key)]

    def index_for(self, key):
        # get a hash of the object
        h = int(hashlib.md5(key).hexdigest(), 16) % self.total
        h = h % self.total
        return h

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.weights)


class RDBMultiClient(DictNature):
    pool_size = 5 # use this many threads per RDBClient

    def __init__(self, weights):
        self.weights = weights
        self.nodes = set(x[0] for x in weights)
        self.hasher = ConsistantHasher(weights)

        self.clients = dict((node, RDBClient(node))
                            for node in self.nodes)

        self.parallel_transfer = True
        if self.parallel_transfer:
            # a thread-pool to support concurrent bulk requests that
            # span multiple nodes
            self.thread_pool = ThreadPool(len(self.nodes) * self.pool_size)

    def get(self, key, *a, **kw):
        self.clients[self.hasher[key]].get(key, *a, **kw)

    def put(self, key, value, *a, **kw):
        self.clients[self.hasher[key]].put(key, value, *a, **kw)

    def delete(self, key, *a, **kw):
        self.clients[self.hasher[key]].delete(key, *a, **kw)

    def get_multi(self, keys):
        return self.bulk(get = keys)

    def put_multi(self, keys):
        return self.bulk(put = keys)

    def delete_multi(self, keys):
        return self.bulk(delete = keys)

    def bulk(self, get = [], put = {}, delete = []):
        """Do multiple _bulk requests in parallel"""
        if not isinstance(put, dict):
            put = dict(put)

        by_node = {}
        for key in get:
            by_node.setdefault(self.hasher[key],
                               {}).setdefault('get', []).append(key)
        for key in delete:
            by_node.setdefault(self.hasher[key],
                               {}).setdefault('delete', []).append(key)
        for key, val in put.iteritems():
            by_node.setdefault(self.hasher[key],
                               {}).setdefault('put', {})[key] = val

        funcs = []
        for node, ops in by_node.iteritems():
            def fetch(_node, _ops):
                def _fetch():
                    return self.clients[_node].bulk(**_ops)
                return _fetch
            funcs.append(fetch(node, ops))

        if self.parallel_transfer and len(funcs) > 1:
            bulks = self.thread_pool.pmap(funcs)
        else:
            bulks = [f() for f in funcs] 

        ret = {}
        for bulk in bulks:
            ret.update(bulk)
        return ret

    def _by_node(self, keys):
        ret = {}
        for key in keys:
            ret.setdefault(self.hasher[key], []).append(key)
        return ret.items()

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.weights)

class RDBClient(DictNature):
    """A non-thread-safe client for RDB. Use RDBMultiClient for
       thread-safety and multi-server hashing"""

    def __init__(self, server):
        if ':' in server:
            server, port = server.split(':')
            port = int(port)
        else:
            server, port = server, 6552

        self.server = server
        self.port = port

        self.http_pool = urllib3.HTTPConnectionPool(self.server, self.port)

    def get(self, key, default = NotFound):
        try:
            return self.decode_value(self.openurl('GET', key = key), from_json=False)
        except NotFound:
            if default is NotFound:
                raise
            else:
                return default
            

    def put(self, key, value):
        self.openurl('PUT', key = key,
                     postdata = self.encode_value(value, return_json = False))

    def delete(self, key):
        self.openurl('DELETE', key = key)

    def get_multi(self, keys):
        return self.bulk(get = keys)

    def put_multi(self, keys):
        return self.bulk(put = keys)

    def delete_multi(self, keys):
        return self.bulk(delete = keys)

    def bulk(self, get = [], put = {}, delete = []):
        assert get or put or delete

        postdata = {}

        # To make the logs a little more readable, make the URLs more
        # descriptive by changing func where appropriate. _get_multi,
        # _put_multi, and _delete_multi are just aliases for _bulk,
        # which ignores everything after the command in the URL, so we
        # can put the keys there for humans to see
        func = '_bulk'
        if get:
            postdata['get'] = {'keys': map(self.encode_key, get)}
            if not put and not delete:
                func = '_get_multi'
        if put:
            if not isinstance(put, dict):
                put = dict(put)

            put = dict((self.encode_key(key), self.encode_value(val))
                       for (key, val) in put.iteritems())
            postdata['put'] = put # already a dictionary

            if not get and not delete:
                func = '_put_multi'
        if delete:
            postdata['delete'] = {'keys': delete}
            if not get and not put:
                func = '_delete_multi'
        keys_str = '+'.join(quote(x, safe='') for x in chain(get, put.keys(), delete))
        func = '/%s/%s' % (func, keys_str)

        # where key, value are just e.g. dict('get' -> jsondata)
        postdata = dict((key, json.dumps(value))
                        for (key, value)
                        in postdata.iteritems())

        ret = self.openurl('POST', func=func,
                           postdata=postdata,
                           return_json=True)

        # the return data is a dict() containing any items requested
        # to GET, and may be an empty dict. for the key, json should
        # decode unicode keys for us
        ret = dict((key, self.decode_value(val))
                   for (key, val) in ret.iteritems())

        return ret

    def keys(self):
        ret = self.openurl('GET', func='/_all_keys',
                           return_json=True)
        return map(self.decode_key, ret) # json.loads should handle
                                         # decoding the keys to
                                         # unicode

    def items(self):
        ret = self.openurl('GET', func='/_all_data',
                           return_json=True)
        return dict((self.decode_key(key),
                     self.decode_value(value, from_json=True))
                    for (key, value) in ret.iteritems())

    iteritems = items

    def openurl(self, method, key = None, func = None,
                postdata = None, return_json=False):
        assert key or func and not (key and func)

        if key:
            url = '/data/%s' % quote(self.encode_key(key), safe='')
        else:
            assert isinstance(func, str)
            url = func

        # if we have post-data, encode it as necessary
        if isinstance(postdata, dict):
            postdata = urlencode(postdata)

        headers = {}
        if method == 'POST':
            # encoded by our caller
            headers['Content-Type'] = 'application/x-www-form-urlencoded'

        resp = self.http_pool.urlopen(method, url,
                                      body = postdata or None,
                                      headers = headers)
        code = resp.status
        msg = resp.reason

        if code == 404 and key is not None:
            raise NotFound

        if code != 200:
            raise Exception("Bad response: %s %s" % (code, msg))

        ret = resp.data

        return json.loads(ret) if return_json else ret

    @classmethod
    def encode_value(self, obj, return_json = True):
        try:
            ret = {'type': 'object', 'value': obj}
            json.dumps(ret)
        except TypeError:
            ret = {'type': 'pickle', 'value': pickle.dumps(obj)}
        return ret if return_json else json.dumps(ret, ensure_ascii=True)

    @classmethod
    def decode_value(cls, s, from_json = True):
        if not from_json:
            s = json.loads(s)

        if s['type'] == 'object':
            ret = s['value']
        elif s['type'] == 'pickle':
            ret = pickle.loads(str(s['value']))
        else:
            raise ValueError("Unknown return type %r" % s.get('type', None))

        return ret

    @classmethod
    def encode_key(cls, key):
        if isinstance(key, str):
            return key
        elif isinstance(key, unicode):
            return key.decode('us-ascii')
        raise TypeError('invalid key')

    @classmethod
    def decode_key(cls, key):
        if isinstance(key, str):
            return key
        elif isinstance(key, unicode):
            return key.decode('us-ascii')
        raise TypeError('invalid key')
