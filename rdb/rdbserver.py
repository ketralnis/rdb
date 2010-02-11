#!/usr/bin/env python

import re
import sys
import logging
import simplejson as json
from optparse import OptionParser

import tornado.httpserver
import tornado.ioloop
import tornado.web

from backends import backends
from rdbutil import NotFound


class Config(object):

    def __init__(self, backend = None, port = None):
        self.backend = backend
        self.port = port


class RDBRequestHandler(tornado.web.RequestHandler):

    @property
    def _backend(self):
        return self.application.settings['config'].backend


class MainHandler(RDBRequestHandler):
    "/"

    def get(self):
        resp = '''
               <html>
                <body>
                 <form method="POST" action="/_bulk">
                  <table>
                   <tr>
                    <th>method</th>
                    <th>example</th>
                    <th></th>
                   </tr>
                   <tr>
                    <td>get</td>
                    <td>{&quot;keys&quot;: [&quot;bacon&quot;]}</td>
                    <td><input name="get"/></td>
                   </tr>
                   <tr>
                    <td>put</td>
                    <td>{&quot;bacon&quot;: &quot;is yummy&quot;}</td>
                    <td><input name="put"/></td>
                   </tr>
                   <tr>
                    <td>delete</td>
                    <td>{&quot;keys&quot;: [&quot;bacon&quot;]}</td>
                    <td><input name="delete"/></td>
                   </tr>
                  </table>
                  <input type="submit" />
                 </form>
                </body>
               </html>
               '''
        self.write(resp)


class DataHandler(RDBRequestHandler):
    '/data/.*'

    def get(self, key):
        try:
            self.write(self._backend[key])
        except NotFound:
            raise tornado.web.HTTPError(404)

    def put(self, key):
        value = self.request.body
        try:
            json.loads(value) # this will throw an exception if it's not
                              # valid JSON data
        except:
            raise tornado.web.HTTPError(406, 'Not valid JSON')
        self._backend[key] = value

    # some day this should support a mime multipart decode for
    # form-based upload
    #def post(self, key):
    #    pass
    def delete(self, key):
        del self._backend[key]


class BulkHandler(RDBRequestHandler):

    def post(self, _op, _keysstr):
        # we actually ignore the operation and use the same handler
        # for all bulk operations. Yes, that means you can pass put=
        # to _delete_multi if you *really* wanted
        ret = {}

        get = self.get_argument('get', None)
        if get:
            keys = json.loads(get)['keys']
            ret = self._backend.get_multi(keys)
            ret = dict((key, json.loads(value))
                       for (key, value) in ret.iteritems())

        put = self.get_argument('put', None)
        if put:
            values = json.loads(put)
            values = dict((key, json.dumps(val))
                          for (key, val)
                          in values.iteritems())
            self._backend.put_multi(values)

        delete = self.get_argument('delete', None)
        if delete:
            keys = json.loads(delete)['keys']
            for key in keys:
                self._backend.delete(key)

        self.write(json.dumps(ret))


class IteratorHandler(RDBRequestHandler):
    '/_all_keys, /_all_data'

    def get(self, op):
        if not self._backend.supports_iteration:
            raise HTTPError(501)

        if op == '_all_data':
            ret = self._yield_json_dict((key, json.loads(value))
                                        for key, value
                                        in self._backend.items())
        elif op == '_all_keys':
            ret = self._yield_json_list(self._backend.keys())

        for s in ret:
            self.write(s)

    def _yield_json_list(self, l):
        """Utility function to yield an arbitrarily long JSON list"""
        first = True
        yield '['
        for key in l:
            if first:
                first = False
            else:
                yield ','
            yield json.dumps(key)
        yield ']'

    def _yield_json_dict(self, l):
        """Utility function to yield an arbitrarily long JSON
           dict. Despite the name, takes an iterator yielding
           two-tuples"""
        first = True
        yield '{'
        for key, value in l:
            if first:
                first = False
            else:
                yield ','
            yield json.dumps(key)
            yield ':'
            yield json.dumps(value)
        yield '}'


class StatsHandler(RDBRequestHandler):
    '/_stats'

    def get(self):
        self.write(json.dumps(self._backend.stats()))


class RDBServerApplication(tornado.web.Application):
    maps = [
        (r'/', MainHandler),
        (r'/data/(.*)', DataHandler),
        (r'/(_bulk|_get_multi|_put_multi|_delete_multi)(/?.*|$)', BulkHandler),
        (r'/(_all_data|_all_keys)', IteratorHandler),
        (r'/_stats', StatsHandler),
        ]

    def __init__(self, config):
        self.rdb_config = config
        tornado.web.Application.__init__(self, self.maps, config = config)


def args_to_config(sysargs):
    parser = OptionParser(usage="%prog [rdb options] backendname"+
                          " [ -- backendoptions ... ]")
    parser.add_option('-p', '--port', dest='port',
                      help='which TCP port to listen on',
                      metavar='PORT',
                      type='int', default=6552)
    serveroptions, args = parser.parse_args(sysargs)

    if len(args) < 1:
        parser.error('no backend specified')

    backendname, backend_args = args[0], args[1:]

    if backendname not in backends:
        parser.error('unknown backend %r' % backendname)

    # each new process will need to build its own backend, since they
    # shouldn't share file descriptors or sockets
    backend_cls = backends[backendname]
    backend_optionparser = OptionParser()
    backend_cls.parse_arguments(backend_optionparser)
    backend_options, backend_args = (
        backend_optionparser.parse_args(backend_args))
    backend = backend_cls(backend_options, backend_args)

    return Config(backend=backend,
                  port=serveroptions.port)
    

def main(sysargs):
    config = args_to_config(sysargs[1:])
    logging.basicConfig(level=logging.INFO)

    config.backend.open() # let's do this now so that we fail early if
                          # it can't be opened. request processors
                          # will open their own

    application = RDBServerApplication(config)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(config.port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main(sys.argv)
