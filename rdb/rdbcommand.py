#!/usr/bin/env python

import os
import sys
import os.path
import simplejson as json
from optparse import OptionParser

from rdbclient import client_from_spec


class RDBCommand(object):
    requires_keys = True

    def __init__(self, server, json_output, newlines = True):
        self.rdb = client_from_spec(server)
        self.json_output = json_output
        self.newlines = newlines

    def cmd_error(self, s):
        sys.stderr.write(s)
        sys.stderr.write('\n')
        sys.exit(1)


class RDBls(RDBCommand):
    requires_keys = False

    def run(self, keys):
        if keys:
            ret_keys = self.rdb.get_multi(keys)
            ret_keys = [key for (key, val) in ret_keys.iteritems()
                         if val]
            if self.json_output:
                print json.dumps(ret_keys)
            else:
                for key in ret_keys:
                    print key
        else:
            ret_keys = self.rdb.keys()
            if self.json_output:
                print json.dumps(ret_keys)
            else:
                for key in ret_keys:
                    print key


class RDBcat(RDBCommand):

    def run(self, keys):
        ret = {}

        # we could actually just use _bulk all of the time, but the
        # point here is to test all of the available methods
        if len(keys) == 1:
            ret[keys[0]] = self.rdb.get(keys[0])
        else:
            ret = self.rdb.get_multi(keys)

        if self.json_output:
            print json.dumps(ret)
        else:
            if self.newlines:
                for val in ret.values():
                    print val
            else:
                # don't use 'print' so that we can inhibit the extra
                # newline
                sys.stdout.write(''.join(ret.values()))


class RDBput(RDBCommand):

    def run(self, keys):
        if len(keys) == 1:
            self.rdb.put(keys[0], sys.stdin.read())
        else:
            # not sure how we'd manage this with only one stdin, so we
            # just won't do it
            self.cmd_error("can only PUT one key at a time")


class RDBrm(RDBCommand):

    def run(self, keys):
        for key in keys:
            self.rdb.delete(key)


class rdbtestobject(object):
    "Just a picklable object to be used by the RDBtest command"

    def __init__(self, test):
        self.test = test

    def __eq__(self, other):
        return self.test == other.test

    def __str__(self):
        return "%s(%r)" % (self.__class__.__name__, self.test)


class RDBtest(RDBCommand):

    def run(self, keys):
        print "Using client %r" % self.rdb
        if len(keys) < 2:
            self.cmd_error("need at least two keys that I can play with. "
                           "note that I'll destroy them")
        print 'put_multi'
        self.rdb.put_multi((key, key)
                           for key in keys)
        print 'get_multi'
        assert all(key == value
                   for (key, value)
                   in self.rdb.get_multi(keys).iteritems())
        testval = 'a new value!'
        for key in keys:
            print 'put', key
            self.rdb[key] = testval
            print 'get', key
            assert self.rdb[key] == testval
            print 'del', key
            del self.rdb[key]

        print 'test unicode'
        unic = u'bacon' + unichr(40960) + u'abcd' + unichr(1972)
        self.rdb[keys[0]] = unic
        assert self.rdb[keys[0]] == unic
        self.rdb.put_multi({keys[0]: unic,
                            keys[1]: unic})
        assert all(value == unic
                   for (key, value)
                   in self.rdb.get_multi([keys[0], keys[1]]).iteritems())

        print 'json objects'
        obj = dict(a = 1, b = 2)
        self.rdb[keys[0]] = obj
        assert self.rdb[keys[0]] == obj
        self.rdb.put_multi({keys[0]: obj,
                            keys[1]: obj})
        assert all(value == obj
                   for (key, value)
                   in self.rdb.get_multi([keys[0], keys[1]]).iteritems())

        print 'pickled objects'
        obj = rdbtestobject('bacon')
        self.rdb[keys[0]] = obj
        assert self.rdb[keys[0]] == obj
        self.rdb.put_multi({keys[0]: obj,
                            keys[1]: obj})
        assert all(value == obj
                   for (key, value)
                   in self.rdb.get_multi([keys[0], keys[1]]).iteritems())

        # not yet allowing unicode keys
        #self.rdb[unic] = unic
        #assert self.rdb[unic] == unic
        #self.rdb.put_multi({unic: unic})
        #assert self.rdb.get_multi([unic]) == {unic: unic}
        #del self.rdb[unic]

        print 'cleanup'
        self.rdb.delete_multi(keys)

if __name__=='__main__':
    defaultserver = os.environ.get('RDB_SERVER', 'localhost:6552')

    parser = OptionParser(usage='%prog: [options] [key1 key2 ...]')
    parser.add_option('-s', '--server',
                      dest='server',
                      help="""address of the server (can be specified by the
                              environment variable RDB_SERVER, e.g. "localhost"
                              or "localhost:6552"). If this string contains a
                              semicolon, it should be of the form
                              "server:port,weight;server:port,weight" """,
                      metavar='SERVER',
                      default=defaultserver)
    parser.add_option('-j', '--json',
                      action='store_true',
                      dest='json',
                      help='JSON output',
                      default=False)

    parser.add_option('-n', '--newlines',
                      action='store_true',
                      dest='newlines',
                      help='''print a newline when print multiple non-JSON
                              values''',
                      default=True)
    parser.add_option('-r', '--nonewlines',
                      action='store_false',
                      dest='newlines',
                      help='''don't print a newline when print multiple
                              non-JSON values''',
                      default=True)

    options, keys = parser.parse_args()

    if not options.server:
        parser.error('server not specified')

    clss = {'rdbls': RDBls,
            'rdbrm': RDBrm,
            'rdbput': RDBput,
            'rdbcat': RDBcat,
            'rdbtest': RDBtest}
    myname = os.path.basename(sys.argv[0])
    if myname.endswith('.py'):
        myname = myname[:-3]
    if myname not in clss:
        parser.error('unknown operation %r' % myname)

    cls = clss[myname]
    command = cls(options.server, options.json, newlines = options.newlines)

    if not keys and command.requires_keys:
        parser.error('no keys specified')

    command.run(keys)
  
