import os.path

from backend import StorageBackend

from .. rdbutil import NotFound, DictNature, trace

try:
    from bsddb3 import db
    have_bdb = True
except ImportError:
    have_bdb=False

class BDBBackend(StorageBackend):
    supports_iteration = True

    def __init__(self, options, args):
        self.basedir = options.basedir
        assert os.path.exists(self.basedir)

        self.shmkey = options.shmkey

        self.env = self.data_db = None

        self.open()

    @classmethod
    def parse_arguments(cls, optparse):
        optparse.add_option('-b', '--basedir', dest='basedir',
                            help='base dir for BDB',
                            metavar='BASEDIR',
                            default='./')
        optparse.add_option('-k', '--shmkey', dest='shmkey',
                            help='''The shmkey to use for
                            BDB. Multiple instances referencing the
                            same BDB store must share the same shmkey,
                            and multiple BDB databases in different
                            stores must not share the same shmkey.''',
                            type='int',
                            metavar='SHMKEY',
                            default=0)

    def _get(self, key, default = None):
        """Note that we let the superclass's _get_multi just call this
           multiple times"""
        return self.data_db.get(key, default = default)

    def _put(self, key, value):
        """Note that we let the superclass's _put_multi just call this
           multiple times"""
        return self.data_db.put(key, value)

    def has_key(self, key):
        return self.data_db.exists(key)

    def _delete(self, key):
        try:
            self.data_db.delete(key)
        except db.DBNotFoundError:
            pass

    def keys(self):
        return iter(self.data_db.keys())

    def stats(self):
        return self.data_db.stat()

    def open(self):
        self.close()

        env = db.DBEnv()
        env.set_shm_key(self.shmkey)

        flags = db.DB_CREATE | db.DB_INIT_MPOOL | db.DB_SYSTEM_MEM
        # we'll need this when multi-process/multi-threaded support is added
        # flags |= db.DB_INIT_CDB
        env.open(self.basedir, flags)
        self.env = env

        data_db = db.DB(dbEnv = self.env)
        data_db.open('data.db', dbname = 'data',
                     dbtype = db.DB_HASH, flags = db.DB_CREATE)
        self.data_db = data_db

    def close(self):
        if hasattr(self, 'data_db') and self.data_db is not None:
            self.data_db.close()
        self.data_db = None
        if hasattr(self, 'env') and self.env is not None:
            self.env.close()
        self.env = None
