from Queue import Queue
from contextlib import contextmanager
from threading import Semaphore, Lock, Thread

class Pool(object):
    def __init__(self, clients):
        self.clients = list(clients)

        # to have callers block when there's no available client
        self.clients_sem = Semaphore(len(self.clients)) 

        # to protect accesses to the list of clients
        self.lock = Lock()
        
    @contextmanager
    def get_client(self):
        with self.clients_sem:
            with self.lock:
                c = self.clients.pop()
            try:
                yield c
            finally:
                with self.lock:
                    self.clients.insert(0, c)


class PooledThread(Thread):
    def __init__(self):
        self.q = Queue()
        self.kill_threads_on_exception = False
        Thread.__init__(self)

    def run(self):
        while True:
            func, resp_q = self.q.get()
            ret = exc = None
            try:
                ret = func()
            except Exception, e:
                exc = e
                if self.kill_threads_on_exception:
                    raise
            resp_q.put((ret, exc))
            self.q.task_done()

    def do(self, func):
        q = self.async_do(func)
        return self.async_retreive(q)

    def async_do(self, func):
        """Tells this thread to process this item, returning a queue
           that can be used to retreive the result when calculated"""
        q = Queue(1)
        self.q.put((func, q))
        return q

    def async_retrieve(self, q):
        resp, exc = q.get()
        q.task_done()
        if exc:
            raise exc
        else:
            return resp


class ThreadPool(Pool):

    def __init__(self, size = 10):
        threads = [PooledThread() for x in xrange(size)]
        for thread in threads:
            thread.setDaemon(True)
            thread.start()
        Pool.__init__(self, threads)

    def pmap(self, funcs):
        funcs = list(funcs)
        q_s = []
        for func in funcs:
            # damn you, Python scoping rules
            def _debind(_itm):
                return _itm
            with self.get_client() as client:
                q_s.append(client.async_do(_debind(func)))

        rets = []
        excs = []
        for q in q_s:
            resp, exc = q.get()
            if exc:
                excs.append(exc)
            else:
                rets.append(resp)
            q.task_done()
        if excs:
            raise excs[0]
        else:
            return rets

