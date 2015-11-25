'''
Utility-module to wrap essential parts of the Evenlet-API.
Falls back to regular threads where eventlet is unavailable
'''
from __future__ import print_function

try:
    import eventlet
    if getattr(eventlet, 'disabled', False):
        raise ImportError
    from os import environ
    if environ.get('CONCURRENT_EVENTLET') == '0':
        raise ImportError

    from eventlet import sleep, spawn, spawn_after
    from eventlet import GreenPool as Pool
    from eventlet import connect, listen
    from eventlet.event import Event
    from eventlet.green import socket, subprocess
    from eventlet.hubs import trampoline
    from eventlet.timeout import Timeout

    class ThreadLock:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_value, traceback):
            pass

    def serve(sock, handler):
        try:
            return eventlet.serve(sock, handler)
        except socket.error:
            pass

except ImportError:
    import ctypes, socket, sys, subprocess, threading, traceback, weakref
    from errno import EBADF, errorcode
    from time import sleep as _sleep, time

    from multiprocessing.pool import ThreadPool
    from threading import RLock as ThreadLock

    tl = threading.local()
    tl.timeouts = []

    def _calcTimeout():
        t = getattr(tl, 'timeouts', None)
        if t:
            return min(t) - time()
        else:
            return None

    def sleep(seconds=0):
        return _sleep(seconds)

    class Pool(ThreadPool):
        def __init__(self, size):
            if not hasattr(threading.current_thread(), "_children"):
                threading.current_thread()._children = weakref.WeakKeyDictionary()
            ThreadPool.__init__(self, size)
        def _catch_exc(self, func, *args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                print("Exception caught in ThreadPool", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

        def spawn(self, *args, **kwargs):
            # First arg in args is the function to execute
            self.apply_async(self._catch_exc, args, kwargs)

    class Event:
        def __init__(self):
            self._value = None
            self._error = None
            self._event = threading.Event()

        def send(self, value):
            self._value = value
            self._error = None
            return self._event.set()

        def send_exception(self, e, v=None, t=None):
            self._error = (e, v, t)
            return self._event.set()

        def wait(self):
            self._event.wait(_calcTimeout())
            if self._error:
                (e, v, t) = self._error
                raise e, v, t
            else:
                return self._value

    def _async_raise(tid, excobj):
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(excobj))
        if res == 0:
            raise ValueError("nonexistent thread id")
        elif res > 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    class Thread(threading.Thread):
        def __init__(self, target, args, kwargs, *_args, **_kwargs):
            self.target = target
            self.args = args
            self.kwargs = kwargs
            super(Thread, self).__init__(*_args, target=target, args=args, kwargs=kwargs, **_kwargs)

        def _print_exc(self):
            try:
                traceback.print_exception(*self.result)
            except:
                pass

        def wait(self):
            self.join()
            return self.result

        def kill(self, excobj=SystemExit):
            assert self.isAlive(), "thread must be started"
            for tid, tobj in threading._active.items():
                if tobj is self:
                    _async_raise(tid, excobj)
                    return

        def run(self):
            try:
                self.result = self.target(*self.args, **self.kwargs)
            except:
                self.result = sys.exc_info()
                self._print_exc()

    class Timeout(threading.Thread):
        def __init__(self, duration, excobj=None):
            threading.Thread.__init__(self)
            self.duration = duration
            self.excobj = excobj or self
            self.setDaemon(True)
            self.deadline = time() + duration
            self._tid = threading.current_thread().ident
            self._done = threading.Event()
            self.start()

        def __enter__(self):
            if hasattr(tl, 'timeouts'):
                tl.timeouts.append(self.deadline)
            else:
                tl.timeouts = [self.deadline]
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            deadline = tl.timeouts.pop()
            assert deadline == self.deadline
            self._done.set()
            if exc_value == self:
                return 1

        def run(self):
            if not self._done.wait(self.duration):
                _async_raise(self._tid, self.excobj)

    def spawn(func, *args, **kwargs):
        t = Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    def spawn_after(time, func):
        t = threading.Timer(time, func)
        t.start()
        return t

    class _socket(socket.socket):
        def __init__(self, *args, **kwargs):
            self.accepting = False
            super(_socket, self).__init__(*args, **kwargs)

        def listen(self, backlog):
            self.accepting = True
            super(_socket, self).listen(backlog)

        def close(self):
            if self.accepting:
                self.accepting = False
                try:
                    connect(self.getsockname())
                except:
                    pass
            super(_socket, self).close()

    def connect(address, family=socket.AF_INET, bind=None):
        if family in (socket.AF_INET, socket.AF_INET6, socket.AF_UNIX):
            s = _socket(family, socket.SOCK_STREAM)
            if bind:
                s.bind(bind)
            s.connect(address)
            return s
        else:
            raise ValueError("Unknown address type")

    def listen(address, backlog=5):
        s = _socket(socket.AF_INET, socket.SOCK_STREAM, )
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(address)
        s.listen(backlog)
        return s

    def serve(sock, handler):
        try:
            while sock.accepting:
                conn, addr = sock.accept()
                if sock.accepting:
                    spawn(handler, conn, addr)
        except socket.error:
            pass

    def trampoline(fd, *args, **kwargs):
        'Not applicable without eventloop'
        pass