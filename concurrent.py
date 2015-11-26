'''
Utility-module to wrap essential parts of the Evenlet-API.
Falls back to regular threads where eventlet is unavailable
'''
from __future__ import print_function

try:
    from os import environ
    if environ.get('CONCURRENT_EVENTLET') == '0':
        raise ImportError
    import eventlet
    if getattr(eventlet, 'disabled', False):
        raise ImportError

    from eventlet import sleep, spawn, spawn_after
    from eventlet import GreenPool as Pool
    from eventlet import connect, listen
    from eventlet.event import Event as _Event
    from eventlet.green import socket, subprocess
    from eventlet.hubs import trampoline
    from eventlet.timeout import Timeout as Timeout

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

    class Event(_Event):
        def wait(self, timeout=None):
            if timeout:
                with Timeout(timeout):
                    return super(Event, self).wait()
            else:
                return super(Event, self).wait()

except ImportError:
    import socket
    import sys
    import subprocess
    import threading
    import traceback

    from time import sleep as _sleep
    from multiprocessing.pool import ThreadPool
    from threading import RLock as ThreadLock
    from weakref import WeakKeyDictionary

    def sleep(seconds=0):
        return _sleep(seconds)

    class Pool(ThreadPool):
        def __init__(self, size):
            if not hasattr(threading.current_thread(), "_children"):
                threading.current_thread()._children = WeakKeyDictionary()
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

        def ready(self):
            return self._event.is_set()

        def send(self, value):
            self._value = value
            self._error = None
            return self._event.set()

        def send_exception(self, e, v=None, t=None):
            self._error = (e, v, t)
            return self._event.set()

        def wait(self, timeout=None):
            if not self._event.wait(timeout):
                raise Timeout()
            if self._error:
                (e, v, t) = self._error
                raise e, v, t
            else:
                return self._value

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

        def run(self):
            try:
                self.result = self.target(*self.args, **self.kwargs)
            except:
                self.result = sys.exc_info()
                self._print_exc()

    class Timeout(BaseException):
        pass

    def spawn(func, *args, **kwargs):
        t = Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    def spawn_after(t, func):
        t = threading.Timer(t, func)
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
