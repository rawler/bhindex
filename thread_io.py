from multiprocessing.pool import ThreadPool
from sys import exc_info as _exc_info
from threading import Thread as _Thread, Event as _Event
from time import sleep
from traceback import print_exception as _print_exception
import socket as _socket


class Thread(_Thread):
        def __init__(self, target, args, kwargs, *_args, **_kwargs):
            self.target = target
            self.args = args
            self.kwargs = kwargs
            super(Thread, self).__init__(*_args, target=target, args=args, kwargs=kwargs, **_kwargs)

        def wait(self):
            self.join()
            return self.result

        def run(self):
            try:
                self.result = self.target(*self.args, **self.kwargs)
            except:
                self.result = _exc_info()
                _print_exception(*self.result)


def spawn(func, *args, **kwargs):
    t = Thread(target=func, args=args, kwargs=kwargs)
    t.daemon = True
    t.start()
    return t


class Promise(object):
    def __init__(self):
        self._event = _Event()

    def get(self, default=None):
        try:
            return self._result
        except AttributeError:
            return default

    def set(self, result):
        self._result = result
        self._event.set()

    def wait(self, timeout=None):
        if self._event.wait(timeout):
            return self._result
        else:
            raise Timeout()


class Timeout(Exception):
    pass


class _listen_socket(_socket.socket):
        def __init__(self, *args, **kwargs):
            self.accepting = False
            super(_listen_socket, self).__init__(*args, **kwargs)

        def listen(self, backlog):
            self.accepting = True
            super(_listen_socket, self).listen(backlog)

        def close(self):
            if self.accepting:
                self.accepting = False
                try:
                    connect(self.getsockname())
                except:
                    pass
            super(_listen_socket, self).close


def serve(sock, handler):
    try:
        while sock.accepting:
            conn, addr = sock.accept()
            if sock.accepting:
                spawn(handler, conn, addr)
    except _socket.error:
        pass


def connect(address, family=_socket.AF_INET, bind=None):
        if family in (_socket.AF_INET, _socket.AF_INET6, _socket.AF_UNIX):
            s = _socket.socket(family, _socket.SOCK_STREAM)
            if bind:
                s.bind(bind)
            s.connect(address)
            return s
        else:
            raise ValueError("Unknown address type")


def listen(address, backlog=5):
    s = _listen_socket(_socket.AF_INET, _socket.SOCK_STREAM)
    s.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    s.bind(address)
    s.listen(backlog)
    return s
