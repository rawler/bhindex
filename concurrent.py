'''
Utility-module to wrap essential parts of the Evenlet-API.
Falls back to regular threads where eventlet is unavailable
'''

try:
    import eventlet
    if getattr(eventlet, 'disabled', False):
        raise ImportError

    from eventlet.event import Event
    from eventlet import sleep, spawn, spawn_after
    from eventlet import GreenPool as Pool
    from eventlet import connect, listen, serve
    from eventlet.green import socket
    from eventlet.hubs import trampoline

    def cede():
        sleep()

    class ThreadLock:
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_value, traceback):
            pass

except ImportError:
    import socket, threading
    from time import sleep

    from multiprocessing.pool import ThreadPool
    from socket import create_connection as connect
    from threading import RLock as ThreadLock

    class Pool(ThreadPool):
        def __init__(self, size):
            ThreadPool.__init__(self, size)
        def spawn(self, func, *args, **kwargs):
            self.apply_async(func, args, kwargs)

    class Event:
        def __init__(self):
            self._value = None
            self._error = None
            self._event = threading.sEvent()

        def send(self, value):
            self._value = value
            self._error = None
            return self._event.set()

        def send_exception(self, e, v=None, t=None):
            self._error = (e, v, t)
            return self._event.set()

        def wait(self, value):
            self._event.wait()
            if self._error:
                (e, v, t) = self._error
                raise e, v, t
            else:
                return self._value

    def cede():
        pass # Not needed for real threads

    class Thread(threading.Thread):
        def wait(self):
            return self.join()

    def spawn(func, *args, **kwargs):
        t = Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    def spawn_after(time, func):
        t = threading.Timer(time, func)
        t.start()
        return t

    def listen(address):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(address)
        s.listen(5)
        return s

    def serve(sock, handler):
        while True:
            conn, addr = sock.accept()
            spawn(handler, conn, addr)

    def trampoline(fd, *args, **kwargs):
        'Not applicable without eventloop'
        pass
