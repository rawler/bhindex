try:
    import eventlet
    if getattr(eventlet, 'disabled', False):
        raise ImportError

    from eventlet.event import Event
    from eventlet import sleep, spawn, spawn_after

    def cede():
        sleep()

except ImportError:
    import threading
    from time import sleep

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

    def spawn(func, *args, **kwargs):
        t = threading.Thread(target=func, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return t

    def spawn_after(time, func):
        t = threading.Timer(time, func)
        t.start()
        return t

