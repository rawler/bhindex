from util import *
import concurrent

from mock import MagicMock

def test_DelayedAction():
    def test():
        ctr = MagicMock()
        a = DelayedAction(ctr.inc)
        a.schedule(0.00)
        a.schedule(0.00)
        concurrent.sleep(0.000)
        ctr.assert_called_once()

    test()
    try:
        import eventlet
        eventlet.disabled = True
        reload(concurrent)
        test()
    except ImportError:
        pass