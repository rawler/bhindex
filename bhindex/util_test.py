from mock import MagicMock

import os

from .util import *


def test_DelayedAction():
    def test():
        ctr = MagicMock()
        a = DelayedAction(ctr)
        a.schedule(0.05)
        a.schedule(0.5)
        concurrent.sleep(0.1)
        ctr.assert_called_once_with()

    os.environ['CONCURRENT_EVENTLET'] = '0'
    import concurrent
    test()

    try:
        del os.environ['CONCURRENT_EVENTLET']
        reload(concurrent)
        test()
    except ImportError:
        pass
