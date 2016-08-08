from mock import MagicMock

import os

from .util import *
import concurrent

def test_DelayedAction():
    ctr = MagicMock()
    a = DelayedAction(ctr)
    a.schedule(0.05)
    a.schedule(0.5)
    concurrent.sleep(0.1)
    ctr.assert_called_once_with()
