import concurrent

from nose.tools import *


def test_Event_Timeout():
    event = concurrent.Event()
    with assert_raises(concurrent.Timeout):
        event.wait(0.2)
