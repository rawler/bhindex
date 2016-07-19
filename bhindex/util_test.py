from mock import MagicMock

import os
from nose.tools import *

import concurrent
from distdb import Object, ValueSet
from .util import *
from .util import _object_availability


def test__object_availability():
    assert_equal(_object_availability(Object('qwe123', {
    }), 1500), (None, None))

    assert_equal(_object_availability(Object('qwe123', {
        'bh_status': ValueSet((u'False',), 1000),
        'bh_status_confirmed': ValueSet((), 1500),
    }), 2000), (-500, 500))

    assert_equal(_object_availability(Object('qwe123', {
        'bh_status': ValueSet((u'True',), 1000),
        'bh_status_confirmed': ValueSet((), 1500),
    }), 2000), (500, 500))

    assert_equal(_object_availability(Object('qwe123', {
        'bh_availability': ValueSet(u'384', 1000),
        'bh_status': ValueSet((u'True',), 1000),
        'bh_status_confirmed': ValueSet((), 1500),
    }), 2000), (384, 1000))


def test_calc_new_availability():
    elapsed = 500
    factored_elapsed = elapsed * ASSET_WAIT_FACTOR
    availability = 800
    assert_equal(calc_new_availability(True, availability, elapsed), availability + factored_elapsed)
    assert_equal(calc_new_availability(False, availability, elapsed), -factored_elapsed)
    assert_equal(calc_new_availability(True, -availability, elapsed), factored_elapsed)
    assert_equal(calc_new_availability(False, -availability, elapsed), -availability - factored_elapsed)
    assert_equal(calc_new_availability(True, None, None), 36)
    assert_equal(calc_new_availability(False, None, None), -36)


def test_DelayedAction():
    ctr = MagicMock()
    a = DelayedAction(ctr)
    a.schedule(0.05)
    a.schedule(0.5)
    concurrent.sleep(0.1)
    ctr.assert_called_once_with()
