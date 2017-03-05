from mock import MagicMock

from nose.tools import *

import concurrent
from distdb import Object
from distdb.obj import TimedValues
from .util import *
from .util import _object_availability, _checkAsset


def test__object_availability():
    assert_equal(_object_availability(Object('qwe123', {
    }), 1500), (0, 0))

    assert_equal(_object_availability(Object('qwe123', {
        'bh_status': TimedValues((u'False',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    }), 2000), (-500, 500))

    assert_equal(_object_availability(Object('qwe123', {
        'bh_status': TimedValues((u'True',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    }), 2000), (500, 500))

    assert_equal(_object_availability(Object('qwe123', {
        'bh_availability': TimedValues(u'384', 1000),
        'bh_status': TimedValues((u'True',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    }), 2000), (384, 1000))


def test_hasValidStatus():
    assert_equal(hasValidStatus(Object('qwe123', {
    }), 1500), None)

    assert_equal(hasValidStatus(Object('qwe123', {
        'bh_status': TimedValues((u'True',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    }), 2000), None)

    assert_equal(hasValidStatus(Object('qwe123', {
        'bh_status': TimedValues((u'False',), 1000),
        'bh_status_confirmed': TimedValues((), 2980),
    }), 2000), False)

    assert_equal(hasValidStatus(Object('qwe123', {
        'bh_status': TimedValues((u'True',), 1000),
        'bh_status_confirmed': TimedValues((), 2980),
    }), 2000), True)

    assert_equal(hasValidStatus(Object('qwe123', {
        'bh_availability': TimedValues(u'10000', 1500),
        'bh_status': TimedValues((u'True',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    }), 2000), True)


class MockBithorde(object):
    def __init__(self, status):
        self._status = status

    def open(self, ids):
        return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def status(self):
        x = MagicMock()
        x.status = self._status
        x.size = 1024 * 1024
        return x


def test__checkAsset():
    t = 2000
    asset = Object('qwe123', {
        'xt': TimedValues(u'tree:tiger:5CN2KZAUNFUPOVI3DP4KLHTZ2POBOYMCULZCE5A', 1000),
        'bh_status': TimedValues((u'False',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    })
    _checkAsset(MockBithorde(message.SUCCESS), asset, t)
    assert_equal(asset['bh_availability'], TimedValues((unicode(500 * AVAILABILITY_BONUS),), 2000))
    assert_not_in('bh_status', asset)
    assert_not_in('bh_status_confirmed', asset)


def test_calc_new_availability():
    elapsed = 500
    factored_elapsed = elapsed * AVAILABILITY_BONUS
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
