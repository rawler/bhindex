from mock import MagicMock

from nose.tools import *

from distdb import Object
from distdb.obj import TimedValues
from .util import *
from .util import _object_availability, _checkAsset


def test__object_availability():
    assert_equal(_object_availability(Object('qwe123', {
    })), (0, 0))

    assert_equal(_object_availability(Object('qwe123', {
        'bh_availability': TimedValues(u'384', 1000),
        'bh_status': TimedValues((u'True',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    })), (384, 1000))


def test_hasValidStatus():
    assert_equal(hasValidStatus(Object('qwe123', {
    }), 1500), None)

    assert_equal(hasValidStatus(Object('qwe123', {
        'bh_availability': TimedValues(u'10000', 2500),
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
        'bh_availability': TimedValues((u'700',), 1900),
        'bh_status': TimedValues((u'False',), 1000),
        'bh_status_confirmed': TimedValues((), 1500),
    })
    expected_score = float(asset.any('bh_availability')) + AVAILABILITY_BASE_CHANGE
    expected_valid_until = (t + expected_score) * UNCHANGED_WAIT_FACTOR
    _checkAsset(MockBithorde(message.SUCCESS), asset, t)
    assert_equal(asset.getitem('bh_availability'), TimedValues((unicode(expected_score),), expected_valid_until))
    assert_not_in('bh_status', asset)
    assert_not_in('bh_status_confirmed', asset)


def test_calc_new_availability():
    availability = 800
    assert_equal(
        calc_new_availability(True, availability),
        (
            availability + AVAILABILITY_BASE_CHANGE,
            (availability + AVAILABILITY_BASE_CHANGE) * UNCHANGED_WAIT_FACTOR,
        )
    )
    assert_equal(
        calc_new_availability(False, availability),
        (
            (availability * AVAILABILITY_ALTENATOR) - AVAILABILITY_BASE_CHANGE,
            CHANGE_WAIT_VALUE,
        )
    )
    assert_equal(
        calc_new_availability(True, -availability),
        (
            AVAILABILITY_BASE_CHANGE,
            CHANGE_WAIT_VALUE,
        )
    )
    assert_equal(
        calc_new_availability(False, -availability),
        (
            -availability - AVAILABILITY_BASE_CHANGE,
            abs(-availability - AVAILABILITY_BASE_CHANGE) * UNCHANGED_WAIT_FACTOR,
        )
    )
    assert_equal(calc_new_availability(True, None), (600, 60))
    assert_equal(calc_new_availability(False, None), (-600, 60))
