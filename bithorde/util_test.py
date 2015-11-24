from nose.tools import *
from StringIO import StringIO

from .util import *


def test_b32decode():
    assert_equal(b32decode(""), "")
    assert_equal(b32decode("JBSWUII="), "Hej!")
    assert_equal(b32decode("JBSWUII"), "Hej!")
    assert_equal(b32decode("JbsWUII"), "Hej!")


def test_fsize():
    buf = StringIO("APA")
    buf.seek(1)
    assert_equal(fsize(buf), 3)
    assert_equal(buf.tell(), 1)


def test_read_in_chunks():
    count = 0
    for chunk in read_in_chunks(StringIO("AP"*21), 8):
        if count < 5:
            assert_equal(chunk, "APAPAPAP")
        else:
            assert_equal(chunk, "AP")
        count += 1
    assert_equal(count, 6)
