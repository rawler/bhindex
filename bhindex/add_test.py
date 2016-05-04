from nose.tools import *

from mock import MagicMock

from . import add
from .tree import Filesystem, Path as P
from distdb import DB, ValueSet
from bithorde import Identifiers, proto

hashIds1 = [proto.Identifier(type=proto.TREE_TIGER, id="0123456789abcdef")]
hashIds2 = [proto.Identifier(type=proto.TREE_TIGER, id="abcdef0123456789")]


def test_add_success():
    dummy_uploader = MagicMock(return_value=hashIds1)

    fs = Filesystem(DB(':memory:'))

    adder = add.AddController(fs, dummy_uploader)
    uploaded = adder("Movies/midsummer.mp4", mtime=0, t=10)
    dummy_uploader.assert_called_once_with("Movies/midsummer.mp4")

    f = fs.lookup(P("Movies/midsummer.mp4"))
    assert_equal(f, uploaded)
    assert_equal(uploaded.ids(), Identifiers([u'tree:tiger:GAYTEMZUGU3DOOBZMFRGGZDFMY']))
    assert_regexp_matches(uploaded.obj.any(u'directory'), 'dir:[^/]+/midsummer.mp4')
    assert_equal(adder.added, set([("Movies/midsummer.mp4", uploaded)]))

    return fs


def test_add_exists_without_force():
    fs = test_add_success()
    dummy_uploader = MagicMock(return_value=hashIds2)

    adder = add.AddController(fs, dummy_uploader)
    with assert_raises(add.FileExistsError):
        adder("Movies/midsummer.mp4", mtime=0)
    dummy_uploader.assert_not_called()
    assert_equal(adder.added, set())


def test_add_exists_with_force():
    fs = test_add_success()
    old_obj = fs.lookup(P("Movies/midsummer.mp4")).obj

    dummy_uploader = MagicMock(return_value=hashIds2)

    adder = add.AddController(fs, dummy_uploader)
    uploaded = adder("Movies/midsummer.mp4", mtime=0, force=True)
    dummy_uploader.assert_called_once_with("Movies/midsummer.mp4")

    f = fs.lookup(P("Movies/midsummer.mp4"))
    assert_equal(f, uploaded)
    assert_equal(adder.added, set([("Movies/midsummer.mp4", uploaded)]))

    assert_not_equal(old_obj.id, uploaded.obj.id)
    assert_equal(len(uploaded.obj[u'directory']), 1)
    assert_regexp_matches(uploaded.obj.any(u'directory'), 'dir:[^/]+/midsummer.mp4')
