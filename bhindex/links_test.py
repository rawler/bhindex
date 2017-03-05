# -*- coding: utf-8 -*-

from tempfile import mkdtemp
from shutil import rmtree
from os import path

from . import links
import itertools
from nose.tools import *
from mock import Mock, MagicMock

from distdb import DB, Object
from distdb.obj import TimedValues
from bithorde import proto


class TempDir(object):
    def __init__(self):
        self.name = mkdtemp()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        rmtree(self.name)

    def assert_exists(self, p):
        assert path.exists(path.join(self.name, p))

    def assert_missing(self, p):
        assert not path.exists(path.join(self.name, p))

    def assert_points_to(self, p, tgt):
        p = path.realpath(path.join(self.name, p))
        tgt = path.realpath(path.join(self.name, tgt))
        assert_equal(p, tgt)


def test_LinksWriter():
    with TempDir() as d:
        w = links.LinksWriter(d.name, "/tmp/bhfuse")
        d.assert_missing("some/path")
        w("some/path", "magnet:?urn=something")
        d.assert_missing("some/path")
        w("some/åäö/path", "magnet:?urn=something")
        d.assert_points_to("some/åäö/path", "/tmp/bhfuse/magnet:?urn=something")
        w("some/path", "magnet:?urn=something")
        d.assert_points_to("some/path", "/tmp/bhfuse/magnet:?urn=something")
        w("some/path", "magnet:?urn=something_else")
        d.assert_points_to(
            "some/path", "/tmp/bhfuse/magnet:?urn=something_else")


def test_FilteredExporter():
    export = MagicMock()

    fe = links.FilteredExporter(export, ["tmp/apa"])
    fe("urk/apa", "1")
    fe("tmp/apa/file", "2")
    export.assert_called_once_with("tmp/apa/file", "2")


def test_DBExporter():
    path = u'apa/movie'
    xt = u'tree:tiger:ASDASDSADASDASDSADASDASDSADASDASDSADASD'
    db = DB(':memory:')
    with db.transaction() as t:
        t.update(Object(u"dir:apa", {
            u'directory': TimedValues(u"dir:/apa"),
        }))
        t.update(Object('some_file', {
            u'directory': TimedValues(u"dir:apa/movie"),
            u'xt': TimedValues(xt),
        }))

    asset = Mock()
    asset.__enter__ = Mock(return_value=asset)
    asset.__exit__ = Mock(return_value=False)
    asset.status = Mock(
        return_value=proto.AssetStatus(status=proto.SUCCESS))

    bithorde = Mock()
    bithorde.open = Mock(return_value=asset)
    bithorde.pool = Mock(return_value=itertools)

    writer = Mock()

    magnet = u'magnet:?xt=urn:' + xt
    exp = links.DBExporter(db, bithorde, writer)
    exp.export(False)
    writer.assert_called_once_with(path, magnet)
    writer.reset_mock()

    exp.export(False)
    writer.assert_not_called()

    exp.export(True)
    writer.assert_called_once_with(path, magnet)
