# -*- coding: utf8 -*-

from nose.tools import *
from warnings import catch_warnings

from .tree import *

from db import DB, Object, ValueSet

P = Path


def fz(*items):
    return frozenset(items)


def ids_set(x):
    if hasattr(x, 'obj'):
        return x.obj.id
    else:
        return frozenset(a.id for a in x.objs)


def type_ids_set(l):
    return frozenset((type(n), ids_set(n)) for n in l)


def test_Path():
    assert_equals(Path("/apa/apa"), ("apa", "apa"))
    assert_equals(Path("/apa/apa"), Path([u"apa", u"apa"]))
    assert_equals(Path("/apa/apa"), Path(" apa/apa"))
    assert_equals(Path("/åpa/åpa"), Path([u"åpa", u"åpa"]))


class TestFilesystem(object):
    def setup(self):
        xt = u'tree:tiger:ASDASDSADASDASDSADASDASDSADASDASDSADASD'
        db = self.db = DB(':memory:')
        self.d = db.update(Object(u"dir:some/dir", {
            u'directory': ValueSet(u"dir:/apa", 0),
        }))
        self.d = db.update(Object(u"dir:redundant", {
            u'directory': ValueSet(u"dir:/apa", 0),
        }))
        self.f = db.update(Object('some_file', {
            u'directory': ValueSet(u"dir:some/dir/file", 0),
            u'xt': ValueSet(xt),
        }))
        self.fs = Filesystem(db)

    def test_paths_for(self):
        assert_set_equal(self.fs.paths_for(self.d), fz((u"apa",)))
        assert_set_equal(self.fs.paths_for(self.f), fz((u"apa", u"file")))

    def test_broken_direntry(self):
        broken = [
            self.db.update(Object(u"dir:broken_dir", {
                u'directory': ValueSet(u"broken_shit1", 0),
            })),
            self.db.update(Object(u"dir:broken_dir", {
                u'directory': ValueSet(u"broken_shit2/", 0),
            })),
            self.db.update(Object(u"dir:broken_dir", {
                u'directory': ValueSet(u"/broken_shit4", 0),
            })),
        ]

        for o in broken:
            print o
            with catch_warnings(True) as w:
                assert_equal(self.fs.paths_for(o), set())
                assert_equal(len(w), 1)

    def test_mkdir(self):
        self.fs.mkdir(P("Movies/Anime"))
        assert_is_instance(self.fs.lookup(P("Movies/Anime")), Directory)

    def test_root(self):
        assert_is_instance(self.fs.root(), Directory)

    def test_ls(self):
        files = list(self.fs.root().ls())

        assert_set_equal(type_ids_set(files), fz((Directory, fz(u'dir:some/dir', u'dir:redundant'))))
        assert_set_equal(type_ids_set(files[0]), fz((File, u'some_file')))

    def test_rm(self):
        assert_is_instance(self.fs.lookup(['apa', 'file']), File)
        self.fs.root()['apa'].rm('file')
        with assert_raises(NotFoundError):
            self.fs.lookup(['apa', 'file'])

    def test_mv(self):
        def assert_same_file(path, ref_file):
            assert_equals(self.fs.lookup(path).ids(), self.f['xt'])
        assert_same_file(P("apa/file"), self.f)
        self.fs.mv(P("apa"), P("banan"), t=1)
        with assert_raises(NotFoundError):
            self.fs.lookup(P("apa"))
        assert_same_file(P("banan/file"), self.f)
        self.fs.mv(P("banan/file"), P("banan/other_file"), t=2)
        assert_same_file(P("banan/other_file"), self.f)
        self.fs.mv(P("banan/other_file"), P("apa/file"), t=3)
        assert_same_file(P("apa/file"), self.f)
