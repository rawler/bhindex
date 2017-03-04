# -*- coding: utf8 -*-

from nose.tools import *
from warnings import catch_warnings

from .tree import *
from distdb import DB, Object, ValueSet

P = Path


def fz(*items):
    return frozenset(items)


def ids_set(x):
    if hasattr(x, 'obj'):
        return x.obj.id
    else:
        return frozenset(a.id for a in x.objs)


def name_type_ids_set(l):
    return frozenset((name, type(obj), ids_set(obj)) for name, obj in l)


def test_Path():
    assert_equals(Path("/apa/apa"), ("apa", "apa"))
    assert_equals(Path("/apa/apa"), Path([u"apa", u"apa"]))
    assert_equals(Path("/apa/apa"), Path(" apa/apa"))
    assert_equals(Path("/åpa/åpa"), Path([u"åpa", u"åpa"]))


class TestFilesystem(object):
    def setup(self):
        xt = u'tree:tiger:ASDASDSADASDASDSADASDASDSADASDASDSADASD'
        db = self.db = DB(':memory:')
        with db.transaction() as t:
            self.d = t.update(Object(u"dir:some/dir", {
                u'directory': ValueSet(u"dir:/apa", 0),
            }))
            self.d2 = t.update(Object(u"dir:redundant", {
                u'directory': ValueSet(u"dir:/apa", 0),
            }))
            self.f = t.update(Object('some_file', {
                u'directory': ValueSet(u"dir:some/dir/file.ext", 0),
                u'xt': ValueSet(xt),
            }))
        self.fs = Filesystem(db)

    def test_paths_for(self):
        assert_set_equal(self.fs.paths_for(self.d), fz((u"apa",)))
        assert_set_equal(self.fs.paths_for(self.d2), fz((u"apa",)))
        assert_set_equal(self.fs.paths_for(self.f), fz((u"apa", u"file.ext")))

    def test_broken_direntry(self):
        with self.fs.transaction() as t:
            broken = [
                t.update(Object(u"dir:broken_dir", {
                    u'directory': ValueSet(u"broken_shit1", 0),
                })),
                t.update(Object(u"dir:broken_dir", {
                    u'directory': ValueSet(u"broken_shit2/", 0),
                })),
                t.update(Object(u"dir:broken_dir", {
                    u'directory': ValueSet(u"/broken_shit4", 0),
                })),
            ]

        for o in broken:
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

        assert_set_equal(name_type_ids_set(files), fz(
            ('apa', Directory, fz(u'dir:some/dir', u'dir:redundant'))))
        assert_set_equal(
            name_type_ids_set(files[0][1]), fz(('file.ext', File, u'some_file')))

    def test_rm(self):
        assert_is_instance(self.fs.lookup(['apa', 'file.ext']), File)
        self.fs.root()['apa'].rm('file.ext')
        with assert_raises(NotFoundError):
            self.fs.lookup(['apa', 'file.ext'])

    def test_mv(self):
        def assert_same_file(path, ref_file):
            f = self.fs.lookup(path)
            assert_equals(f.ids(), self.f['xt'])
        assert_same_file(P("apa/file.ext"), self.f)
        self.fs.mv(P("apa"), P("banan"), t=1)
        with assert_raises(NotFoundError):
            self.fs.lookup(P("apa"))
        assert_same_file(P("banan/file.ext"), self.f)
        self.fs.mv(P("banan/file.ext"), P("banan/other_file"), t=2)
        assert_same_file(P("banan/other_file"), self.f)
        self.fs.mv(P("banan/other_file"), P("apa/file.ext"), t=3)
        assert_same_file(P("apa/file.ext"), self.f)

    def test_colliding_file(self):
        with self.fs.transaction() as t:
            f2 = t.update(Object('some_file_colliding_dir', {
                u'directory': ValueSet(u"dir:some/dir/file.ext", 0),
            }))

        assert_set_equal(name_type_ids_set(self.fs.lookup(P('apa'))), fz(
            ('file.ext', Split, fz(u'some_file', f2.id)),
        ))
        assert_set_equal(name_type_ids_set(self.fs.lookup(P('apa/file.ext'))), fz(
            (u'some_file.ext', File, u'some_file'),
            (u'some_file_colliding_dir.ext', Directory,
             fz(u'some_file_colliding_dir', )),
        ))

        self.fs.mv(P('apa/file.ext'), P('banan/file'))

        assert_set_equal(name_type_ids_set(self.fs.lookup(P('banan'))), fz(
            ('file', Split, fz(u'some_file', f2.id)),
        ))
        assert_set_equal(name_type_ids_set(self.fs.lookup(P('banan/file'))), fz(
            (u'some_file', File, u'some_file'),
            (u'some_file_colliding_dir', Directory,
             fz(u'some_file_colliding_dir', )),
        ))

        self.fs.mv(P('banan/file/some_file'), P('apa/file.ext'))
        assert_set_equal(name_type_ids_set(self.fs.lookup(P('apa'))), fz(
            (u'file.ext', File, u'some_file'),
        ))
        assert_set_equal(name_type_ids_set(self.fs.lookup(P('banan'))), fz(
            (u'file', Directory, fz(f2.id,)),
        ))
