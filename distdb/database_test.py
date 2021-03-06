from tempfile import mkdtemp
from shutil import rmtree
from os import path
from time import time

from nose.tools import *
from distdb.obj import TimedValues, Set, Object
from distdb.database import DB
from distdb.query import Key

HOURS = 3600


def future(n, unit):
    return time() + n * unit


class TempDir:
    def __init__(self):
        self.name = mkdtemp()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        rmtree(self.name)


def test_DB_file_backed():
    with TempDir() as d:
        db_path = path.join(d.name, 'db')

        db1 = DB(db_path)
        assert_true(path.exists)
        obj1 = db1.get('aia')
        assert_is_not_none(path.exists)
        obj1[u'name'] = u'Test Person'
        with db1.transaction() as t:
            t.update(obj1)
            assert_is_not_none(db1['aia'][u'name'])

        db2 = DB(db_path)
        obj2 = db2[u'aia']
        assert_equal(obj1[u'name'], obj2[u'name'])


class TestInRam():
    def setup(self):
        self.db = db = DB(':memory:')
        self.o = o = db.get('some_id')
        o[u'key'] = TimedValues([u'Test Person', u'And alternatives'], t=1)
        with self.db.transaction() as t:
            t.update(o)

    def test_simple_object_put_get(self):
        assert_is_not_none(self.o)
        self.o[u'name'] = Set(u'Test Person')
        with self.db.transaction() as t:
            t.update(self.o)
        assert_equal(self.db[self.o.id], self.o)

    def test_future_object_put_get(self):
        assert_is_not_none(self.o)
        self.o.set(u'name', u'Future Person', future(100, HOURS))
        with self.db.transaction() as t:
            t.update(self.o)
        assert_equal(self.db[self.o.id], self.o)

    def test_get_with_fields(self):
        o = self.db.get('some_id', fields="noexisting")
        assert_not_in(u'key', o)
        assert_not_in(u'noexisting', o)

    def test_failed_transaction(self):
        try:
            with self.db.transaction() as t:
                self.o[u'name'] = Set(u'Test Person')
                t.update(self.o)
                raise KeyError
        except:
            pass
        assert_not_in(u'name', self.db[self.o.id])

    def test_success_transaciton(self):
        with self.db.transaction() as t:
            self.o[u'name'] = Set(u'Test Person')
            t.update(self.o)
        assert_in(u'name', self.db[self.o.id])

    def test_query(self):
        assert_not_in(self.o, self.db.query(Key('key') == 'Not here'))
        assert_not_in(self.o, self.db.query(Key('nokey').any()))
        assert_in(self.o, list(self.db.query(Key('key') == u'Test Person')))
        assert_in(self.o, list(self.db.query(Key('apa').missing())))
        assert_in(self.o, list(self.db.query(Key('key').startswith('Test'))))

    def test_query_keyed(self):
        p1 = self.db.get("some_id")
        with self.db.transaction() as t:
            p2 = t.update(Object("other_id", init={u"key": TimedValues(u"Other Person", t=1)}))
            p3 = t.update(Object("3d_id", init={u"key": TimedValues(u"First Person", t=1)}))

        assert_equals(list(self.db.query_keyed({}, "+key")), [
            (u"And alternatives", p1),
            (u"First Person", p3),
            (u"Other Person", p2),
            (u"Test Person", p1),
        ])

    def test_update_empty(self):
        self.o[u'key'] = Set([])
        with self.db.transaction() as t:
            t.update(self.o)
        o = self.db.get(self.o.id)
        assert_not_in(u'key', o)

    def test_update_with_same_t(self):
        self.o[u'key'] = Set([u'Something completely differrent'], t=1)
        with self.db.transaction() as t:
            t.update(self.o)
        assert_equal(self.db[self.o.id][u'key'], Set([u'Something completely differrent'], t=1))

    def test_update_attr(self):
        db, o = self.db, self.o
        with self.db.transaction() as t:
            t.update_attr(o.id, 'key', TimedValues([], t=1))
            assert_equal(db.get(o.id), o)
            t.update_attr(o.id, 'key', [u'apa'])
            assert_equal(db.get(o.id)[u'key'], {u'apa'})
            t.update_attr(o.id, 'key', TimedValues(o[u'key']))
            assert_equal(db.get(o.id)[u'key'], o[u'key'])
            t.update_attr(o.id, 'key', TimedValues(u"future", future(100, HOURS)))
            assert_equal(db.get(o.id)[u'key'], set((u"future",)))

    def test_del_attr(self):
        db, o1 = self.db, self.o
        with self.db.transaction() as t:
            t.update_attr(o1.id, 'deleted', TimedValues([u'apa'], t=1))

        with self.db.transaction() as t:
            o2 = db.get(o1.id)
            assert_equal(o2[u'deleted'], Set([u'apa']))
            del o2[u'deleted']
            t.update(o2)
            assert_equal(db.get(o1.id), o1)

    def test_del_obj(self):
        db, o = self.db, self.o

        with self.db.transaction() as t:
            t.delete(o)
        assert_equal(db[o.id], Object(o.id))
        assert_equal(list(db.query(Key("key").any())), [])

    def test_vacuum(self):
        db, o = self.db, self.o
        with self.db.transaction() as t:
            t.delete(o)
        assert_is_not_none(db._get_list_id(o[u'key']))
        assert_is_not_none(db._get_id('key', u'key'))
        assert_is_not_none(db._get_id('obj', o.id))
        db.vacuum()
        assert_is_none(db._get_list_id(self.o[u'key']))
        assert_is_not_none(db._get_id('key', u'key'))
        assert_is_not_none(db._get_id('obj', o.id))
        db.vacuum(0)
        assert_is_none(db._get_list_id(self.o[u'key']))
        assert_is_not_none(db._get_id('key', u'key'))
        assert_is_not_none(db._get_id('obj', o.id))

    def test_get_public_mappings_after(self):
        items = self.db.get_public_mappings_after()
        item = next((x for x in items if x[0] == u'some_id'), None)
        assert_is_not_none(item)
        item_serial = item[3]
        assert_equal(self.db.last_serial(), item_serial)
        assert_false(list(self.db.get_public_mappings_after(item_serial)))
        with self.db.transaction() as t:
            t.delete(self.o)
        items = self.db.get_public_mappings_after(item_serial)
        del_item = next((x for x in items if x[0] == u'some_id'), None)
        assert_equal(del_item[0], item[0])
        assert_equal(del_item[1], item[1])
        assert_equal(del_item[3], self.db.last_serial())
        assert_equal(del_item[4], set([]))

    def test_sync_state(self):
        assert_equal(self.db.get_sync_state('my_peer'), {"last_received": 0})
        self.db.set_sync_state('my_peer', 2)
        assert_equal(self.db.get_sync_state('my_peer'), {"last_received": 2})

    def test_select(self):
        assert_equal(
            self.db._select('objid').where(Key('key').any()).apply(),
            ("SELECT objid FROM map WHERE LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL", (1,))
        )

        assert_equal(
            self.db._select('objid').where(Key('NONE_EXISTING').any()).apply(),
            ("SELECT objid FROM map WHERE LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL", (2,))
        )
