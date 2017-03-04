from tempfile import mkdtemp
from shutil import rmtree
from os import path

from nose.tools import *
from distdb.obj import ValueSet, Object
from distdb.database import ANY, DB, Starts


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
        obj1[u'name'] = ValueSet(u'Test Person')
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
        o[u'key'] = ValueSet([u'Test Person', u'And alternatives'], t=1)
        with self.db.transaction() as t:
            t.update(o)

    def test_simple_object_put_get(self):
        assert_is_not_none(self.o)
        self.o[u'name'] = ValueSet(u'Test Person')
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
                self.o[u'name'] = ValueSet(u'Test Person')
                t.update(self.o)
                raise KeyError
        except:
            pass
        assert_not_in(u'name', self.db[self.o.id])

    def test_success_transaciton(self):
        with self.db.transaction() as t:
            self.o[u'name'] = ValueSet(u'Test Person')
            t.update(self.o)
        assert_in(u'name', self.db[self.o.id])

    def test_query(self):
        assert_not_in(self.o, self.db.query({u'key': 'Not here'}))
        assert_not_in(self.o, self.db.query({u'nokey': ANY}))
        assert_in(self.o, list(self.db.query({u'key': u'Test Person'})))
        assert_in(self.o, list(self.db.query({u'apa': None})))
        assert_in(self.o, list(self.db.query({u'key': Starts('Test')})))

    def test_query_keyed(self):
        p1 = self.db.get("some_id")
        with self.db.transaction() as t:
            p2 = t.update(Object("other_id", init={u"key": ValueSet(u"Other Person", t=1)}))
            p3 = t.update(Object("3d_id", init={u"key": ValueSet(u"First Person", t=1)}))

        assert_equals(list(self.db.query_keyed({}, "+key")), [
            (u"And alternatives", p1),
            (u"First Person", p3),
            (u"Other Person", p2),
            (u"Test Person", p1),
        ])

    def test_query_objids(self):
        assert_not_in(self.o.id, self.db.query_ids({u'key': 'Not here'}))
        assert_in(self.o.id, list(self.db.query_ids({u'key': u'Test Person'})))
        assert_in(self.o.id, list(self.db.query_ids({u'apa': None})))
        assert_in(self.o.id, list(self.db.query_ids({u'key': Starts('Test')})))

    def test_update_empty(self):
        self.o[u'key'] = ValueSet([])
        with self.db.transaction() as t:
            t.update(self.o)
        o = self.db.get(self.o.id)
        assert_not_in(u'key', o)

    def test_update_with_same_t(self):
        self.o[u'key'] = ValueSet([u'Something completely differrent'], t=1)
        with self.db.transaction() as t:
            t.update(self.o)
        assert_equal(self.db[self.o.id][u'key'], ValueSet([u'Something completely differrent'], t=1))

    def test_update_attr(self):
        db, o = self.db, self.o
        with self.db.transaction() as t:
            t.update_attr(o.id, 'key', ValueSet([], t=1))
            assert_equal(db.get(o.id), o)
            t.update_attr(o.id, 'key', ValueSet([u'apa']))
            assert_equal(db.get(o.id)[u'key'], ValueSet([u'apa']))
            t.update_attr(o.id, 'key', ValueSet(o[u'key']))
            assert_equal(db.get(o.id)[u'key'], o[u'key'])

    def test_del_attr(self):
        db, o1 = self.db, self.o
        with self.db.transaction() as t:
            t.update_attr(o1.id, 'deleted', ValueSet([u'apa'], t=1))

        with self.db.transaction() as t:
            o2 = db.get(o1.id)
            assert_equal(o2[u'deleted'], ValueSet([u'apa']))
            del o2[u'deleted']
            t.update(o2)
            assert_equal(db.get(o1.id), o1)

    def test_del_obj(self):
        db, o = self.db, self.o

        with self.db.transaction() as t:
            t.delete(o)
        assert_equal(db[o.id], Object(o.id))
        assert_equal(list(db.query({"key": ANY})), [])

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
