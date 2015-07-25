from tempfile import mkdtemp
from shutil import rmtree
from os import path

from nose.tools import *
from db.obj import ValueSet, Object
from db.database import DB, Starts, ANY

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
        db1.update(obj1)
        assert_is_not_none(db1['aia'][u'name'])
        db1.commit()

        db2 = DB(db_path)
        obj2 = db2[u'aia']
        assert_equal(obj1[u'name'], obj2[u'name'])

class TestInRam():
    def setup(self):
        self.db = db = DB(':memory:')
        self.o = o = db.get('some_id')
        o[u'key'] = ValueSet([u'Test Person', u'And alternatives'])
        db.update(o)

    def test_simple_object_put_get(self):
        assert_is_not_none(self.o)
        self.o[u'name'] = ValueSet(u'Test Person')
        self.db.update(self.o)
        assert_equal(self.db[self.o.id], self.o)

    def test_get_with_fields(self):
        o = self.db.get('some_id', fields="noexisting")
        assert_not_in(u'key', o)
        assert_not_in(u'noexisting', o)

    def test_failed_transaction(self):
        try:
            with self.db.transaction():
                self.o[u'name'] = ValueSet(u'Test Person')
                self.db.update(self.o)
                raise KeyError
        except:
            pass
        assert_not_in(u'name', self.db[self.o.id])

    def test_success_transaciton(self):
        with self.db.transaction():
            self.o[u'name'] = ValueSet(u'Test Person')
            self.db.update(self.o)
        assert_in(u'name', self.db[self.o.id])

    def test_query(self):
        assert_not_in(self.o, self.db.query({u'key': 'Not here'}))
        assert_not_in(self.o, self.db.query({u'nokey': ANY}))
        assert_in(self.o, list(self.db.query({u'key': u'Test Person'})))
        assert_in(self.o, list(self.db.query({u'apa': None})))
        assert_in(self.o, list(self.db.query({u'key': Starts('Test')})))

    def test_query_objids(self):
        assert_not_in(self.o.id, self.db.query_ids({u'key': 'Not here'}))
        assert_in(self.o.id, list(self.db.query_ids({u'key': u'Test Person'})))
        assert_in(self.o.id, list(self.db.query_ids({u'apa': None})))
        assert_in(self.o.id, list(self.db.query_ids({u'key': Starts('Test')})))

    def test_update_empty(self):
        self.o[u'key'] = ValueSet([])
        self.db.update(self.o)
        o = self.db.get(self.o.id)
        assert_false(o[u'key'])

    def test_update_attr(self):
        db, o = self.db, self.o
        db.update_attr(o.id, 'key', ValueSet([], t=0))
        assert_equal(db.get(o.id), o)
        db.update_attr(o.id, 'key', ValueSet([u'apa']))
        assert_equal(db.get(o.id)[u'key'], ValueSet([u'apa']))
        db.update_attr(o.id, 'key', ValueSet(o[u'key']))
        assert_equal(db.get(o.id)[u'key'], o[u'key'])

    def test_vacuum(self):
        db, o = self.db, self.o
        db.update_attr(o.id, 'key', ValueSet([u'key']))
        assert_is_not_none(db._get_list_id(o[u'key']))
        db.vacuum()
        assert_is_none(db._get_list_id(self.o[u'key']))


    def test_get_public_mappings_after(self):
        items = self.db.get_public_mappings_after()
        item = next((x for x in items if x[0] == u'some_id'), None)
        assert_is_not_none(item)
        assert_equal(self.db.last_serial(), item[3])
        assert_false(list(self.db.get_public_mappings_after(item[3])))

    def test_sync_state(self):
        assert_equal(self.db.get_sync_state('my_peer'), {"last_received": 0})
        self.db.set_sync_state('my_peer', 2)
        assert_equal(self.db.get_sync_state('my_peer'), {"last_received": 2})


