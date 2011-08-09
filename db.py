# -*- coding: utf-8 -*-
from urlparse import urlparse, parse_qs
from urllib import quote_plus as urlquote
import cPickle as pickle
import time

import bithorde

import sqlite3

def path_str2lst(str):
    return [x for x in str.split("/") if x]

def path_lst2str(list):
    return "/".join(list)

ANY = object()

class ValueSet(set):
    def __init__(self, v=[], t=None):
        if isinstance(v, basestring):
            set.__init__(self, [v])
        else:
            set.__init__(self, v)
        self.t = t

    def any(self):
        for x in self:
            return x

class Object(dict):
    def __init__(self, objid, init={}):
        self.id = objid
        self.dirty = set()
        self.update(init)

    def _update(self, key, value):
        dict.__setitem__(self, key, value)

    def update(self, other):
        for k,v in other.iteritems():
            self[k] = v

    def __do_set(self, key, value):
        assert isinstance(key, unicode)
        for v in value:
            assert isinstance(v, unicode)

        if key not in self or self[key] != value:
            self.dirty.add(key)
            dict.__setitem__(self, key, value)

    def __setitem__(self, key, value):
        value = ValueSet(value)
        self.__do_set(key, value)

    def add(self, key, value):
        value = ValueSet(value)
        if key in self:
            value.update(self[key])
        self.__do_set(key, value)

    def timestamp(self):
        return max(x.t for x in self.itervalues())

def create_DB(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS map (
        objid TEXT NOT NULL,
        key TEXT NOT NULL,
        timestamp INT NOT NULL,
        listid INTEGER,
        PRIMARY KEY (objid, key)
    );
    CREATE INDEX IF NOT EXISTS map_obj ON map (objid);
    CREATE INDEX IF NOT EXISTS map_key ON map (key);
    CREATE INDEX IF NOT EXISTS map_list ON map (listid);

    CREATE TABLE IF NOT EXISTS list (
        itemid INTEGER PRIMARY_KEY AUTO_INCREMENT,
        id INTEGER NOT NULL,
        value TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS list_id ON list (id);
    CREATE INDEX IF NOT EXISTS list_value ON list (value);
    """)

class DB(object):
    def __init__(self, config):
        self.conn = sqlite3.connect(config.get('DB', 'file'))
        create_DB(self.conn)

    def _query_all(self, query, args):
        c = self.conn.cursor()
        c.execute(query, args)
        return c.fetchall()

    def _query_first(self, query, args):
        c = self.conn.cursor()
        c.execute(query, args)
        return c.fetchone()

    def _query_single(self, query, args=[], default=None):
        res = self._query_first(query, args)
        if res:
            return res[0]
        else:
            return default

    def __getitem__(self, objid):
        obj = Object(objid)
        for key, timestamp, listid in self._query_all("SELECT key, timestamp, listid FROM map WHERE objid = ?", (objid,)):
            values = ValueSet((x for x, in self._query_all("SELECT value FROM list WHERE id = ?", (listid,))), t=timestamp)
            obj._update(key, values)
        return obj

    def query(self, criteria):
        ids = None
        match_query = """SELECT DISTINCT objid FROM map JOIN list ON (map.listid = list.id)
                         WHERE map.key = ? AND list.value = ?"""
        any_query = """SELECT DISTINCT objid FROM map WHERE key = ?"""
        for k,v in criteria.iteritems():
            if v is ANY:
                objs = self._query_all(any_query, (k,))
            else:
                objs = self._query_all(match_query, (k,v))

            if ids is None:
                ids = set(x for x, in objs)
            else:
                ids.intersection_update(x for x, in objs)

        for objid in ids:
            yield self[objid]

    def all(self):
        for objid, in self._query_all('SELECT DISTINCT objid FROM map', ()):
            yield self[objid]

    def merge(self, obj):
        objid = obj.id
        for key in obj.dirty:
            newlistid = self._query_single("SELECT MAX(id)+1 FROM list") or 1
            c = self.conn.executemany("INSERT INTO list (id, value) VALUES (?, ?)", ((newlistid, value) for value in obj[key]))
            self.conn.execute("""INSERT OR REPLACE INTO map (objid, key, timestamp, listid)
                                VALUES (?, ?, strftime('%s','now'), ?)""", 
                                (objid, key, newlistid))
        obj.dirty.clear()

    def commit(self):
        self.conn.commit()

    def vacuum(self):
        #for x, in self._query_all("SELECT DISTINCT list.id FROM list LEFT JOIN map ON (map.listid = list.id) WHERE map.listid IS NULL", ()):
            #self.conn.execute("DELETE FROM list WHERE list.id = ?", (x,))
        pass

    def dir(self, attr, prefix=[], **criteria):
        '''Return the sub-partitions of attr from items in DB. I.E. for a DB containing
        prefix=(a/a, a/b, b/a, c/a), db.partition("prefix") will return set('a','b','c').'''
        assert attr=="path", "Sorry, only path supported ATM."
        assert attr not in criteria
        result = dict()
        if prefix:
            criteria[attr] = prefix
            plen = len(prefix)
        else:
            plen = 0

        for k,v in self.query(criteria):
            if not k.startswith("dn:"):
                continue
            k = path_str2lst(k[3:])
            k = k[plen]
            if k in result:
                result[k] += 1
            else:
                result[k] = 1

        return result

def open(config):
    return DB(config)

if __name__ == '__main__':
    import config
    db = DB(config.read())

    obj = db['myasset']
    obj['name'] = 'monkeyman'
    print "Yeah, I got", str(obj), obj.dirty

    db.merge(obj)
    print "Yeah, I got", str(obj), obj.dirty

    obj = db['myasset']
    print "Yeah, I got", str(obj), obj.dirty

    for obj in db.query({'name': 'monkeyman'}):
        print obj

    db.commit()