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
class Starts(unicode):
    pass

class ValueSet(set):
    def __init__(self, v, t):
        if isinstance(v, unicode):
            set.__init__(self, [v])
        else:
            set.__init__(self, v)
            for x in self:
                assert isinstance(x, unicode)
        self.t = t

    def any(self):
        for x in self:
            return x

    def join(self, sep=u', '):
        return unicode(sep).join(self)

class Object(object):
    def __init__(self, objid, init={}):
        self.id = objid
        self._dirty = set()
        self._dict = dict()
        self.update(init)

    def update(self, other):
        for k,v in other.iteritems():
            self[k] = v

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def __setitem__(self, key, value):
        assert isinstance(key, unicode)
        assert isinstance(value, ValueSet)

        if key not in self._dict or (value.t >= self._dict[key].t and self._dict[key] != value):
            self._dirty.add(key)
            self._dict[key] = value

    def iteritems(self):
        return self._dict.iteritems()

    def update_if_newer(self, key, value):
        assert isinstance(value, ValueSet)
        if key not in self or self[key].t < value.t:
            self.__do_set(key, value)

    def timestamp(self):
        return max(x.t for x in self._dict.itervalues())

    def __repr__(self):
        return repr(self._dict)

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

def _sql_for_criteria(crit):
    sql = []
    params = []
    match_query = """SELECT DISTINCT objid FROM map JOIN list ON (map.listid = list.id)
                        WHERE map.key = ? AND list.value LIKE ?"""
    any_query = """SELECT DISTINCT objid FROM map WHERE key = ?"""
    for k,v in crit.iteritems():
        params.append(k)
        if v is ANY:
            sql.append(any_query)
        elif isinstance(v, Starts):
            sql.append(match_query)
            params.append(v+'%')
        else:
            sql.append(match_query)
            params.append(v)

    return " INTERSECT ".join(sql), params

class DB(object):
    ANY = ANY
    Starts = Starts

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
            values = ValueSet(v=(x for x, in self._query_all("SELECT value FROM list WHERE id = ?", (listid,))), t=timestamp)
            obj._dict[key] = values
        return obj

    def query(self, criteria):
        for objid, in self._query_all(*_sql_for_criteria(criteria)):
            yield self[objid]

    def all(self):
        for objid, in self._query_all('SELECT DISTINCT objid FROM map', ()):
            yield self[objid]

    def list_keys(self, criteria=None):
        '''Returns a iterator of (key, distinct values for key) for all keys in DB.'''
        query = """SELECT key, COUNT(*)
    FROM (SELECT key, list.value, COUNT(*) AS c
        FROM map
            JOIN list ON map.listid = list.id
            %s
        GROUP BY key, list.value)
    GROUP BY key"""
        if criteria:
            q, args = _sql_for_criteria(criteria)
            query %= "JOIN (%s) USING (objid)" % q
        else:
            query %= ""
            args = ()

        return sorted(self._query_all(query, args), key=lambda (k,c): c)

    def list_values(self, key):
        for x, in self._query_all("""SELECT DISTINCT value
                FROM map
                    JOIN list ON map.listid = list.id
                WHERE map.key = ?
                ORDER BY value""", (key,)):
            yield x

    def update(self, obj):
        objid = obj.id
        _dict = obj._dict
        for key in obj._dirty:
            newlistid = self._query_single("SELECT MAX(id)+1 FROM list") or 1
            c = self.conn.executemany("INSERT INTO list (id, value) VALUES (?, ?)", ((newlistid, value) for value in _dict[key]))
            self.conn.execute("""INSERT OR REPLACE INTO map (objid, key, timestamp, listid)
                                VALUES (?, ?, ?, ?)""", 
                                (objid, key, _dict[key].t, newlistid))
        obj._dirty.clear()

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
            criteria[attr] = Starts('/'.join(prefix))
            plen = len(prefix)
        else:
            criteria[attr] = self.ANY
            plen = 0

        for obj in self.query(criteria):
            for k in obj[attr]:
                k = path_str2lst(k)
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
    obj[u'name'] = ValueSet(u'monkeyman', t=time.time())
    print "Yeah, I got", str(obj), obj._dirty

    db.update(obj)
    print "Yeah, I got", str(obj), obj._dirty

    obj = db['myasset']
    print "Yeah, I got", str(obj), obj._dirty

    for obj in db.query({'name': 'monkeyman'}):
        print obj

    for k,c in db.list_keys():
        print k,c

    db.commit()