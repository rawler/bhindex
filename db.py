# -*- coding: utf-8 -*-
from urlparse import urlparse, parse_qs
from urllib import quote_plus as urlquote
import cPickle as pickle
import hashlib, struct
from time import time

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
    def __init__(self, v, t=None):
        if isinstance(v, unicode):
            set.__init__(self, [v])
        else:
            set.__init__(self, v)
            for x in self:
                assert isinstance(x, unicode)
        if not t:
            t = time()
        self.t = t

    def update(self, v, t=None):
        if not t:
            t = time()
        self.t = max([t, self.t])
        set.update(self, v)

    def any(self):
        for x in self:
            return x

    def join(self, sep=u', '):
        return unicode(sep).join(self)
    __unicode__ = __str__ = join

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

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def __contains__(self, key):
        return key in self._dict

    def __setitem__(self, key, value):
        assert isinstance(key, unicode)
        assert isinstance(value, ValueSet)

        key = key.lower()
        if key not in self._dict or (self._dict[key] != value and value.t >= self._dict[key].t):
            self._dirty.add(key)
            self._dict[key] = value

    def __delitem__(self, key):
        assert isinstance(key, unicode)

        if key in self._dict:
            self._dirty.add(key)
            self._dict[key].clear()
            self._dict[key].t = time()

    def iteritems(self):
        return self._dict.iteritems()

    def matches(self, criteria):
        for key, value in criteria.iteritems():
            if key not in self._dict:
                return False
            if value not in (None, ANY) and value not in self._dict[key]:
                return False
        return True

    def update_key(self, key, values, t=None):
        if not t:
            t=time()
        if isinstance(values, unicode):
            values = set([values])

        # Try to prune upcased properties
        lkey = key.lower()
        if lkey != key:
            del self[key]
            key = lkey

        if key in self:
            self[key].update(values, t)
            self._dirty.add(key)
        else:
            self[key] = ValueSet(values, t)

    def timestamp(self):
        return max(x.t for x in self._dict.itervalues())

    def __repr__(self):
        return repr(self._dict)

    def __unicode__(self):
        return u"db.Object {\n%s\n}" % u'\n'.join(u" %s: %s" % x for x in self.iteritems())

def create_DB(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS obj (
        objid INTEGER PRIMARY KEY AUTOINCREMENT,
        obj TEXT UNIQUE NOT NULL
    );
    CREATE INDEX IF NOT EXISTS obj_obj ON obj (obj);

    CREATE TABLE IF NOT EXISTS key (
        keyid INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT UNIQUE NOT NULL
    );
    CREATE INDEX IF NOT EXISTS key_key ON key (key);

    CREATE TABLE IF NOT EXISTS map (
        serial INTEGER NOT NULL PRIMARY KEY,
        objid INTEGER NOT NULL,
        keyid INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        listid INTEGER,
        UNIQUE (objid, keyid),
        FOREIGN KEY (objid) REFERENCES obj (objid),
        FOREIGN KEY (keyid) REFERENCES key (keyid),
        FOREIGN KEY (listid) REFERENCES list (listid)
    );
    CREATE INDEX IF NOT EXISTS map_key ON map (keyid);
    CREATE INDEX IF NOT EXISTS map_list ON map (listid);

    CREATE TABLE IF NOT EXISTS list (
        itemid INTEGER PRIMARY KEY AUTOINCREMENT,
        listid INTEGER NOT NULL,
        value TEXT NOT NULL,
        CONSTRAINT unique_list_value UNIQUE (listid, value) ON CONFLICT IGNORE
    );
    CREATE INDEX IF NOT EXISTS list_id ON list (listid);
    CREATE INDEX IF NOT EXISTS list_value ON list (value);

    CREATE TABLE IF NOT EXISTS sync_state (
        peername STRING PRIMARY KEY,
        last_received INTEGER NOT NULL
    );
    """)

def _sql_for_criteria(crit):
    sql = []
    params = []
    match_query = """SELECT DISTINCT objid FROM map
                        NATURAL JOIN list
                        NATURAL JOIN key
                     WHERE key = ? AND list.value LIKE ?"""
    any_query =   """SELECT DISTINCT objid FROM map
                        NATURAL JOIN key
                     WHERE key = ?"""
    absent_query =   """SELECT DISTINCT objid FROM obj
                            WHERE objid NOT IN (
                                SELECT objid FROM map
                                    NATURAL JOIN key
                                    WHERE key = ? AND listid IS NOT NULL
                            )"""
    for k,v in crit.iteritems():
        params.append(k)
        if v is ANY:
            sql.append(any_query)
        elif v is None:
            sql.append(absent_query)
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
        self.__idCache = dict()

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

    def _getId(self, tbl, id):
        try:
            return self.__idCache[(tbl, id)]
        except KeyError:
            params = locals()
            objid = self._query_single("SELECT %(tbl)sid FROM %(tbl)s WHERE %(tbl)s = ?" % params, (id,))
            if not objid:
                cursor = self.conn.cursor()
                cursor.execute("INSERT OR IGNORE INTO %(tbl)s (%(tbl)s) VALUES (?)" % params, (id,))
                objid = cursor.lastrowid
            self.__idCache[(tbl, id)] = objid
            return objid

    def __getitem__(self, objid):
        obj = Object(objid)
        for key, timestamp, listid in self._query_all("SELECT key, timestamp, listid FROM map NATURAL JOIN key NATURAL JOIN obj WHERE obj = ?", (objid,)):
            values = ValueSet(v=(x for x, in self._query_all("SELECT value FROM list WHERE listid = ?", (listid,))), t=timestamp)
            obj._dict[key] = values
        return obj

    def get_attr(self, objid, attr):
        row = self._query_first("SELECT timestamp, listid FROM map NATURAL JOIN key NATURAL JOIN obj WHERE obj = ? AND key = ?", (objid, attr))
        if row:
            timestamp, listid = row
            return ValueSet(v=(x for x, in self._query_all("SELECT value FROM list WHERE listid = ?", (listid,))), t=timestamp)
        else:
            return None

    def update_attr(self, objid, key, values):
        objid = self._getId('obj', objid)
        keyid = self._getId('key', key)
        tstamp = self._query_single("SELECT timestamp FROM map WHERE objid = ? AND keyid = ?", (objid, keyid))
        if values.t > tstamp:
            newlistid = self.insert_list(values)
            cursor = self.conn.cursor()
            cursor.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                            VALUES (?, ?, ?, ?)""",
                            (objid, keyid, values.t, newlistid))
            return cursor.lastrowid
        else:
            return False

    def _get_list_id(self, values):
        if not values:
            return None
        _values = set(values)
        suspects = [x for (x,) in self._query_all("SELECT listid FROM list WHERE value = ?", (_values.pop(),) )]
        while _values and suspects:
            query = "SELECT listid FROM list WHERE value = ? AND listid IN (%s)" % ( ','.join(['?']*len(suspects)) )
            suspects = [x for (x,) in self._query_all(query, [_values.pop()] + suspects)]
        if suspects and (not _values):
            query = "SELECT listid FROM list WHERE listid IN (%s) GROUP BY listid HAVING COUNT(value) = ?" % ( ','.join(['?']*len(suspects)) )
            listid = self._query_single(query, suspects + [len(values)])
            if listid:
                return listid

    def insert_list(self, values):
        if not values:
            return None
        listid = self._get_list_id(values)
        if listid:
            return listid
        newlistid = (self._query_single("SELECT MAX(listid) FROM list") or 0) + 1
        self.conn.cursor().executemany("INSERT INTO list (listid, value) VALUES (?, ?)", ((newlistid, value) for value in values))
        return newlistid

    def get_mtime(self, objid):
        return self._query_single("SELECT MAX(timestamp) FROM map NATURAL JOIN obj WHERE obj = ?", (objid,))

    def query_ids(self, criteria):
        query, params = _sql_for_criteria(criteria)
        query = "SELECT obj FROM obj NATURAL JOIN (%s)" % query
        for objid, in self._query_all(query, params):
            yield objid

    def query(self, criteria):
        for objid in self.query_ids(criteria):
            yield self[objid]

    def all_ids(self):
        for objid, in self._query_all('SELECT DISTINCT obj FROM map NATURAL JOIN obj', ()):
            yield objid

    def all(self):
        for objid in self.all_ids():
            yield self[objid]

    def list_keys(self, criteria=None):
        '''Returns a iterator of (key, distinct values for key) for all keys in DB.'''
        query = """SELECT key, COUNT(*)
    FROM (SELECT key, list.value, COUNT(*) AS c
        FROM map
            NATURAL JOIN list
            NATURAL JOIN key
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
                FROM map NATURAL JOIN list NATURAL JOIN key
                WHERE key = ?
                ORDER BY value""", (key,)):
            yield x

    def update(self, obj):
        _dict = obj._dict
        objid = self._getId('obj', obj.id)

        cursor = self.conn.cursor()
        for key in obj._dirty:
            values = _dict[key]
            keyid = self._getId('key', key)
            old_timestamp = self._query_single("SELECT timestamp FROM map WHERE objid = ? and keyid = ?", (objid, keyid))
            if not old_timestamp or values.t > old_timestamp:
                listid = self.insert_list(values)
                cursor.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                                    VALUES (?, ?, ?, ?)""",
                                    (objid, keyid, _dict[key].t, listid))
        obj._dirty.clear()

    def commit(self):
        self.conn.commit()

    def vacuum(self):
        for x, in self._query_all("SELECT DISTINCT list.listid FROM list LEFT JOIN map ON (map.listid = list.listid) WHERE map.listid IS NULL", ()):
            self.conn.execute("DELETE FROM list WHERE listid = ?", (x,))
        for x, in self._query_all("SELECT DISTINCT key.keyid FROM key LEFT JOIN map ON (key.keyid = map.keyid) WHERE map.keyid IS NULL", ()):
            self.conn.execute("DELETE FROM key WHERE key.keyid = ?", (x,))
        for x, in self._query_all("SELECT DISTINCT obj.objid FROM obj LEFT JOIN map ON (obj.objid = map.objid) WHERE map.objid IS NULL", ()):
            self.conn.execute("DELETE FROM obj WHERE obj.objid = ?", (x,))
        self.conn.execute("VACUUM")

    def get_public_mappings_after(self, serial=0, limit=4096):
        for obj, key, tstamp, serial, listid in self._query_all("SELECT obj, key, timestamp, serial, listid FROM map NATURAL JOIN key NATURAL JOIN obj WHERE serial > ? AND NOT key LIKE '@%' ORDER BY serial LIMIT ?", (serial, limit)):
            values = set(x for x, in self._query_all("SELECT value FROM list WHERE listid = ?", (listid,)))
            yield obj, key, tstamp, serial, values

    def last_serial(self):
        return self._query_single("SELECT MAX(serial) FROM map") or 0;

    def get_sync_state(self, peername):
        return {
            "last_received": self._query_single("SELECT last_received FROM sync_state WHERE peername=?", (peername,)) or 0
        }

    def set_sync_state(self, peername, last_received):
        self.conn.cursor().execute("INSERT OR REPLACE INTO sync_state (peername, last_received) VALUES (?, ?)", (peername, last_received))

def open(config):
    return DB(config)

if __name__ == '__main__':
    import config
    db = DB(config.read())

    obj = db['myasset']
    obj[u'name'] = ValueSet(u'monkeyman', t=time())
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
