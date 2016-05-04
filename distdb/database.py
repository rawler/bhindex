import contextlib
from time import time

import concurrent
import sqlite3

from obj import Object, ValueSet, ANY
from _setup import create_DB

# Pointers to empty list will be wiped after 30 days.
DEFAULT_GRACE = 3600 * 24 * 30


# Matcher-class to match a prefix of a value
class Starts(unicode):
    pass


# Generate an SQL condition that finds object with matching criteria
def _sql_condition(k, v):
    equal_query = """SELECT DISTINCT objid FROM map
                       NATURAL JOIN list
                       NATURAL JOIN key
                       WHERE key = ? AND list.value = ?"""
    starts_query = """SELECT DISTINCT objid FROM map
                        WHERE
                          keyid = (SELECT keyid FROM key WHERE key = ?)
                            AND
                            listid IN (SELECT listid FROM list WHERE value GLOB '%s*')"""
    any_query = """SELECT DISTINCT objid FROM map
                     NATURAL JOIN key
                     WHERE key = ?"""
    absent_query = """SELECT DISTINCT objid FROM obj
                        WHERE NOT EXISTS (
                          SELECT 1 FROM map
                            NATURAL JOIN key
                            WHERE obj.objid = map.objid AND key = ? AND listid IS NOT NULL
                        )"""
    if v is ANY:
        return (any_query, (k,))
    elif v is None:
        return (absent_query, (k,))
    elif isinstance(v, Starts):
        return (starts_query % v.replace("'", "''"), (k,))
    else:
        return (equal_query, (k, v))


def _sql_for_criteria(crit):
    sql = []
    params = []
    for k, v in crit.iteritems():
        sql_fragment, new_params = _sql_condition(k, v)
        sql.append(sql_fragment)
        params += new_params

    return " INTERSECT ".join(sql), params


class DB(object):
    ANY = ANY
    Starts = Starts

    def __init__(self, path, sync=True):
        self.conn = sqlite3.connect(
            path, timeout=60, isolation_level='DEFERRED', check_same_thread=False)
        self.conn.execute("PRAGMA synchronous = %s" %
                          (sync and 'NORMAL' or 'OFF'))
        self.cursor = self.conn.cursor()
        create_DB(self.conn)
        self.__idCache = dict()
        self.lock = concurrent.ThreadLock()

    @contextlib.contextmanager
    def transaction(self):
        with self.lock, self.conn:
            yield

    def _query_all(self, query, args):
        with self.lock:
            c = self.conn.cursor()
            c.execute(query, args)
            return c.fetchall()

    def _query_first(self, query, args):
        with self.lock:
            c = self.cursor
            c.execute(query, args)
            return c.fetchone()

    def _query_single(self, query, args=[], default=None):
        res = self._query_first(query, args)
        if res:
            return res[0]
        else:
            return default

    def _get_id(self, tbl, id):
        objid = self._query_single(
            "SELECT %sid FROM %s WHERE %s = ?" % (tbl, tbl, tbl), (id,))
        if not objid:
            with self.lock:
                self.cursor.execute(
                    "INSERT OR IGNORE INTO %s (%s) VALUES (?)" % (tbl, tbl), (id,))
                objid = self._query_single(
                    "SELECT %sid FROM %s WHERE %s = ?" % (tbl, tbl, tbl), (id,))
        return objid

    def _getCachedId(self, tbl, id):
        try:
            return self.__idCache[(tbl, id)]
        except KeyError:
            id = self._get_id(tbl, id)
            self.__idCache[(tbl, id)] = id
            return id

    def get(self, obj, fields=None):
        if isinstance(obj, int):
            objid = obj
            obj = self._query_single(
                'SELECT obj FROM obj WHERE objid = ?', (objid,))
        else:
            objid = self._query_single(
                'SELECT objid FROM obj WHERE obj = ?', (obj,))
        obj = Object(obj)
        query = "SELECT key, timestamp, listid FROM map NATURAL JOIN key WHERE objid = ?"
        if fields is not None:
            query += " AND key IN (" + ', '.join(('?',) * len(fields)) + ")"
        for key, timestamp, listid in self._query_all(query, (objid,) + tuple(fields or ())):
            if listid is None:
                continue
            obj._dict[key] = ValueSet(v=(x for x, in self._query_all(
                "SELECT value FROM list WHERE listid = ?", (listid,))), t=timestamp)
        return obj

    def __getitem__(self, obj):
        return self.get(obj)

    def __delitem__(self, obj):
        object_id = getattr(obj, 'id', obj)
        objid = self._get_id('obj', object_id)
        t = time()
        with self.lock:
            self.conn.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                            SELECT objid, keyid, ?, NULL FROM map WHERE objid = ?""", (t, objid))

    def update_attr(self, objid, key, values):
        objid = self._get_id('obj', objid)
        keyid = self._getCachedId('key', key)
        tstamp = self._query_single(
            "SELECT timestamp FROM map WHERE objid = ? AND keyid = ?", (objid, keyid))
        if values.t > tstamp:
            newlistid = self._insert_list(values)
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                            VALUES (?, ?, ?, ?)""",
                               (objid, keyid, values.t, newlistid))
            return cursor.lastrowid
        else:
            return False

    def _get_list_id(self, values):
        _values = set(values)
        suspects = [x for (x,) in self._query_all(
            "SELECT listid FROM list WHERE value = ?", (_values.pop(),))]
        while _values and suspects:
            query = "SELECT listid FROM list WHERE value = ? AND listid IN (%s)" % (
                ','.join(['?'] * len(suspects)))
            suspects = [
                x for (x,) in self._query_all(query, [_values.pop()] + suspects)]
        if suspects and (not _values):
            query = "SELECT listid FROM list WHERE listid IN (%s) GROUP BY listid HAVING COUNT(value) = ?" % (
                ','.join(['?'] * len(suspects)))
            listid = self._query_single(query, suspects + [len(values)])
            if listid:
                return listid

    def _insert_list(self, values):
        if not values:
            return None
        listid = self._get_list_id(values)
        if listid:
            return listid
        newlistid = (
            self._query_single("SELECT MAX(listid) FROM list") or 0) + 1
        with self.lock:
            self.conn.cursor().executemany("INSERT INTO list (listid, value) VALUES (?, ?)",
                                           [(newlistid, value) for value in values])
        return newlistid

    def query_ids(self, criteria):
        query, params = _sql_for_criteria(criteria)
        query = "SELECT obj FROM obj NATURAL JOIN (%s)" % query
        for objid, in self._query_all(query, params):
            yield objid

    def query_raw_ids(self, criteria):
        query, params = _sql_for_criteria(criteria)
        for objid, in self._query_all(query, params):
            yield objid

    def query(self, criteria, fields=None):
        for objid in self.query_raw_ids(criteria):
            yield self.get(objid, fields)

    def update(self, obj):
        _dict = obj._dict
        objid = self._get_id('obj', obj.id)

        with self.lock:
            cursor = self.conn.cursor()
            for key in obj._dirty:
                values = _dict.get(key, None)
                if values is None:
                    values = ValueSet([], t=obj._deleted[key])
                keyid = self._getCachedId('key', key)
                old_timestamp = self._query_single(
                    "SELECT timestamp FROM map WHERE objid = ? and keyid = ?", (objid, keyid))
                if not old_timestamp or values.t >= old_timestamp:
                    listid = self._insert_list(values)
                    cursor.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                                        VALUES (?, ?, ?, ?)""",
                                   (objid, keyid, values.t, listid))
        obj._dirty.clear()
        return obj

    def commit(self):
        with self.lock:
            self.conn.commit()

    def vacuum(self, delete_grace=DEFAULT_GRACE):
        with self.lock:
            if delete_grace is not None:
                self.conn.execute("DELETE FROM map WHERE timestamp < ? AND listid IS NULL", (time() - delete_grace,))
            for x, in self._query_all("SELECT DISTINCT list.listid FROM list LEFT JOIN map ON (map.listid = list.listid) WHERE map.listid IS NULL", ()):
                self.conn.execute("DELETE FROM list WHERE listid = ?", (x,))
            for x, in self._query_all("SELECT DISTINCT key.keyid FROM key LEFT JOIN map ON (key.keyid = map.keyid) WHERE map.keyid IS NULL", ()):
                self.conn.execute("DELETE FROM key WHERE key.keyid = ?", (x,))
            for x, in self._query_all("SELECT DISTINCT obj.objid FROM obj LEFT JOIN map ON (obj.objid = map.objid) WHERE map.objid IS NULL", ()):
                self.conn.execute("DELETE FROM obj WHERE obj.objid = ?", (x,))
            self.conn.execute("VACUUM")

    def get_public_mappings_after(self, serial=0, limit=1024):
        for obj, key, tstamp, serial, listid in self._query_all("SELECT obj, key, timestamp, serial, listid FROM map NATURAL JOIN key NATURAL JOIN obj WHERE serial > ? AND NOT key LIKE '@%' ORDER BY serial LIMIT ?", (serial, limit)):
            values = set(x for x, in self._query_all(
                "SELECT value FROM list WHERE listid = ?", (listid,)))
            yield obj, key, tstamp, serial, values

    def last_serial(self):
        return self._query_single("SELECT MAX(serial) FROM map") or 0

    def get_sync_state(self, peername):
        return {
            "last_received": self._query_single("SELECT last_received FROM sync_state WHERE peername=?", (peername,)) or 0
        }

    def set_sync_state(self, peername, last_received):
        with self.lock:
            self.conn.cursor().execute(
                "INSERT OR REPLACE INTO sync_state (peername, last_received) VALUES (?, ?)", (peername, last_received))
