import inspect
import sqlite3
from time import time
from threading import Event, Thread

import concurrent

from obj import Object, TimedValues, ANY
from _setup import create_DB

# Pointers to empty list will be wiped after 30 days.
DEFAULT_GRACE = 3600 * 24 * 30


# Matcher-class to match a prefix of a value
class Starts(tuple):
    def __new__(cls, v):
        if not hasattr(v, '__iter__'):
            v = (v,)
        return super(Starts, cls).__new__(cls, v)


class TimedBefore(int):
    pass


def _sql_query_starts(k, v):
    crit = ' OR '.join("value GLOB '%s*'" % x.replace("'", "''") for x in v)
    query = """SELECT objid FROM map NATURAL JOIN list
        WHERE keyid = ? AND (%s)""" % crit
    return (query, (k,))


def _sql_condition(k, v):
    '''Generate an SQL condition that finds object with matching criteria'''
    equal_query = """SELECT objid FROM map
                       NATURAL JOIN list
                       WHERE LIKELIHOOD(keyid = ?, 0.9375) AND UNLIKELY(list.value = ?)"""
    any_query = """SELECT objid FROM map
                     WHERE keyid = ? AND listid NOT NULL"""
    timed_before_query = """SELECT objid FROM map
                     WHERE LIKELIHOOD(keyid = ?, 0.9375) AND listid IS NOT NULL AND timestamp < ?"""
    absent_query = """SELECT DISTINCT objid FROM map AS ref
                        WHERE LIKELIHOOD(listid NOT NULL, 0.9375) AND NOT EXISTS (
                          SELECT 1 FROM map
                            WHERE ref.objid = map.objid AND keyid = ? AND LIKELIHOOD(listid NOT NULL, 0.9375)
                        )"""
    if v is ANY:
        return (any_query, (k,))
    elif v is None:
        return (absent_query, (k,))
    elif isinstance(v, Starts):
        return _sql_query_starts(k, v)
    elif isinstance(v, TimedBefore):
        return (timed_before_query, (k, v))
    else:
        return (equal_query, (k, v))


def _sql_for_query(crit, key_lookup):
    sql = []
    params = []
    for k, v in crit.iteritems():
        k = key_lookup(k)
        sql_fragment, new_params = _sql_condition(k, v)
        sql.append(sql_fragment)
        params += new_params

    return " INTERSECT ".join(sql), params


def _parse_sort(sort):
    direction, key = sort[:1], sort[1:]
    if direction == '+':
        return "ASC", key
    elif direction == '-':
        return "DESC", key
    else:
        raise ValueError("Direction specifier %s in sort=%s is not valid" % (direction, sort))


def _sql_for_keyed_query(crit, key, offset, sortmeth, key_lookup):
    direction, key = _parse_sort(key)
    selection, params = _sql_for_query(crit, key_lookup)
    sort_key, sort_params = sortmeth()
    if selection:
        selection = "(%s) NATURAL JOIN map" % selection
    else:
        selection = "map"
    selection = """SELECT DISTINCT objid, value FROM %s
NATURAL JOIN list
    WHERE keyid = %s ORDER BY %s %s, objid""" % (selection, key_lookup(key), sort_key, direction)

    if offset:
        selection = selection + (" LIMIT -1 OFFSET %d" % offset)

    return selection, params + sort_params


class Sorting:
    @staticmethod
    def default_sort():
        return "value", []

    @staticmethod
    def split(character):
        return lambda: ("substr(value, instr(value, ?))", [character])

    @staticmethod
    def timestamp():
        return "timestamp", []


class Transaction(object):
    DEFERRED = "DEFERRED"
    IMMEDIATE = "IMMEDIATE"
    EXCLUSIVE = "EXCLUSIVE"
    ACTIVE = set([IMMEDIATE, EXCLUSIVE])

    def __init__(self, db, mode):
        self.db = db
        self.conn = db.conn
        self.lock = db.lock
        self.mode = mode
        self.reserved = mode in self.ACTIVE
        self.created = inspect.getouterframes(inspect.currentframe(), 2)[1:]
        self.count = 0

    def _begin(self):
        self.last_yield = time()
        with self.lock:
            if self.count > 1:
                self.conn.execute('SAVEPOINT s%d' % self.count)
            else:
                self.conn.execute('BEGIN %s TRANSACTION' % self.mode)
                self.db.in_transaction = self

    def __enter__(self):
        self.count += 1
        self._begin()
        return self

    def __exit__(self, type, exc, tb):
        assert self.count
        if exc:
            self._rollback()
        else:
            self._commit()
        self.count -= 1

    def _rollback(self):
        assert self.db.in_transaction is self
        with self.lock:
            if self.count > 1:
                self.conn.execute('ROLLBACK TO SAVEPOINT s%d' % self.count)
            else:
                self.conn.rollback()
                self.db.in_transaction = None

    def _commit(self):
        assert self.db.in_transaction is self
        with self.lock:
            if self.count > 1:
                self.conn.execute('RELEASE SAVEPOINT s%d' % self.count)
            else:
                self.conn.commit()
                self.db.in_transaction = None

    def yield_from(self, threshold=0):
        assert self.count == 1
        if self.last_yield + threshold > time():
            return
        self._commit()
        self._begin()

    def _insert_list(self, values):
        if not values:
            return None
        assert self.reserved
        listid = self.db._get_list_id(values)
        if listid:
            return listid
        newlistid = (
            self.db._query_single("SELECT MAX(listid) FROM list") or 0) + 1
        with self.lock:
            self.conn.cursor().executemany("INSERT INTO list (listid, value) VALUES (?, ?)",
                                           [(newlistid, value) for value in values])
        return newlistid

    def update_attr(self, objid, key, assignment):
        if not isinstance(assignment, TimedValues):
            assignment = TimedValues(assignment)
        objid = self.db._get_id('obj', objid)
        keyid = self.db.keys(key)
        tstamp = self.db._query_single(
            "SELECT timestamp FROM map WHERE objid = ? AND keyid = ?", (objid, keyid))
        if assignment.t > tstamp:
            newlistid = self._insert_list(assignment.v)
            with self.lock:
                cursor = self.conn.cursor()
                cursor.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                            VALUES (?, ?, ?, ?)""",
                               (objid, keyid, assignment.t, newlistid))
            return cursor.lastrowid
        else:
            return False

    def update(self, obj):
        _dict = obj._dict
        objid = self.db._get_id('obj', obj.id)

        with self.lock:
            cursor = self.conn.cursor()
            for key in obj._dirty:
                assignment = _dict[key]
                keyid = self.db.keys(key)
                old_timestamp = self.db._query_single(
                    "SELECT timestamp FROM map WHERE objid = ? and keyid = ?", (objid, keyid))
                if not old_timestamp or assignment.t >= old_timestamp:
                    listid = self._insert_list(assignment.v)
                    cursor.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                                        VALUES (?, ?, ?, ?)""",
                                   (objid, keyid, assignment.t, listid))
        obj._dirty.clear()
        return obj

    def delete(self, obj, t=None):
        object_id = getattr(obj, 'id', obj)
        objid = self.db._get_id('obj', object_id)
        t = t or time()
        with self.lock:
            self.conn.execute("""INSERT OR REPLACE INTO map (objid, keyid, timestamp, listid)
                            SELECT objid, keyid, ?, NULL FROM map WHERE objid = ?""", (t, objid))


class AsyncCommitter(Thread):
    def __init__(self, db, min_interval=0.5):
        super(AsyncCommitter, self).__init__(name="AsyncCommitter")
        self.daemon = True
        self.db = db
        self._db = db.clone()
        self._min_interval = min_interval
        self._pending = list()

    def __enter__(self):
        self._stop = Event()
        self.start()
        return self

    def __exit__(self, type, exc, tb):
        self._stop.set()
        self.join()

    def run(self):
        next_commit = 0
        stopped = False
        while not stopped:
            stopped = self._stop.wait(next_commit - time())
            next_commit = time() + self._min_interval

            pending, self._pending = self._pending, list()
            self._flush(pending)

    def _flush(self, objs):
        if not objs:
            return
        with Transaction(self._db, Transaction.IMMEDIATE) as t:
            for obj in objs:
                t.update(obj)

    def update(self, obj):
        self._pending.append(obj)


class Key(int):
    __slots__ = ('name')

    def __new__(cls, id, name):
        res = super(Key, cls).__new__(cls, id)
        res.name = name
        return res


class Keys(dict):
    def __init__(self, db):
        self.db = db

    def __call__(self, k):
        if isinstance(k, Key):
            return k
        v = self.get(k, None) or self._read(k)
        return v

    def _read(self, name):
        id = self.db._get_id('key', name)
        self[id] = self[name] = Key(id, name)
        return self[id]


class DB(object):
    ANY = ANY
    Starts = Starts

    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(
            path, timeout=60, isolation_level=None, check_same_thread=False)
        self.cursor = self.conn.cursor()
        create_DB(self.conn)
        self.keys = Keys(self)
        self.lock = concurrent.ThreadLock()
        self.in_transaction = None

    def clone(self):
        return type(self)(self.path)

    def set_volatile(self, v):
        sync = v and 'OFF' or 'NORMAL'
        self.conn.execute("PRAGMA synchronous = %s" % sync)

    def transaction(self, type=Transaction.IMMEDIATE):
        if self.in_transaction:
            return self.in_transaction
        else:
            t = Transaction(self, type)
            t.created = inspect.getouterframes(inspect.currentframe(), 2)[1:]
            return t

    def _query_all(self, query, args):
        with self.lock:
            c = self.conn.cursor()
            print(query, args)
            c.execute(query, args)
            return c.fetchall()

    def _query_first(self, query, args):
        with self.lock:
            c = self.conn.cursor()
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
            obj._dict[key] = TimedValues(v=(x for x, in self._query_all(
                "SELECT value FROM list WHERE listid = ?", (listid,))), t=timestamp)
        return obj

    def __getitem__(self, obj):
        return self.get(obj)

    def query_ids(self, criteria):
        query, params = _sql_for_query(criteria, self.keys)
        query = "SELECT obj FROM obj NATURAL JOIN (%s)" % query
        for objid, in self._query_all(query, params):
            yield objid

    def query_raw_ids(self, criteria):
        query, params = _sql_for_query(criteria, self.keys)
        for objid, in self._query_all(query, params):
            yield objid

    def query(self, criteria, fields=None):
        query, params = _sql_for_query(criteria, self.keys)
        for objid, in self._query_all(query, params):
            yield self.get(objid, fields)

    def query_keyed(self, criteria, key, offset=0, fields=None, sortmeth=Sorting.default_sort):
        query, params = _sql_for_keyed_query(criteria, key, offset, sortmeth, self.keys)
        for objid, key_value in self._query_all(query, params):
            yield key_value, self.get(objid, fields)

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

    def vacuum(self, delete_grace=DEFAULT_GRACE):
        Q_CLEAN_MAP = "SELECT DISTINCT list.listid FROM list LEFT JOIN map ON (map.listid = list.listid) WHERE map.listid IS NULL"
        Q_CLEAN_LIST = "SELECT DISTINCT key.keyid FROM key LEFT JOIN map ON (key.keyid = map.keyid) WHERE map.keyid IS NULL"
        Q_CLEAN_OBJS = "SELECT DISTINCT obj.objid FROM obj LEFT JOIN map ON (obj.objid = map.objid) WHERE map.objid IS NULL"

        with self.lock:
            with self.transaction():
                if delete_grace is not None:
                    self.conn.execute("DELETE FROM map WHERE timestamp < ? AND listid IS NULL", (time() - delete_grace,))
                for x, in self._query_all(Q_CLEAN_MAP, ()):
                    self.conn.execute("DELETE FROM list WHERE listid = ?", (x,))
                for x, in self._query_all(Q_CLEAN_LIST, ()):
                    self.conn.execute("DELETE FROM key WHERE key.keyid = ?", (x,))
                for x, in self._query_all(Q_CLEAN_OBJS, ()):
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
