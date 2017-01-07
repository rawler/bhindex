from __future__ import absolute_import

import codecs
import logging
import sys
from time import time
from types import GeneratorType
from warnings import warn

import concurrent
from bithorde import parseHashIds, message
from distdb import Transaction, ValueSet

if getattr(sys.stdout, 'encoding', 'UNDEFINED') is None:
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
if getattr(sys.stderr, 'encoding', 'UNDEFINED') is None:
    sys.stderr = codecs.getwriter('utf8')(sys.stderr)

AVAILABILITY_BONUS = 0.02
AVAILABILITY_EXPONENT = 0.77


class Counter(object):
    '''Incrementing counter in nested scopes'''
    def __init__(self, val=0):
        self.val = val

    def inc(self, value=1):
        self.val += value

    def __nonzero__(self):
        return self.val != 0

    def __int__(self):
        return self.val

    def __long__(self):
        return self.val

    def __trunc__(self):
        return self.val

    def __str__(self):
        return str(self.val)

    def __mul__(self, b):
        return self.val * b

    def __rmul__(self, b):
        return self.val * b

    def __div__(self, b):
        return self.val / b

    def __rdiv__(self, b):
        return b / self.val


class Duration(float):
    def __str__(self):
        '''Return a string '''
        x = self
        if x < 4:
            return "%d milliseconds" % (x * 1000)
        if x < 120:
            return "%d seconds" % x
        x /= 60
        if x < 120:
            return "%d minutes" % x
        x /= 60
        if x < 48:
            return "%d hours" % x
        x /= 24
        if x < 14:
            return "%d days" % x
        if x < 740:
            return "%d weeks" % (x / 7)
        else:
            return "%d years" % (x / 365.25)


class DelayedAction(object):
    def __init__(self, action):
        self.action = action
        self._scheduled = None

    def schedule(self, delay):
        if self._scheduled is None:
            self._scheduled = concurrent.spawn_after(delay, self.fire)

    def fire(self):
        self._scheduled = None
        self.action()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self._scheduled:
            self.fire()


def time_str(t):
    if abs(t) < 120:
        return "%d seconds" % t
    t /= 60
    if abs(t) < 120:
        return "%d minutes" % t
    t /= 60
    if abs(t) < 48:
        return "%d hours" % t
    t /= 24
    if abs(t) < 14:
        return "%d days" % t
    t /= 7
    return "%d weeks" % t


def object_string(obj):
    try:
        return obj['xt']
    except:
        return obj.id


def utf8(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return s


def hasValidStatus(obj, t=time()):
    '''Returns True, False or None, respectively'''
    validity = validAvailability(obj, t)
    if validity is None:
        return None
    else:
        return validity > 0


def validAvailability(obj, t=time()):
    '''Returns the number of seconds the object is believed to have been available/unavailable, or None for outdated cache'''
    availability, time_since_check = _object_availability(obj, t)

    valid_for = (abs(availability or 0) ** AVAILABILITY_EXPONENT) - (time_since_check or 0)

    log = logging.getLogger('hasValidStatus')
    if valid_for > 0:
        log.debug("%s: Current availability %d valid for another %s", object_string(obj), availability, Duration(valid_for))
        if availability > 0:
            return availability - time_since_check
        else:
            return availability + time_since_check
    else:
        log.debug("%s: Current availability %d invalid since %s", object_string(obj), availability, Duration(-valid_for))
        return None


def _object_availability(obj, t):
    '''Returns (availability, time_since_check)'''
    try:
        dbAvailability = obj[u'bh_availability']
        avail = float(dbAvailability.any(0))
        time_since_check = t - dbAvailability.t
        return avail, time_since_check
    except:
        pass

    # Legacy
    try:
        dbStatus = obj[u'bh_status']
        dbConfirmedStatus = obj[u'bh_status_confirmed']
        time_since_check = (t - dbConfirmedStatus.t)
        if dbStatus.any() == 'True':
            avail = dbConfirmedStatus.t - dbStatus.t
        else:
            avail = -(dbConfirmedStatus.t - dbStatus.t)
        return avail, time_since_check
    except:
        pass

    return 0, 0


def calc_new_availability(status_ok, avail, seconds_since_check):
    if seconds_since_check is None:
        seconds_since_check = 1800
    if avail is None:
        avail = 0

    bonus = seconds_since_check * AVAILABILITY_BONUS

    if status_ok:
        return max(avail, 0) + bonus
    else:
        return min(avail, 0) - bonus


def updateFolderAvailability(db, item, newAvail, t):
    tgt = t + newAvail ** AVAILABILITY_EXPONENT

    for dir_mapping in item.get(u'directory', []):
        dir_mapping = dir_mapping.split('/', 1)
        if not len(dir_mapping) == 2:
            continue
        (dir_id, _) = dir_mapping
        directory = db.get(dir_id)

        if directory.empty():
            continue

        objAvail = directory.get(u'bh_availability', 0)
        if objAvail:
            value = float(objAvail.any(0))
            if value > 0:
                objAvail = objAvail.t + value ** AVAILABILITY_EXPONENT
            else:
                objAvail = objAvail.t
        if tgt > objAvail:
            directory[u'bh_availability'] = ValueSet(unicode(newAvail), t=t)
            db.update(directory)
            updateFolderAvailability(db, directory, newAvail, t)


def _checkAsset(bithorde, obj, t):
    ids = parseHashIds(obj.get('xt', tuple()))
    if not ids:
        return obj, None

    with bithorde.open(ids) as bhAsset:
        status = bhAsset.status()
        status_ok = status and status.status == message.SUCCESS or False
    avail, time_since_check = _object_availability(obj, t)
    newAvail = calc_new_availability(status_ok, avail, time_since_check)

    del obj['bh_status']
    del obj['bh_status_confirmed']
    obj['bh_availability'] = ValueSet(unicode(newAvail), t=t)

    if status and (status.size is not None):
        if status.size > 2**40:
            warn("Implausibly large asset-size: %r - %r" % (obj, status))
            return obj, None
        obj['filesize'] = ValueSet((unicode(status.size),), t=t)

    return obj, status_ok


def _cachedCheckAsset(bithorde, obj, t):
    dbStatus = hasValidStatus(obj, t)
    if dbStatus is None:
        return _checkAsset(bithorde, obj, t)
    else:
        concurrent.sleep()  # Not sleeping here could starve other greenlets
        return obj, dbStatus


def _scan_assets(bithorde, objs, transaction, checker, t):
    assetsChecked = Counter()
    cacheUse = Counter()
    available = Counter()

    for obj, status in bithorde.pool().imap(checker, objs):
        assetsChecked.inc()

        if obj.dirty():
            if transaction:
                db = transaction.db
                new_avail = float(obj.any('bh_availability'))
                if new_avail > 0:
                    updateFolderAvailability(transaction.db, obj, new_avail, t)

                db.update(obj)
                transaction.yield_from(2)
        else:
            cacheUse.inc()

        if status:
            available.inc()

        yield obj, status

    if assetsChecked:
        cacheUsePercent = cacheUse * 100 / assetsChecked
        availablePercent = available * 100 / assetsChecked
    else:
        cacheUsePercent = 100
        availablePercent = 100
    logging \
        .getLogger('cachedAssetLiveChecker') \
        .debug("%d assets status-checked. %d cached statuses used (%d%%). %d available (%d%%).",
               assetsChecked, cacheUse, cacheUsePercent, available, availablePercent)


def cachedAssetLiveChecker(bithorde, objs, db=None, force=False):
    t = time()

    if force:
        def checker(obj):
            return _checkAsset(bithorde, obj, t)
    else:
        def checker(obj):
            return _cachedCheckAsset(bithorde, obj, t)

    if db:
        with db.transaction(Transaction.IMMEDIATE) as transaction:
            for x in _scan_assets(bithorde, objs, transaction, checker, t):
                yield x
    else:
        for x in _scan_assets(bithorde, objs, None, checker, t):
            yield x


class Timed:
    def __init__(self, tag):
        self.tag = tag
        self.log = logging.getLogger('timed')

    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, type, value, traceback):
        delta = (time() - self.start) * 1000
        self.log.debug("<%s>: %.1fms" % (self.tag, delta))


def timed(method):
    def timed(*args, **kw):
        with Timed("%r (%r, %r)" % (method.__name__, args, kw)):
            res = method(*args, **kw)
            if isinstance(res, GeneratorType):
                res = list(res)
        return res
    return timed


class RepeatingTimer(object):
    def __init__(self, interval, code):
        self.running = True
        self.running = concurrent.spawn(self._run, interval, code)

    def _run(self, interval, code):
        now = time()
        next = now + interval
        while self.running:
            concurrent.sleep(next - now)
            code()
            now = time()
            next = max(now, next + interval)

    def cancel(self):
        self.running = None
