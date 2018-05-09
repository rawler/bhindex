from __future__ import absolute_import

import codecs
import logging
import sys
from time import time
from types import GeneratorType
from warnings import warn

from bithorde import parseHashIds, message
from distdb import AsyncCommitter

if getattr(sys.stdout, 'encoding', 'UNDEFINED') is None:
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
if getattr(sys.stderr, 'encoding', 'UNDEFINED') is None:
    sys.stderr = codecs.getwriter('utf8')(sys.stderr)

AVAILABILITY_BASE_CHANGE = 600
AVAILABILITY_ALTENATOR = 0.4
UNCHANGED_WAIT_FACTOR = 0.4
CHANGE_WAIT_VALUE = 60

statusLog = logging.getLogger('statusLog')


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


class Timer:
    def __init__(self):
        self.reset()

    def reset(self):
        self.epoch = time()

    def elapsed(self):
        return Duration(time() - self.epoch)


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
        return u', '.join(obj['xt'])
    except KeyError:
        return obj.id


def utf8(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return s


def hasValidStatus(obj, t=None):
    '''Returns True, False or None, respectively'''
    validity = validAvailability(obj, t or time())
    if validity is None:
        return None
    else:
        return validity > 0


def validAvailability(obj, t=None):
    '''Returns availability score, or None for outdated cache'''
    availability, valid_until = _object_availability(obj)

    t = t or time()
    valid_for = valid_until - t
    if valid_for > 0:
        statusLog.debug("%s: Current availability %d valid for another %s", object_string(obj), availability, Duration(valid_for))
        return availability
    else:
        statusLog.debug("%s: Current availability %d invalid since %s", object_string(obj), availability, Duration(-valid_for))
        return None


def _object_availability(obj):
    '''Returns (availability, valid_until)'''
    dbAvailability = obj.getitem('bh_availability')
    if dbAvailability:
        return float(dbAvailability.any(0)), dbAvailability.t
    else:
        return 0, 0


def calc_new_availability(status_ok, avail):
    '''Return (new_availability, validity_seconds)'''
    if avail is None:
        avail = 0

    if avail and (avail > 0) == status_ok:
        if status_ok:
            new_availability = avail + AVAILABILITY_BASE_CHANGE
        else:
            new_availability = avail - AVAILABILITY_BASE_CHANGE
        return new_availability, abs(new_availability * UNCHANGED_WAIT_FACTOR)
    else:
        if status_ok:
            new_availability = AVAILABILITY_BASE_CHANGE
        else:
            new_availability = avail * AVAILABILITY_ALTENATOR - AVAILABILITY_BASE_CHANGE
        return new_availability, CHANGE_WAIT_VALUE


def set_new_availability(obj, status_ok, now=None):
    if now is None:
        now = time()

    avail, _ = _object_availability(obj)
    newAvail, valid_for = calc_new_availability(status_ok, avail)

    del obj['bh_status']
    del obj['bh_status_confirmed']
    obj.set('bh_availability', unicode(newAvail), t=now + valid_for)


def updateFolderAvailability(committer, item, t):
    for dir_mapping in item.get(u'directory', []):
        dir_mapping = dir_mapping.split('/', 1)
        if not len(dir_mapping) == 2:
            continue
        (dir_id, _) = dir_mapping
        directory = committer.db.get(dir_id)

        if directory.empty():
            continue

        objAvail = directory.getitem(u'bh_availability', None)
        if objAvail is None or t > objAvail.t:
            directory.set('bh_availability', unicode(AVAILABILITY_BASE_CHANGE), t=t)
            committer.update(directory)
            updateFolderAvailability(committer, directory, t)


def _checkAsset(bithorde, obj, now):
    ids = parseHashIds(obj.get('xt') or obj.id)
    if not ids:
        return obj, None

    with bithorde.open(ids) as bhAsset:
        status = bhAsset.status()
        status_ok = status and status.status == message.SUCCESS or False

    set_new_availability(obj, status_ok, now=now)

    if status and (status.size is not None):
        if status.size > 2**40:
            warn("Implausibly large asset-size: %r - %r" % (obj, status))
            return obj, None
        obj.set('filesize', unicode(status.size), t=now)

    return obj, status_ok


def _cachedCheckAsset(bithorde, obj, now, required_validity):
    dbStatus = hasValidStatus(obj, required_validity)
    if dbStatus is None:
        return _checkAsset(bithorde, obj, now)
    else:
        return obj, dbStatus


def _scan_assets(bithorde, objs, committer, checker):
    assetsChecked = Counter()
    cacheUse = Counter()
    available = Counter()
    timer = Timer()

    for obj, status in bithorde.pool().imap(checker, objs):
        assetsChecked.inc()

        if obj.dirty():
            if committer:
                new_avail = obj.getitem('bh_availability')
                if float(new_avail.any(0)) > 0:
                    updateFolderAvailability(committer, obj, new_avail.t)

                committer.update(obj)
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
        .debug("%d assets status-checked in %s. %d cached statuses used (%d%%). %d available (%d%%).",
               assetsChecked, timer.elapsed(), cacheUse, cacheUsePercent, available, availablePercent)


def cachedAssetLiveChecker(bithorde, objs, db=None, force=False, required_validity=None):
    now = time()

    if force:
        def checker(obj):
            return _checkAsset(bithorde, obj, now)
    else:
        required_validity = required_validity or time()

        def checker(obj):
            return _cachedCheckAsset(bithorde, obj, now, required_validity)

    if db:
        with AsyncCommitter(db) as committer:
            for x in _scan_assets(bithorde, objs, committer, checker):
                yield x
    else:
        for x in _scan_assets(bithorde, objs, None, checker):
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
