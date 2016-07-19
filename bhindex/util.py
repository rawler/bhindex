from __future__ import absolute_import

import codecs
import logging
import sys
from time import time
from types import GeneratorType
from warnings import warn

import concurrent
from bithorde import parseHashIds, message
from distdb import ValueSet

if getattr(sys.stdout, 'encoding', 'UNDEFINED') is None:
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
if getattr(sys.stderr, 'encoding', 'UNDEFINED') is None:
    sys.stderr = codecs.getwriter('utf8')(sys.stderr)

ASSET_WAIT_FACTOR = 0.02


class DelayedAction(object):
    def __init__(self, action):
        self.action = action
        self._scheduled = None

    def schedule(self, delay):
        if self._scheduled is None:
            self._scheduled = concurrent.spawn_after(delay, self._fire)

    def _fire(self):
        self._scheduled = None
        self.action()


def hasValidStatus(obj, t=time()):
    '''Returns True, False or None, respectively'''
    try:  # New status
        availability = obj['bh_availability']
        available = float(availability.any(0))

        if (availability.t + abs(available)) > t:
            return available > 0
        else:
            return None
    except:
        pass

    try:  # Legacy
        dbStatus = obj['bh_status']
        dbConfirmedStatus = obj['bh_status_confirmed']

        stable = dbConfirmedStatus.t - dbStatus.t
        nextCheck = dbConfirmedStatus.t + (stable * ASSET_WAIT_FACTOR)
        if t < nextCheck:
            return dbStatus.any() == 'True'
        else:
            return None
    except:
        pass

    return None


def _object_availability(obj, t):
    '''Returns (bool(lastCheck), time_since_check)'''
    try:
        dbAvailability = obj[u'bh_availability']
        avail = float(dbAvailability.any(0))
        time_since_check = t - dbAvailability.t
        return avail, time_since_check
    except:
        pass

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

    return None, None


def calc_new_availability(status_ok, avail, seconds_since_check):
    if seconds_since_check is None:
        seconds_since_check = 1800
    if avail is None:
        avail = 0

    bonus = seconds_since_check * ASSET_WAIT_FACTOR

    if status_ok:
        return max(avail, 0) + bonus
    else:
        return min(avail, 0) - bonus


def updateFolderAvailability(db, item, newAvail, t):
    tgt = t + newAvail

    for dir_mapping in item.get(u'directory', []):
        dir_mapping = dir_mapping.split('/', 1)
        if not len(dir_mapping) == 2:
            continue
        (dir_id, _) = dir_mapping
        directory = db.get(dir_id)

        if not directory.empty():
            objAvail = directory.get(u'bh_availability', 0)
            if objAvail:
                objAvail = objAvail.t + float(objAvail.any(0))
            else:
                objAvail = 0
            if tgt > objAvail:
                directory[u'bh_availability'] = ValueSet(unicode(newAvail), t=t)
                db.update(directory)
                updateFolderAvailability(db, directory, newAvail, t)


def cachedAssetLiveChecker(bithorde, objs, db=None, force=False):
    t = time()
    if db:
        commit_pending = DelayedAction(db.commit)

    def checkAsset(obj):
        if not force:
            dbStatus = hasValidStatus(obj, t)
            if dbStatus is not None:
                concurrent.sleep()  # Not sleeping here could starve other greenlets
                return obj, dbStatus

        ids = parseHashIds(obj.get('xt', tuple()))
        if not ids:
            return obj, None

        with bithorde.open(ids) as bhAsset:
            status = bhAsset.status()
            status_ok = status and status.status == message.SUCCESS
            obj['bh_status'] = ValueSet((unicode(status_ok),), t=t)
            obj['bh_status_confirmed'] = ValueSet((unicode(t),), t=t)

            avail, time_since_check = _object_availability(obj, t)
            newAvail = calc_new_availability(status_ok, avail, time_since_check)
            obj[u'bh_availability'] = ValueSet(unicode(newAvail), t=t)
            if newAvail > 0:
                updateFolderAvailability(db, obj, newAvail, t)

            if status and (status.size is not None):
                if status.size > 2**40:
                    warn("Implausibly large asset-size: %r - %r" % (obj, status))
                    return obj, None
                obj['filesize'] = ValueSet((unicode(status.size),), t=t)
            if db:
                db.update(obj)
                commit_pending.schedule(2)

            return obj, status_ok

    return bithorde.pool().imap(checkAsset, objs)


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
            concurrent.sleep(next-now)
            code()
            now = time()
            next = max(now, next + interval)

    def cancel(self):
        self.running = None
