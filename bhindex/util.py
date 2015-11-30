from __future__ import absolute_import

import codecs
import logging
import sys
from time import time
from types import GeneratorType
from warnings import warn

import concurrent
from bithorde import parseHashIds, message
from db import ValueSet

if getattr(sys.stdout, 'encoding', 'UNDEFINED') is None:
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
if getattr(sys.stderr, 'encoding', 'UNDEFINED') is None:
    sys.stderr = codecs.getwriter('utf8')(sys.stderr)


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

ASSET_WAIT_FACTOR = 0.01


def hasValidStatus(obj, t=time()):
    try:
        dbStatus = obj['bh_status']
        dbConfirmedStatus = obj['bh_status_confirmed']
    except KeyError:
        return None
    stable = dbConfirmedStatus.t - dbStatus.t
    nextCheck = dbConfirmedStatus.t + (stable * ASSET_WAIT_FACTOR)
    if t < nextCheck:
        return dbStatus.any() == 'True'
    else:
        return None


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

        ids = parseHashIds(obj['xt'])
        if not ids:
            return obj, None

        with bithorde.open(ids) as bhAsset:
            status = bhAsset.status()
            status_ok = status and status.status == message.SUCCESS
            obj['bh_status'] = ValueSet((unicode(status_ok),), t=t)
            obj['bh_status_confirmed'] = ValueSet((unicode(t),), t=t)
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
