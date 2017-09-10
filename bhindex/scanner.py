from __future__ import absolute_import

from itertools import chain, islice
from logging import getLogger
from random import randint
from time import time
from warnings import warn

from concurrent import sleep

from bithorde import Client, parseConfig
from bhindex.util import cachedAssetLiveChecker
from distdb import ANY, Sorting, TimedBefore

SAFETY = 60
FUDGE = 30
MAX_BATCH = 1000
MIN_SLEEP = 5
UNCHECKED_SCAN_INTERVAL = 360


class Scanner(object):
    fields = ('bh_availability', 'xt', 'directory')

    def __init__(self, db, bithorde):
        self.db = db
        self.bithorde = bithorde
        self.available = 0
        self.processed = 0
        self.size = 0
        self.log = getLogger('scanner')
        self.last_unchecked_scan = 0

    def pick_stale(self, expires_before):
        q = self.db.query_keyed({
            'xt': ANY,
            'bh_availability': TimedBefore(expires_before),
        }, '+bh_availability', fields=self.fields,
            sortmeth=Sorting.timestamp)
        for _, obj in q:
            yield obj.getitem('bh_availability').t, obj.id

    def pick_unchecked(self):
        q = self.db.query_ids({
            'xt': ANY,
            'bh_availability': None,
        })
        for id in q:
            yield 0, id

    def pick_batch(self, expires_before, limit=MAX_BATCH):
        objs = self.pick_stale(expires_before)

        # pick_unchecked is slow, and unlikely to yield anything
        # so only run it in big intervals
        if self.last_unchecked_scan < expires_before - UNCHECKED_SCAN_INTERVAL:
            objs = chain(self.pick_unchecked(), objs)
            self.last_unchecked_scan = expires_before

        return islice(objs, limit)

    def _fudged_batch(self, expires_before, fudge=FUDGE):
        batch = self.pick_batch(expires_before + fudge)
        fudged_batch = ((t - randint(0, fudge), id) for t, id in batch)
        return ((t, id) for t, id in fudged_batch if t < expires_before)

    def run(self):
        while True:
            limit = time() + SAFETY
            batch = sorted(self._fudged_batch(limit))
            if batch:
                sleep(max(batch[0][0] - limit, MIN_SLEEP))
                self.run_batch((self.db[id] for _, id in batch))
            else:
                sleep(MIN_SLEEP)

    def run_batch(self, objs):
        self.available = 0
        self.processed = 0

        start = time()
        for obj, status_ok in cachedAssetLiveChecker(self.bithorde, objs, db=self.db, force=True):
            self.processed += 1
            if not status_ok:
                continue

            fsize = int(obj.any('filesize', 0))
            if fsize:
                if fsize < 1024 * 1024 * 1024 * 1024:  # Reasonably sized file
                    self.available += 1
                    self.size += fsize
                else:
                    warn("Asset with implausible size: %s" % obj)

        self.log.debug("Processed %d assets in %d seconds", self.processed, time() - start)


def prepare_args(parser, config):
    parser.add_argument("-a", "--all-objects",
                        action="store_true", dest="all_objects", default=False,
                        help="Look for availability on all objects, even those with still valid status in DB.")
    parser.set_defaults(main=main)


def main(args, config, db):
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    scanner = Scanner(db, bithorde)

    if args.all_objects:
        scanner.run_batch(db.query({'xt': ANY}, scanner.fields))
        scanner.log.info("Scanned %d assets. %d available, totaling %d GB",
                         scanner.processed, scanner.available, scanner.size / (1024 * 1024 * 1024))
    else:
        scanner.run()
