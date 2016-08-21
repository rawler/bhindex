from __future__ import absolute_import

import logging
from warnings import warn

from bithorde import Client, parseConfig
from bhindex.util import cachedAssetLiveChecker
from distdb import ANY


class Scanner(object):

    def __init__(self, db, bithorde):
        self.db = db
        self.bithorde = bithorde
        self.available = 0
        self.processed = 0
        self.size = 0

    def run(self, objs, force=False):
        for obj, status_ok in cachedAssetLiveChecker(self.bithorde, objs, db=self.db, force=force):
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


def prepare_args(parser, config):
    parser.add_argument("-a", "--all-objects",
                        action="store_true", dest="all_objects", default=False,
                        help="Look for availability on all objects, even those with still valid status in DB.")
    parser.set_defaults(main=main)


def main(args, config, db):
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    scanner = Scanner(db, bithorde)

    log = logging.getLogger('scanner')

    scanner.run(db.query({'xt': ANY}), args.all_objects)

    log.info("Scanned %d assets. %d available, totaling %d GB",
             scanner.processed, scanner.available, scanner.size / (1024 * 1024 * 1024))
