from __future__ import absolute_import

import os
import os.path as path
import sys
from ConfigParser import ConfigParser
from optparse import OptionParser

from bithorde import Client, parseConfig
from bhindex.util import cachedAssetLiveChecker, RepeatingTimer

import config
import db

config = config.read()

LINKDIR = path.normpath(config.get('LINKSEXPORT', 'linksdir'))
BHFUSEDIR = config.get('BITHORDE', 'fusedir')


class StepCounter:
    def __init__(self):
        self.i = 0

    def inc(self):
        self.i += 1

    def read_and_reset(self):
        res = self.i
        self.i = 0
        return res


class Scanner(object):
    def __init__(self, db, bithorde):
        self.db = db
        self.bithorde = bithorde
        self.processed = StepCounter()
        self.count = 0
        self.size = 0

    def run(self, objs, force=False):
        for obj, status_ok in cachedAssetLiveChecker(self.bithorde, objs, db=self.db, force=force):
            self.processed.inc()
            if not status_ok:
                continue

            fsize = int(obj.any('filesize', 0))
            if fsize:
                if fsize < 1024 * 1024 * 1024 * 1024:  # Reasonably sized file
                    self.count += 1
                    self.size += fsize
                else:
                    print "Asset with implausible size: %s" % obj


def prepare_args(parser):
    parser.add_argument("-a", "--all-objects",
                        action="store_true", dest="all_objects", default=False,
                        help="Look for availability on all objects, even those with still valid status in DB.")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", default=False,
                        help="Enables verbose output")
    parser.set_defaults(main=main)


def main(args):
    DB = db.open(config.get('DB', 'file'))
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    scanner = Scanner(DB, bithorde)

    def echo_rate():
        print "Processed/second: ", (scanner.processed.read_and_reset() / 2)
    if args.verbose:
        RepeatingTimer(2, echo_rate)

    scanner.run(DB.query({'xt': db.ANY}), args.all_objects)

    print("Scanned %s assets totaling %s GB" %
          (scanner.count, scanner.size / (1024 * 1024 * 1024)))
