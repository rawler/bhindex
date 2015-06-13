#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess

from bithorde import Client, parseConfig
from util import cachedAssetLiveChecker, RepeatingTimer

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

LINKDIR = path.normpath(config.get('LINKSEXPORT', 'linksdir'))
BHFUSEDIR = config.get('BITHORDE', 'fusedir')

def main(verbose, all_objects):
    DB = db.open(config.get('DB', 'file'))
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    class StepCounter:
        def __init__(self):
            self.i = 0
        def inc(self):
            self.i += 1
        def read_and_reset(self):
            res = self.i
            self.i = 0
            return res
    proc_count = StepCounter()
    count = size = 0

    def echo_rate():
        print "Processed/second: ", (proc_count.read_and_reset()/2)
    if verbose:
        RepeatingTimer(2, echo_rate)

    crit = {'xt': db.ANY}
    for asset, status_ok in cachedAssetLiveChecker(bithorde, DB.query(crit), db=DB, force=all_objects):
        proc_count.inc()
        if not status_ok:
            continue

        fsize = int(asset.any('filesize') or 0)
        if fsize and (fsize < 1024*1024*1024*1024): # Reasonably sized file
            count += 1
            size += int(fsize)

    return count, size

if __name__=='__main__':
    usage = """usage: %prog [options]"""
    parser = OptionParser(usage=usage)

    parser.add_option("-a", "--all-objects",
                      action="store_true", dest="all_objects", default=False,
                      help="Look for availability on all objects, even those with still valid data in DB.")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Enables verbose output")

    (options, args) = parser.parse_args()

    count, size = main(verbose=options.verbose, all_objects=options.all_objects)
    print("Exported %s assets totaling %s GB" % (count, size/(1024*1024*1024)))
