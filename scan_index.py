#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess
from time import time

from bithorde.eventlet import Client, parseConfig
import eventlet
from util import cachedAssetLiveChecker

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

LINKDIR = path.normpath(config.get('LINKSEXPORT', 'linksdir'))
BHFUSEDIR = config.get('BITHORDE', 'fusedir')

def main(verbose, all_objects):
    DB = db.open(config.get('DB', 'file'))
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    proc_count = 0
    count = size = 0

    def echo_rate():
        now = time()
        while bithorde:
            start_count = proc_count
            start_time = now
            eventlet.sleep(2)
            now = time()
            processed = proc_count - start_count
            time_passed = now - start_time
            print "Processed/second: ", (processed/time_passed)
    if verbose:
        eventlet.spawn(echo_rate)

    crit = {'xt': db.ANY}
    for asset, status_ok in cachedAssetLiveChecker(bithorde, DB.query(crit), db=DB, force=all_objects):
        proc_count += 1
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
