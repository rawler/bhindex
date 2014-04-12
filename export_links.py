#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess
from time import time

from bithorde_eventlet import Client, parseConfig
from util import cachedAssetLiveChecker

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

LINKDIR = path.normpath(config.get('LINKSEXPORT', 'linksdir'))
BHFUSEDIR = config.get('BITHORDE', 'fusedir')

def link(linkpath, tgt):
    try:            oldtgt = os.readlink(linkpath)
    except OSError: oldtgt = None

    if oldtgt == tgt:
        return True
    elif oldtgt:
        try: os.unlink(linkpath)
        except OSError as e:
            print "Failed to remove old link %s:" % oldtgt
            print "  ", e
            return False

    dstdir = path.dirname(linkpath)
    if not path.exists(dstdir):
        try: os.makedirs(dstdir)
        except OSError as e:
            print "Failed to create directory %s:" % dstdir
            print "  ", e
            return False
    try: os.symlink(tgt, linkpath)
    except OSError as e:
        print "Failed to create link at %s:" % linkpath
        print "  ", e
        return False
    return True

def path_in_prefixes(path, prefixes):
    for prefix in prefixes:
        if path.startswith(prefix):
            return True
    return False

def main(force_all=False, prefixes=[]):
    DB = db.open(config)
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    t = time()

    crit = {'path': db.ANY, 'xt': db.ANY}
    if not force_all:
        crit['@linked'] = None
    for asset, status_ok in cachedAssetLiveChecker(bithorde, DB.query(crit), db=DB):
        if not status_ok:
            continue

        success = True
        for p in asset['path']:
            if prefixes and not path_in_prefixes(p, prefixes):
                success = False
                continue
            dst = path.normpath(path.join(LINKDIR, p))
            if not dst.startswith(LINKDIR):
                print "Warning! %s tries to break out of directory!" % dst
                continue

            tgt = path.join(BHFUSEDIR, magnet.fromDbObject(asset))
            print u"Linking %s -> %s" % (dst, tgt)
            if not link(dst, tgt):
                success = False

        if success:
                asset[u'@linked'] = db.ValueSet((u'true',), t=t)
                with DB.transaction():
                    DB.update(asset)

if __name__=='__main__':
    parser = OptionParser(usage="usage: %prog [options] [path...]")
    parser.add_option("-T", "--force-all", action="store_true",
                      dest="force_all", default=False,
                      help="Normally, only links not marked with @linked in db, added. This removes that check, adding ALL links.")
    (options, args) = parser.parse_args()

    main(options.force_all, args)
