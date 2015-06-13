#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess
from time import time

from bithorde import Client, parseConfig
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
        old_umask = os.umask(0o022) # u=rwx,g=rx,o=rx
        try:
            os.makedirs(dstdir)
        except OSError as e:
            print "Failed to create directory %s:" % dstdir
            print "  ", e
            return False
        finally:
            os.umask(old_umask)
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

def main(force_all=False, output_dir=LINKDIR, prefixes=[]):
    DB = db.open(config.get('DB', 'file'))
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    t = time()
    count = size = 0

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

            tgt = path.join(BHFUSEDIR, magnet.fromDbObject(asset))
            dst = path.normpath(path.join(output_dir, p))
            if (not dst.startswith(output_dir)) or len(dst) <= len(output_dir):
                print "Warning! %s (%s) tries to break out of directory!" % (dst, tgt)
                continue

            print u"Linking %s -> %s" % (dst, tgt)
            if not link(dst, tgt):
                success = False

        if success:
            fsize = int(asset.any('filesize') or 0)
            if fsize and (fsize < 1024*1024*1024*1024): # Reasonably sized file
                count += 1
                size += int(fsize)

            asset[u'@linked'] = db.ValueSet((u'true',), t=t)
            with DB.transaction():
                DB.update(asset)
    return count, size

if __name__=='__main__':
    usage = """usage: %prog [options] [path] ...
'output-dir' will be read from config, unless provided"""
    parser = OptionParser(usage=usage)
    parser.add_option("-T", "--force-all", action="store_true",
                      dest="force_all", default=False,
                      help="Normally, only links not marked with @linked in db, added. This removes that check, adding ALL links.")
    parser.add_option("-o", "--output-dir", action="store",
                      dest="output_dir", default=LINKDIR,
                      help="Directory to write links to")
    (options, args) = parser.parse_args()


    if options.output_dir:
        count, size = main(prefixes=args, force_all=options.force_all, output_dir=options.output_dir)
        print("Exported %s assets totaling %s GB" % (count, size/(1024*1024*1024)))
    else:
        print("Needs link output-dir in either config or as argument\n")
        parser.print_help()
