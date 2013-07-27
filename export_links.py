#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess
from time import time

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

def main(force_all=False):
    DB = db.open(config)

    t = time()
    for asset in DB.query({'path': db.ANY, 'xt': db.ANY}):
        if asset.get(u'@linked') and not force_all:
            continue

        success = True
        for p in asset['path']:
            dst = path.normpath(path.join(LINKDIR, p)).encode('utf8')
            if not dst.startswith(LINKDIR):
                print "Warning! %s tries to break out of directory!" % dst
                continue

            tgt = path.join(BHFUSEDIR, magnet.fromDbObject(asset))
            if not link(dst, tgt):
                success = False 

	if success:
            asset[u'@linked'] = db.ValueSet((u'true',), t=t)
            DB.update(asset)
    DB.commit()

if __name__=='__main__':
    parser = OptionParser()
    parser.add_option("-T", "--force-all", action="store_true",
                      dest="force_all", default=False,
                      help="Normally, only links not marked with @linked in db, added. This removes that check, adding ALL links.")
    (options, args) = parser.parse_args()

    main(options.force_all)
