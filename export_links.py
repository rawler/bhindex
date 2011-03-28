#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

LINKDIR = path.normpath(config.get('LINKSEXPORT', 'linksdir'))
TIMEREF = path.join(LINKDIR, ".last_import")
BHFUSEDIR = config.get('BITHORDE', 'fusedir')

def main(check_timestamp=False):
    DB = db.open(config)

    if check_timestamp == True:
        check_timestamp = path.exists(TIMEREF) and path.getmtime(TIMEREF)

    for k,v in DB.iteritems('dn:'):
        dst = path.normpath(path.join(LINKDIR, k))
        if not dst.startswith(LINKDIR):
            print "Warning! %s tries to break out of directory!"
            continue
        if check_timestamp and (v.timestamp < check_timestamp):
            continue
        tgt = path.join(BHFUSEDIR, str(v))
        try:
            oldtgt = os.readlink(dst)
        except OSError:
            oldtgt = None

        if oldtgt == tgt:
            continue
        elif oldtgt: 
            os.unlink(dst)

        dstdir = path.dirname(dst)
        if not path.exists(dstdir):
            os.makedirs(dstdir)
        os.symlink(tgt, dst)

    with open(TIMEREF, 'a'):
        os.utime(TIMEREF, None)

if __name__=='__main__':
    parser = OptionParser()
    parser.add_option("-T", "--ignore-timestamps", action="store_false",
                      dest="check_timestamp", default=True,
                      help="Normally, only links with timestamps higher than the last sync are added. This removes that check, adding ALL links.")
    (options, args) = parser.parse_args()

    main(options.check_timestamp)
