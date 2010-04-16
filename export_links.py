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
MTIMEREF = path.join(LINKDIR, ".last_import")
BHFUSEDIR = config.get('BITHORDE', 'fusedir')

def main(check_mtime=False):
    DB = db.open(config)

    if check_mtime == True:
        check_mtime = path.exists(MTIMEREF) and path.getmtime(MTIMEREF)

    for k,v in DB.iteritems('dn:'):
        dst = path.normpath(path.join(LINKDIR, k))
        if not dst.startswith(LINKDIR):
            print "Warning! %s tries to break out of directory!"
            continue
        if check_mtime and ((not hasattr(v,'mtime')) or (v.mtime < check_mtime)):
            continue
        if path.lexists(dst):
            os.unlink(dst)
        dstdir = path.dirname(dst)
        if not path.exists(dstdir):
            os.makedirs(dstdir)
        os.symlink(path.join(BHFUSEDIR, str(v)), dst)

    with open(MTIMEREF, 'a'):
        os.utime(MTIMEREF, None)

if __name__=='__main__':
    parser = OptionParser()
    parser.add_option("-M", "--ignore-mtime", action="store_false",
                      dest="check_mtime", default=True,
                      help="Normally, only links with mtime higher than the last sync are added. This removes that check, adding ALL links.")
    (options, args) = parser.parse_args()

    main(options.check_mtime)
