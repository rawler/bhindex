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

def main(force_all=False):
    DB = db.open(config)

    t = time()
    for asset in DB.query({'path': db.ANY, 'xt': db.ANY}):
        if asset.get(u'@linked') and not force_all:
            continue

        for p in asset['path']:
            dst = path.normpath(path.join(LINKDIR, p)).encode('utf8')
            if not dst.startswith(LINKDIR):
                print "Warning! %s tries to break out of directory!" % dst
                continue

            tgt = path.join(BHFUSEDIR, magnet.fromDbObject(asset))
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
