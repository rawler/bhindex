#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging, os, sys
from optparse import OptionParser
from time import time

from bithorde.eventlet import Client, parseConfig
from util import make_directory

HERE = os.path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('fileindex')

def main(force_all=False):
    DB = db.open(config.get('DB', 'file'))
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    t = time()
    count = size = 0

    crit = {'path': db.ANY, 'xt': db.ANY}
    if not force_all:
        crit['directory'] = None
    for asset in DB.query(crit):
        path = asset.any('path').lstrip('/').split('/')
        if not path: continue
        log.info("Indexing %s", path)

        with DB.transaction():
            dir_id = make_directory(DB, path[:-1], t)

            dir_entry = u"%s/%s" % (dir_id,path[-1])
            asset[u'directory'] = db.ValueSet((dir_entry,), t=t)
            DB.update(asset)

        fsize = int(asset.any('filesize') or 0)
        if fsize and (fsize < 1024*1024*1024*1024): # Reasonably sized file
            count += 1
            size += int(fsize)

    return count, size

if __name__=='__main__':
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-T", "--force-all", action="store_true",
                      dest="force_all", default=False,
                      help="Normally, only links not marked with @linked in db, added. This removes that check, adding ALL links.")
    (options, args) = parser.parse_args()

    count, size = main(force_all=options.force_all)
    log.info("Indexed %s assets totaling %s GB", count, size/(1024*1024*1024))
