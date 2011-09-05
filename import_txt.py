#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys, json, urllib2
from ConfigParser import ConfigParser
from time import time

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet, scraper
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
UNIXSOCKET = config.get('BITHORDE', 'unixsocket')

def readMagnetAssets(input):
    t = time()
    for line in input:
        if line.startswith('magnet:'):
            asset = magnet.objectFromMagnet(line.strip().decode('utf8'), t)
            if asset:
                assert asset.id.startswith(magnet.XT_PREFIX_TIGER)
                tigerhash = bithorde.b32decode(asset.id[len(magnet.XT_PREFIX_TIGER):])
                yield asset, {bithorde.message.TREE_TIGER: tigerhash}

def readJSON(input):
    def obj_hook(d):
        if not '_type' in d:
            return d
        elif d['_type'] == 'db.ValueSet':
            return db.ValueSet(d['values'], d['timestamp'])
        elif d['_type'] == 'db.Object' and 'xt' in d:
            x = db.Object(d['xt'].any())
            del d['_type']
            x.update(d)
            return x
        else:
            return d

    for asset in json.load(input, object_hook=obj_hook):
        assert asset.id.startswith(magnet.XT_PREFIX_TIGER)
        tigerhash = bithorde.b32decode(asset.id[len(magnet.XT_PREFIX_TIGER):])
        yield asset, {bithorde.message.TREE_TIGER: tigerhash}

STATUS = bithorde.message._STATUS
class ImportSession(object):
    def __init__(self, db, imports, do_scrape):
        self.do_scrape = do_scrape
        self.imports = imports
        self.db = db

    def assets(self):
        FORMATS = {
            'magnetlist': readMagnetAssets,
            'json': readJSON,
        }
        for format,url in self.imports:
            formatParser = FORMATS[format]
            input = urllib2.urlopen(url)
            for asset, hash in formatParser(input):
                yield asset, hash
            input.close()

    def onStatusUpdate(self, asset, status, db_asset):
        print u"%s: %s" % (STATUS.values_by_number[status.status].name, u','.join(db_asset['name']))
        if status.status == bithorde.message.SUCCESS:
            if self.do_scrape:
                scraper.scrape_for(db_asset)
            self.db.update(db_asset)

    def run(self):
        client = bithorde.BitHordeClient(self.assets(), self.onStatusUpdate)
        bithorde.connectUNIX(UNIXSOCKET, client)
        bithorde.reactor.run()

        self.db.vacuum()
        self.db.commit()

if __name__ == '__main__':
    import cliopt

    usage = "usage: %prog [options] [<format>:<url>] ..."
    parser = cliopt.OptionParser(usage=usage)
    parser.add_option("-s", "--scrape", action="store_true", dest="scrape", default=False,
                      help="Enables external scraping for found assets.")
    (options, args) = parser.parse_args()

    DB = db.open(config)

    if args:
        assets = (x.split(':',1) for x in args)
    else:
        assets = ((config.get('txt_'+imp, 'format'), config.get('txt_'+imp, 'url'))
                  for imp in config.get('TXTSYNC', 'imports').split(','))
    ImportSession(DB, assets, options.scrape).run()
