#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs, os, os.path as path, sys, json, urllib2
from ConfigParser import ConfigParser
from time import time

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet, scraper, util
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
UNIXSOCKET = config.get('BITHORDE', 'unixsocket')

def readMagnetAssets(input):
    t = None
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
            for key in d:
                if key[0] == '@':
                    del d[key]
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
        self.count = util.Counter()
        self.storage = util.Counter()

    def assets(self):
        FORMATS = {
            'magnetlist': readMagnetAssets,
            'json': readJSON,
        }
        unireader = codecs.getreader('utf-8')
        for format,url in self.imports:
            formatParser = FORMATS[format]
            try:
                input = unireader(urllib2.urlopen(url))
                for asset, hash in formatParser(input):
                    yield asset, hash
                input.close()
            except urllib2.URLError:
                print "Failure on '%s'" % url

    def onStatusUpdate(self, asset, status, db_asset):
        print u"%s: %s" % (STATUS.values_by_number[status.status].name, u','.join(db_asset['name']))
        if status.status == bithorde.message.SUCCESS:
            self.count.inc()
            self.storage.inc(status.size)
            if self.do_scrape:
                scraper.scrape_for(db_asset)
            self.db.update(db_asset)

    def run(self):
        client = bithorde.BitHordeIteratorClient(self.assets(), self.onStatusUpdate)
        bithorde.connectUNIX(UNIXSOCKET, client)
        bithorde.reactor.run()

        self.db.vacuum()
        self.db.commit()
        return self

if __name__ == '__main__':
    import cliopt

    usage = """usage: %prog [options] [<format>:<url>] ...
where <format> is either 'json' or 'magnetlist'"""
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
    sess = ImportSession(DB, assets, options.scrape).run()
    print "Imported %d assets, with %.2fGB worth of data." % (sess.count, sess.storage.inGibi())
