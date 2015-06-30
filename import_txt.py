#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs, os, os.path as path, sys, json, urllib2
from ConfigParser import ConfigParser
from time import time

from bithorde import Client, parseConfig
from util import cachedAssetLiveChecker, Counter

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, scraper
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')

def readJSON(input, all_attributes = False):
    def obj_hook(d):
        if not '_type' in d:
            return d
        elif d['_type'] == 'db.ValueSet':
            return db.ValueSet(d['values'], d['timestamp'])
        elif d['_type'] == 'db.Object' and 'xt' in d:
            x = db.Object(d['xt'].any())
            del d['_type']
            if not all_attributes:
                for key in d:
                    if key[0] == '@':
                        del d[key]
            x.update(d)
            return x
        else:
            return d

    return json.load(input, object_hook=obj_hook)

def runImport(db, imports, do_scrape, all_objects, all_attributes):
    count = Counter()
    storage = Counter()
    dirty = Counter()
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    def importFromSource(url):
        input = unireader(urllib2.urlopen(url))
        assets = readJSON(input, all_attributes=all_attributes)
        if all_objects:
            assets = [(a, True) for a in assets]
        else:
            assets = cachedAssetLiveChecker(bithorde, assets)
        for db_asset, status_ok in assets:
            asset_name = u','.join(db_asset['name'])
            if status_ok:
                print u"SUCCESS: %s" % asset_name
                count.inc()
                storage.inc(int((('filesize' in db_asset) and db_asset['filesize'].any()) or 0))
                if do_scrape:
                    scraper.scrape_for(db_asset)
                db.update(db_asset)
                if dirty.inc() > 500:
                    db.commit()
                    dirty.reset()
            else:
                print u"NOTFOUND: %s" % asset_name
        input.close()

    unireader = codecs.getreader('utf-8')
    for format,url in imports:
        if format != 'json':
            raise "Unsupported Format"
        try:
            importFromSource(url)
        except urllib2.URLError:
            print "Failure on '%s'" % url
    db.vacuum()
    db.commit()

    print "Imported %d assets, with %.2fGB worth of data." % (count, storage.inGibi())

if __name__ == '__main__':
    import cliopt

    usage = """usage: %prog [options] [<format>:<url>] ...
where <format> is either 'json' or 'magnetlist'"""
    parser = cliopt.OptionParser(usage=usage)
    parser.add_option("-s", "--scrape", action="store_true", dest="scrape", default=False,
                      help="Enables external scraping for found assets.")
    parser.add_option("-a", "--all-objects",
                      action="store_true", dest="all_objects", default=False,
                      help="import all objects without checking bithorde for availability")
    parser.add_option("-x", "--all-attributes",
                      action="store_true", dest="all_attributes", default=False,
                      help="import all attributes, even @-attributes that are usually local")

    (options, args) = parser.parse_args()

    DB = db.open(config.get('DB', 'file'))

    if args:
        imports = (x.split(':',1) for x in args)
    else:
        imports = ((config.get('txt_'+imp, 'format'), config.get('txt_'+imp, 'url'))
                  for imp in config.get('TXTSYNC', 'imports').split(','))
    runImport(DB, imports,
        do_scrape=options.scrape,
        all_objects=options.all_objects,
        all_attributes=options.all_attributes,
    )
