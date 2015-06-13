#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json, os, os.path as path, shutil, sys
from ConfigParser import ConfigParser
import subprocess

from bithorde import Client, parseConfig
from util import cachedAssetLiveChecker, Counter, Progress

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

TXTPATH = config.get('TXTSYNC', 'exportpath')

class Encoder(json.JSONEncoder):
    def __init__(self, all_attributes=False, *args, **kwargs):
        self.all_attributes = all_attributes
        json.JSONEncoder.__init__(self, *args, **kwargs)

    @classmethod
    def configured(cls, all_attributes = False):
        def _res(*args, **kwargs):
            return cls(*args, all_attributes=all_attributes, **kwargs)
        return _res

    def default(self, o):
        if isinstance(o, db.ValueSet):
            return {'_type': 'db.ValueSet',
                    'timestamp': o.t,
                    'values': list(o)}
        if isinstance(o, db.Object):
            x = {'_type': 'db.Object'}
            items = o._dict.iteritems()
            if not self.all_attributes:
                items = [(k,v) for k,v in items if not k[0] == '@']
            x.update(items)
            return x
        else:
            return self.default(o)

def main(outfile = TXTPATH, all_objects = False, all_attributes = False, verbose = False):
    DB = db.open(config.get('DB', 'file'))
    tmppath = outfile+".tmp"
    exported = Counter()
    storage = Counter()
    encoder = Encoder.configured(all_attributes=all_attributes)
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    with open(tmppath, 'w') as tmpfile:
        tmpfile.write('[')

        asset_ids = list(DB.query_ids({'path': db.ANY, 'name': db.ANY, 'xt': db.ANY}))

        if verbose:
            pr = Progress(len(asset_ids))
        else:
            pr = None

        assets = (DB[id] for id in asset_ids)
        if all_objects:
            assets = ((a, True) for a in assets)
        else:
            assets = cachedAssetLiveChecker(bithorde, assets, db=DB)
        for dbAsset, status_ok in assets:
            if pr:
                pr.inc()
            if not status_ok:
                continue
            if exported.inc() > 1:
                tmpfile.write(',\n')
            json.dump(dbAsset, tmpfile, cls=encoder, indent=2)
            filesize = int((('filesize' in dbAsset) and dbAsset['filesize'].any()) or 0)
            if filesize > (1<<40):
                print "Warning: %s size of %s" % (dbAsset['xt'].any(), filesize)
            storage.inc(filesize)
        DB.commit()

        tmpfile.write(']')
        if pr:
            pr.wait()

    if os.path.exists(outfile):
        shutil.copymode(outfile, tmppath)
    os.rename(tmppath, outfile)

    print "Exported %d assets, with %.2fGB worth of data." % (exported, storage.inGibi())

if __name__=='__main__':
    from optparse import OptionParser

    usage = """usage: %prog [options] [outfile] ...
'outfile' will be read from config, unless provided """
    parser = OptionParser(usage=usage)
    parser.add_option("-a", "--all-objects",
                      action="store_true", dest="all_objects", default=False,
                      help="export all objects without checking bithorde for availability")
    parser.add_option("-x", "--all-attributes",
                      action="store_true", dest="all_attributes", default=False,
                      help="export all attributes, even @-attributes that are usually local")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False,
                      help="print extra info on stdout")

    (options, args) = parser.parse_args()

    if len(args):
        outfile = args[0]
    else:
        outfile = TXTPATH

    if outfile:
        main(
            outfile=outfile,
            all_objects=options.all_objects,
            all_attributes=options.all_attributes,
            verbose=options.verbose,
        )
    else:
        print("Needs text outfile in either config or as argument\n")
        parser.print_help()
