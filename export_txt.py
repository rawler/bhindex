#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, shutil, sys, json
from ConfigParser import ConfigParser
import subprocess

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet, util
config = config.read()

TXTPATH = config.get('TXTSYNC', 'exportpath')
ADDRESS = config.get('BITHORDE', 'address')

def list_db(db):
    for asset in db.query({'path': db.ANY, 'name': db.ANY, 'xt': db.ANY}):
        assert asset.id.startswith(magnet.XT_PREFIX_TIGER)
        tigerhash = bithorde.b32decode(asset.id[len(magnet.XT_PREFIX_TIGER):])
        yield asset, {bithorde.message.TREE_TIGER: tigerhash}

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

def main(outfile = None, all_objects = False, all_attributes = False):
    DB = db.open(config)
    tmppath = outfile+".tmp"
    tmpfile = open(tmppath, 'w')
    count = util.Counter()
    storage = util.Counter()
    encoder = Encoder.configured(all_attributes=all_attributes)

    def writeOut(db_asset):
        if int(count):
            tmpfile.write(',\n')
        count.inc()
        json.dump(db_asset, tmpfile, cls=encoder, indent=2)

    def onStatusUpdate(asset, status, db_asset):
        if status.status == bithorde.message.SUCCESS:
            storage.inc(status.size)
            writeOut(db_asset)

    tmpfile.write('[')

    if all_objects:
        for db_asset, _ in list_db(DB):
            writeOut(db_asset)
    else:
        client = bithorde.BitHordeIteratorClient(list_db(DB), onStatusUpdate)
        bithorde.connect(ADDRESS, client)
        bithorde.reactor.run()

    tmpfile.write(']')
    tmpfile.close()

    if os.path.exists(outfile):
        shutil.copymode(outfile, tmppath)
    os.rename(tmppath, outfile)

    print "Exported %d assets, with %.2fGB worth of data." % (count, storage.inGibi())

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

    (options, args) = parser.parse_args()

    if len(args):
        outfile = args[0]
    else:
        outfile = TXTPATH

    main(
        outfile=outfile,
        all_objects=options.all_objects,
        all_attributes=options.all_attributes,
    )
