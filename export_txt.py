#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys, json
from ConfigParser import ConfigParser
import subprocess

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

TXTPATH = config.get('TXTSYNC', 'exportpath')
UNIXSOCKET = config.get('BITHORDE', 'unixsocket')

def list_db(db):
    for asset in db.query({'path': db.ANY, 'name': db.ANY, 'xt': db.ANY}):
        assert asset.id.startswith(magnet.XT_PREFIX_TIGER)
        tigerhash = bithorde.b32decode(asset.id[len(magnet.XT_PREFIX_TIGER):])
        yield asset, {bithorde.message.TREE_TIGER: tigerhash}

class Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, db.ValueSet):
            return {'_type': 'db.ValueSet',
                    'timestamp': o.t,
                    'values': list(o)}
        if isinstance(o, db.Object):
            x = {'_type': 'db.Object'}
            x.update((k,v) for k,v in o._dict.iteritems() if not k[0] == '@')
            return x
        else:
            return self.default(o)

class ctr(object):
    def __init__(self):
        self.i = 0

    def __call__(self):
        res = self.i
        self.i += 1
        return res

def main():
    DB = db.open(config)
    tmppath = TXTPATH + ".tmp"
    outfile = open(tmppath, 'w')
    count=ctr()

    def onStatusUpdate(asset, status, db_asset):
        if status.status == bithorde.message.SUCCESS:
            if count():
                outfile.write(',\n')
            json.dump(db_asset, outfile, cls=Encoder, indent=2)

    outfile.write('[')

    client = bithorde.BitHordeIteratorClient(list_db(DB), onStatusUpdate)
    bithorde.connectUNIX(UNIXSOCKET, client)
    bithorde.reactor.run()

    outfile.write(']')
    outfile.close()

    os.rename(tmppath, TXTPATH)

if __name__=='__main__':
    main()
