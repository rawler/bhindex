#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

TXTPATH = config.get('TXTSYNC', 'exportpath')
UNIXSOCKET = config.get('BITHORDE', 'unixsocket')

def main():
    DB = db.open(config)
    outfile = open(TXTPATH, 'w')

    def onStatusUpdate(asset, status, key):
        name, db_asset = key
        if status.status == bithorde.message.SUCCESS:
            outfile.write("%s\n"%db_asset.magnetURL(name))

    def onClientConnected(client, assetIds):
        ai = bithorde.AssetIterator(client, assetIds, onStatusUpdate, whenDone)

    def onClientFailed(reason):
        print "Failed to connect to BitHorde; '%s'" % reason
        bithorde.reactor.stop()

    def whenDone():
        bithorde.reactor.stop()

    assets = (((k,v),v.bithordeHashIds()) for k,v in DB.iteritems('dn:'))

    bithorde.connectUNIX(UNIXSOCKET, onClientConnected, onClientFailed, assets)
    bithorde.reactor.run()

    outfile.close()

if __name__=='__main__':
    main()
