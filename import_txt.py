#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess, urllib2

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
IMPORTS = config.get('TXTSYNC', 'imports').split(',')
UNIXSOCKET = config.get('BITHORDE', 'unixsocket')

DB = db.open(config)

def onStatusUpdate(asset, status, key):
    if status.status == bithorde.message.SUCCESS:
        DB.merge(key)

def onClientConnected(client, assetIds):
    ai = bithorde.AssetIterator(client, assetIds, onStatusUpdate, whenDone)

def onClientFailed(reason):
    print "Failed to connect to BitHorde; '%s'" % reason
    bithorde.reactor.stop()

def whenDone():
    bithorde.reactor.stop()

def assets():
    for imp in IMPORTS:
        url = config.get('txt_'+imp, 'url')
        input = urllib2.urlopen(url)
        for line in input:
            if line.startswith('magnet:'):
                asset = db.Asset.fromMagnet(line.strip())
                yield asset, asset.bithordeHashIds()
        input.close()

bithorde.connectUNIX(UNIXSOCKET, onClientConnected, onClientFailed, assets())
bithorde.reactor.run()

DB.commit()
