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

def assets():
    for imp in IMPORTS:
        url = config.get('txt_'+imp, 'url')
        input = urllib2.urlopen(url)
        for line in input:
            if line.startswith('magnet:'):
                asset = db.Asset.fromMagnet(line.strip())
                yield asset, asset.bithordeHashIds()
        input.close()

STATUS = bithorde.message._STATUS
def onStatusUpdate(asset, status, key):
    print "%s:%s" % (STATUS.values_by_number[status.status].name, key.name)
    if status.status == bithorde.message.SUCCESS:
        DB.merge(key)

client = bithorde.BitHordeClient(assets(), onStatusUpdate)
bithorde.connectUNIX(UNIXSOCKET, client)
bithorde.reactor.run()

DB.commit()
