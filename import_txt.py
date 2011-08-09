#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess, urllib2

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
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
                x = magnet.parse(line.strip().decode('utf8'))
                if x:
                    asset = DB[x['xt']]
                    asset.add(u'path', x['path'])
                    asset.add(u'name', x['name'])
                    asset.add(u'filetype', x['filetype'])
                    assert asset.id.startswith(magnet.XT_PREFIX_TIGER)
                    tigerhash = bithorde.b32decode(asset.id[len(magnet.XT_PREFIX_TIGER):])
                    yield asset, {bithorde.message.TREE_TIGER: tigerhash}
        input.close()

def onStatusUpdate(asset, status, key):
    if status.status == bithorde.message.SUCCESS:
        DB.merge(key)

client = bithorde.BitHordeClient(assets(), onStatusUpdate)
bithorde.connectUNIX(UNIXSOCKET, client)
bithorde.reactor.run()

DB.vacuum()
DB.commit()
