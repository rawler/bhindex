#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys, json, urllib2
from ConfigParser import ConfigParser
from time import time

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
IMPORTS = config.get('TXTSYNC', 'imports').split(',')
UNIXSOCKET = config.get('BITHORDE', 'unixsocket')

DB = db.open(config)

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

def assets():
    FORMATS = {
        'magnetlist': readMagnetAssets,
        'json': readJSON,
    }
    for imp in IMPORTS:
        url = config.get('txt_'+imp, 'url')
        formatParser = FORMATS[config.get('txt_'+imp, 'format')]

        input = urllib2.urlopen(url)
        for asset, hash in formatParser(input):
            yield asset, hash
        input.close()

STATUS = bithorde.message._STATUS
def onStatusUpdate(asset, status, key):
    print u"%s: %s" % (STATUS.values_by_number[status.status].name, u','.join(key['name']))
    if status.status == bithorde.message.SUCCESS:
        DB.update(key)

client = bithorde.BitHordeClient(assets(), onStatusUpdate)
bithorde.connectUNIX(UNIXSOCKET, client)
bithorde.reactor.run()

DB.vacuum()
DB.commit()
