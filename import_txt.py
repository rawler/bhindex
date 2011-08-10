#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys, json, urllib2
from ConfigParser import ConfigParser
from time import time

import bithorde

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, magnet, re
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
IMPORTS = config.get('TXTSYNC', 'imports').split(',')
UNIXSOCKET = config.get('BITHORDE', 'unixsocket')

DB = db.open(config)

PATH_RULES = [
    re.compile(r'(?P<category>Movies|TV)/(?!XXX)'),
    re.compile(r'Movies/(?P<category>XXX)'),
    re.compile(r'Movies/(?P<title>.*) \((?P<year>\d{4})\)/'),
    re.compile(r'Movies/(?P<title>[^/]+)[. ](720p|1080p|bdrip|dvdrid|dvdr|PAL|xvid|\.)*.*/', re.I),
    re.compile(r'TV/(?P<series>[^/]+)/Season (?P<season>\d+)/.* \d{1,2}?x(?P<episode>\d{2})'),
]

def mapPath(path, asset, t):
    for rule in PATH_RULES:
        m = rule.match(path)
        if m:
            for k, v in m.groupdict().iteritems():
                k = unicode(k, 'utf8')
                if k in asset:
                    asset[k].add(v)
                else:
                    asset[k] = db.ValueSet(v, t)

def readMagnetAssets(input):
    for line in input:
        if line.startswith('magnet:'):
            x = magnet.parse(line.strip().decode('utf8'))
            if x:
                asset = db.Object(x['xt'])
                t = time()
                asset[u'path'] = db.ValueSet(x['path'], t)
                asset[u'name'] = db.ValueSet(x['name'], t)
                asset[u'filetype'] = db.ValueSet(x['filetype'], t)
                for path in x['path']:
                    mapPath(path, asset, t)

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

def onStatusUpdate(asset, status, key):
    if status.status == bithorde.message.SUCCESS:
        DB.merge(key)

client = bithorde.BitHordeClient(assets(), onStatusUpdate)
bithorde.connectUNIX(UNIXSOCKET, client)
bithorde.reactor.run()

DB.vacuum()
DB.commit()
