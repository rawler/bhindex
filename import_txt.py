#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess, urllib2

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
IMPORTS = config.get('TXTSYNC', 'imports').split(',')

DB = db.open(config)

for imp in IMPORTS:
    url = config.get('txt_'+imp, 'url')
    input = urllib2.urlopen(url)
    for line in input:
        if line.startswith('magnet:'):
            DB.merge(line.strip())
    input.close()
DB.commit()
