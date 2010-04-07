#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db

config = ConfigParser()
config.read(path.join(HERE, 'my.config'))

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
TXTPATH = config.get('TXTEXPORT', 'txtpath')

DB = db.open(config)

name_dict = dict()

for k,v in DB.iteritems('dn:'):
    name_dict[k] = v

outfile = open(TXTPATH, 'w')
for _,v in name_dict.iteritems():
    outfile.write("%s\n"%v.magnetURL())
outfile.close()
