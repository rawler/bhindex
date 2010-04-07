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
BHFUSEDIR = config.get('BITHORDE', 'fusedir')

DB = db.open(config)

name_dict = dict()

for k,v in DB.iteritems('dn:'):
    name_dict[k] = v

for k,v in name_dict.iteritems():
    dst = path.join(LINKDIR, k)
    if path.lexists(dst):
        os.unlink(dst)
    os.symlink(path.join(BHFUSEDIR, str(v)), dst)
