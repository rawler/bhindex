#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

LINKDIR = path.normpath(config.get('LINKSEXPORT', 'linksdir'))
BHFUSEDIR = config.get('BITHORDE', 'fusedir')

DB = db.open(config)

name_dict = dict()

for k,v in DB.iteritems('dn:'):
    name_dict[k] = v

for k,v in name_dict.iteritems():
    dst = path.normpath(path.join(LINKDIR, k))
    if not dst.startswith(LINKDIR):
        print "Warning! %s tries to break out of directory!"
	continue	
    if path.lexists(dst):
        os.unlink(dst)
    dstdir = path.dirname(dst)
    if not path.exists(dstdir):
        os.makedirs(dstdir)
    os.symlink(path.join(BHFUSEDIR, str(v)), dst)
