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

def main():
    DB = db.open(config)
    for k,v in DB.iteritems('dn:'):
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

if __name__=='__main__':
    main()
