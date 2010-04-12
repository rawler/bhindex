#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

LINKDIR = config.get('LINKSEXPORT', 'linksdir')
TXTPATH = config.get('TXTSYNC', 'exportpath')

def main():
    DB = db.open(config)

    outfile = open(TXTPATH, 'w')
    for k,v in DB.iteritems('dn:'):
        outfile.write("%s\n"%v.magnetURL(k))
    outfile.close()

if __name__=='__main__':
    main()
