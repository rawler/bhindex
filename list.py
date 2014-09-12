#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys, time
from ConfigParser import ConfigParser
from optparse import OptionParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config
config = config.read()

TXTPATH = config.get('TXTSYNC', 'exportpath')

def timestamp_to_str(ts):
    t = time.gmtime(ts)
    return time.strftime("%Y-%m-%dT%H:%M:%S", t)

class DB:
    def __init__(self, db):
        self._db = db;

    def list(self, filter=[]):
        for k,v in self._db.filter(path=filter):
            print "%s: %s"%(timestamp_to_str(v.timestamp), v.name)

    def dir(self, prefix=[]):
        for path, count in self._db.dir("path", prefix).iteritems():
            print count, '\t', path

if __name__=='__main__':
    parser = OptionParser(usage="usage: %prog [options] <PATH>")
    parser.add_option("-d", "--dir", action="store_true", dest="dir",
                      help="dir-mode, list subdirectory")
    parser.add_option("-l", "--list", action="store_false", dest="dir",
                      help="list-mode, list files")

    (options, args) = parser.parse_args()
    if len(args)>1:
        parser.error("Only one path-argument supported")
    elif args:
        path=db.path_str2lst(args[0])
    else:
        path=[]

    thisdb = DB(db.open(config.get('DB', 'file')))
    if options.dir:
        thisdb.dir(path)
    else:
        thisdb.list(path)