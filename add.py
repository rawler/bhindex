#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config

config = config.read()

bh_bindir = path.expanduser(config.get('BITHORDE', 'bindir'))
bh_upload_bin = path.join(bh_bindir, 'bhupload')

def sanitizedpath(file):
    '''Assumes path is normalized through path.normpath first,
    then santizies by removing leading path-fixtures such as [~./]'''
    return file.lstrip('./~') # Remove .. and similar placeholders

def bh_upload(file):
    '''Assumes file is normalized through path.normpath'''
    bhup = subprocess.Popen([bh_upload_bin, file], stdout=subprocess.PIPE)
    bhup_out, _ = bhup.communicate()

    for line in bhup_out.splitlines():
        try:
            proto, _ = line.split(':', 1)
        except ValueError:
            continue
        if proto == 'magnet':
            asset = db.Asset.fromMagnet(line)
            return asset
    return None

if __name__ == '__main__':
    from optparse import OptionParser
    usage = "usage: %prog [options] file1 [file2 ...]" \
            "  An argument of '-' will expand to filenames read line for line on standard input."
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--strip-path", action="store_const", dest="sanitizer",
                      default=sanitizedpath, const=path.basename,
                      help="Strip name to just the name of the file, without path")
    parser.add_option("-f", "--force",
                      action="store_true", dest="force", default=False,
                      help="Force upload even of assets already in sync in index")

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("At least one file must be specified")

    DB = db.open(config)

    def add(file):
        '''Try to upload one file to bithorde and add to index'''
        file = path.normpath(file)
        name = options.sanitizer(file)
        mtime = path.getmtime(file)

        oldasset = DB.by_name(name)
        if (not options.force) and oldasset and hasattr(oldasset,'mtime') \
             and (oldasset.mtime == mtime):
            return # Skip this file if already in index
        else:
            asset = bh_upload(file)
            if asset:
                asset.name = name
                asset.mtime = mtime
                DB.merge(asset)
            else:
                print "Error adding %s" % file

    try:
        for arg in args:
            if arg == '-':
                for line in sys.stdin:
                    add(line.strip())
            else:
                add(arg)
    finally:
        DB.commit()