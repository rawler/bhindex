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

bh_bindir = path.expanduser(config.get('BITHORDE', 'bindir'))
bh_upload_bin = path.join(bh_bindir, 'bhupload')

def bh_upload(file, keep_path=False):
    bhup = subprocess.Popen([bh_upload_bin, file], stdout=subprocess.PIPE)
    bhup_out, _ = bhup.communicate()

    for line in bhup_out.splitlines():
        try:
            proto, _ = line.split(':', 1)
        except ValueError:
            continue
        if proto == 'magnet':
            asset = db.Asset.fromMagnet(line)
            if keep_path:
                p = path.normpath(file)
                p = p.lstrip('./~') # Remove .. and similar placeholders
                asset.name = p
            return asset
    return None

if __name__ == '__main__':
    from optparse import OptionParser
    usage = "usage: %prog [options] file1 [file2 ...]"
    parser = OptionParser(usage=usage)
    parser.add_option("-p", "--keep-path",
                      action="store_true", dest="keeppath", default=False,
                      help="Retain the full path in generated magnetUrl:s")

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("At least one file must be specified")

    DB = db.open(config)
    for file in args:
        asset = bh_upload(file, options.keeppath)
        if asset:
            DB.merge(asset)
        else:
            print "Error adding %s" % file
    DB.commit()