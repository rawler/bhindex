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

def bh_upload(file):
    bhup = subprocess.Popen([bh_upload_bin, sys.argv[1]], stdout=subprocess.PIPE)
    bhup_out, _ = bhup.communicate()

    for line in bhup_out.splitlines():
        try:
            proto, _ = line.split(':', 1)
        except ValueError:
            continue
        if proto == 'magnet':
            return db.Asset.fromMagnet(line)
    return None

if __name__ == '__main__':
    DB = db.open(config)
    asset = bh_upload(sys.argv[1])
    DB.merge(asset)
    DB.commit()