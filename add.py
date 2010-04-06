#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import ConfigParser
import subprocess

import db

config = ConfigParser()
config.read(path.join(path.dirname(__file__), 'my.config'))

DB = db.open(config)

bh_bindir = path.expanduser(config.get('BITHORDE', 'bindir'))
bh_upload = path.join(bh_bindir, 'bhupload')

bhup = subprocess.Popen([bh_upload, sys.argv[1]], stdout=subprocess.PIPE)
bhup_out, _ = bhup.communicate()

magnet = None
for line in bhup_out.splitlines():
    try:
        proto, _ = line.split(':', 1)
    except ValueError:
        continue
    if proto == 'magnet':
        asset = db.Asset.fromMagnet(line)
        DB.merge(asset)
