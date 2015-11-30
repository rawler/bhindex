#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from argparse import ArgumentParser
from bhindex import config, syncer
from db import DB

if __name__ == '__main__':
    CLI = ArgumentParser(
        description='syncer.py - Deprecated, see bhindex.py instead',
    )
    cfg = config.read()
    syncer.prepare_args(CLI, cfg)
    args = CLI.parse_args(sys.argv[1:])
    syncer.main(args, cfg, DB(cfg.get('DB', 'file')))
