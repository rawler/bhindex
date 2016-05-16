#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from argparse import ArgumentParser
from bhindex import add, config
from distdb import DB

if __name__ == '__main__':
    CLI = ArgumentParser(
        description='add.py - Deprecated, see bhindex.py instead',
    )
    cfg = config.read()
    add.prepare_args(CLI, cfg)
    args = CLI.parse_args(sys.argv[1:])
    add.main(args, cfg, DB(cfg.get('DB', 'file')))
