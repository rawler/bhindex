#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from argparse import ArgumentParser
from bhindex import config, links
from db import DB

if __name__ == '__main__':
    CLI = ArgumentParser(
        description='export_links.py - Deprecated, see bhindex.py instead',
    )
    cfg = config.read()
    links.prepare_args(CLI, cfg)
    args = CLI.parse_args(sys.argv[1:])
    links.main(args, cfg, DB(cfg.get('DB', 'file')))
