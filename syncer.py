#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from argparse import ArgumentParser
from bhindex import syncer

if __name__ == '__main__':
    CLI = ArgumentParser(
        description='syncer.py - Deprecated, see bhindex.py instead',
    )
    syncer.prepare_args(CLI)
    args = CLI.parse_args(sys.argv[1:])
    syncer.main(args)
