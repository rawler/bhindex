#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from argparse import ArgumentParser
from bhindex import add

if __name__ == '__main__':
    CLI = ArgumentParser(
        description='add.py - Deprecated, see bhindex.py instead',
    )
    add.prepare_args(CLI)
    args = CLI.parse_args(sys.argv[1:])
    add.main(args)
