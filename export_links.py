#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from argparse import ArgumentParser
from bhindex import links

if __name__ == '__main__':
    CLI = ArgumentParser(
        description='export_links.py - Deprecated, see bhindex.py instead',
    )
    links.prepare_args(CLI)
    args = CLI.parse_args(sys.argv[1:])
    links.main(args)
