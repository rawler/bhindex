#!/usr/bin/env python

from db.syncer import Syncer

import db
import config


def parse_addr(addr):
    addr = addr.strip()
    if not addr:
        return None
    if addr[0] == '/':
        return addr
    addr = addr.rsplit(':', 1)
    if len(addr) == 2:
        return addr[0], int(addr[1])
    else:
        return addr[0]

if __name__ == '__main__':
    import cliopt

    usage = """usage: %prog [options] [<format>:<url>] ...
    where <format> is either 'json' or 'magnetlist'"""
    parser = cliopt.OptionParser(usage=usage)
    parser.add_option("-s", "--no-sync", action="store_false", dest="sync", default=True,
                      help="Improve I/O write-performance at expense of durability. Might be worth it during initial sync.")

    (options, args) = parser.parse_args()

    config = config.read()
    sync_config = config.items('LIVESYNC')
    connect_addresses = set(parse_addr(addr)
                            for addr in sync_config['connect'].split(","))
    sync_config = {
        'name': sync_config['name'],
        'port': int(sync_config['port']),
        'connect_addresses': connect_addresses,
        'db_poll_interval': float(sync_config['db_poll_interval']),
    }
    Syncer(db=db.open(config.get('DB', 'file'), sync=options.sync), **sync_config).wait()
