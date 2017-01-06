from distdb.syncer import Syncer


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


def prepare_args(parser, config):
    parser.add_argument("--volatile", action="store_true", dest="volatile", default=False,
                        help="Improve I/O write-performance at expense of durability. Might be worth it during initial sync.")
    parser.set_defaults(main=main)


def main(args, config, db):
    db.set_volatile(args.volatile)
    sync_config = config.items('LIVESYNC')
    connect_addresses = set(parse_addr(addr)
                            for addr in sync_config['connect'].split(","))
    sync_config = {
        'name': sync_config['name'],
        'port': int(sync_config['port']),
        'connect_addresses': connect_addresses,
        'db_poll_interval': float(sync_config['db_poll_interval']),
    }

    Syncer(db, **sync_config).wait()
