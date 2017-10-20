from contextlib import contextmanager
import logging
from . import add, cat, config, fusefs, links, scanner, syncer, tree, vacuum


@contextmanager
def noop_context_manager():
    yield


def main(args=None):
    if not args:
        from sys import argv
        args = argv[1:]

    from argparse import ArgumentError, ArgumentParser
    from distdb import DB

    cfg = config.read()

    CLI = ArgumentParser(description='BHIndex - Distributed Filesystem using BitHorde')
    CLI.add_argument('--database', '--db', dest="db", default=cfg.get('DB', 'file'),
                     help="Path to the SQLite database")
    CLI.add_argument('--setuid', dest="suid", help="Set username before running")
    CLI.add_argument('--verbose', '-v', action="store_true",
                     help="write debug-level output")
    CLI.set_defaults(setup=lambda args, cfg, db: (noop_context_manager(), (args, cfg, db)))
    subparsers = CLI.add_subparsers(title="Sub-commands")

    Add = subparsers.add_parser('add', help='Add files to BitHorde and BHIndex')
    add.prepare_args(Add, cfg)

    Cat = subparsers.add_parser('cat', help='Read one or more files from BitHorde')
    cat.prepare_args(Cat, cfg)

    ExportLinks = subparsers.add_parser('link', help='Exports the bhindex-files to a folder of symlinks')
    links.prepare_args(ExportLinks, cfg)

    LS = subparsers.add_parser('ls', help='List files in a directory of BHIndex')
    tree.prepare_ls_args(LS, cfg)

    MOUNT = subparsers.add_parser('mount', help='Mount bhindex as a FUSE file system')
    fusefs.prepare_args(MOUNT, cfg)

    MV = subparsers.add_parser('mv', help='Move a file or directory in the bithorde tree')
    tree.prepare_mv_args(MV, cfg)

    Scanner = subparsers.add_parser('update', help='Scans for asset-availability in bithorde and updates DB')
    scanner.prepare_args(Scanner, cfg)

    Syncer = subparsers.add_parser('syncer', help='Runs online synchronization with other bhindex')
    syncer.prepare_args(Syncer, cfg)

    Vacuum = subparsers.add_parser('vacuum', help='Runs routine DB-maintenance')
    vacuum.prepare_args(Vacuum, cfg)

    args = CLI.parse_args(args)
    try:
        if args.verbose:
            lvl = logging.DEBUG
        else:
            lvl = logging.INFO
        logging.basicConfig(level=lvl, format="%(levelname)-8s %(asctime)-15s <%(name)s> %(message)s")
        logging.getLogger().setLevel(lvl)

        db = DB(args.db)

        ctx, main_args = args.setup(args, cfg, db)
        if args.suid:
            from pwd import getpwnam
            from os import setuid
            setuid(getpwnam(args.suid).pw_uid)

        with ctx:
            args.main(*main_args)
    except ArgumentError, e:
        CLI.error(e)
