from . import add, cat, config, links, scanner, syncer, tree, vacuum


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
    subparsers = CLI.add_subparsers(title="Sub-commands")

    Add = subparsers.add_parser('add', help='Add files to BitHorde and BHIndex')
    add.prepare_args(Add, cfg)

    Cat = subparsers.add_parser('cat', help='Read one or more files from BitHorde')
    cat.prepare_args(Cat, cfg)

    ExportLinks = subparsers.add_parser('link', help='Exports the bhindex-files to a folder of symlinks')
    links.prepare_args(ExportLinks, cfg)

    LS = subparsers.add_parser('ls', help='List files in a directory of BHIndex')
    tree.prepare_ls_args(LS, cfg)

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
        db = DB(args.db)
        args.main(args, cfg, db)
    except ArgumentError, e:
        CLI.error(e)
