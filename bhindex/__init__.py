from . import add, links, scanner, syncer, tree, vacuum


def main(args):
    from argparse import ArgumentError, ArgumentParser

    CLI = ArgumentParser(description='BHIndex - Distributed Filesystem using BitHorde')
    subparsers = CLI.add_subparsers(title="Sub-commands")

    Add = subparsers.add_parser('add', help='Add files to BitHorde and BHIndex')
    add.prepare_args(Add)

    ExportLinks = subparsers.add_parser('link', help='Exports the bhindex-files to a folder of symlinks')
    links.prepare_args(ExportLinks)

    MV = subparsers.add_parser('mv', help='Move a file or directory in the bithorde tree')
    tree.prepare_mv_args(MV)

    Scanner = subparsers.add_parser('update', help='Scans for asset-availability in bithorde and updates DB')
    scanner.prepare_args(Scanner)

    Syncer = subparsers.add_parser('syncer', help='Runs online synchronization with other bhindex')
    syncer.prepare_args(Syncer)

    Vacuum = subparsers.add_parser('vacuum', help='Runs routine DB-maintenance')
    vacuum.prepare_args(Vacuum)

    args = CLI.parse_args(args)
    try:
        args.main(args)
    except ArgumentError, e:
        CLI.error(e)
