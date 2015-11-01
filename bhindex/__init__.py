from . import links, syncer, vacuum


def main(args):
    from argparse import ArgumentError, ArgumentParser

    CLI = ArgumentParser(description='BHIndex - Distributed Filesystem using BitHorde')
    subparsers = CLI.add_subparsers(title="Sub-commands")

    ExportLinks = subparsers.add_parser('link', help='Exports the bhindex-files to a folder of symlinks')
    links.prepare_args(ExportLinks)

    Syncer = subparsers.add_parser('syncer', help='Runs online synchronization with other bhindex')
    syncer.prepare_args(Syncer)

    Vacuum = subparsers.add_parser('vacuum', help='Runs routine DB-maintenance')
    vacuum.prepare_args(Vacuum)

    args = CLI.parse_args(args)
    try:
        args.main(args)
    except ArgumentError, e:
        CLI.error(e)