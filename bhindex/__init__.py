from . import links, syncer


def main(args):
    from argparse import ArgumentError, ArgumentParser

    CLI = ArgumentParser(description='BHIndex - Distributed Filesystem using BitHorde')
    subparsers = CLI.add_subparsers(title="Sub-commands")

    ExportLinks = subparsers.add_parser('link', help='Exports the bhindex-files to a folder of symlinks')
    links.prepare_args(ExportLinks)

    Syncer = subparsers.add_parser('syncer', help='Runs online synchronization with other bhindex')
    syncer.prepare_args(Syncer)

    args = CLI.parse_args(args)
    try:
        args.main(args)
    except ArgumentError, e:
        CLI.error(e)
