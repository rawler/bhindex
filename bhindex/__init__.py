from . import links


def main(args):
    from argparse import ArgumentError, ArgumentParser

    CLI = ArgumentParser(description='BHIndex - Distributed Filesystem using BitHorde')

    subparsers = CLI.add_subparsers(title="Sub-commands")

    ExportLinks = subparsers.add_parser('link', help='Exports the bhindex-files to a folder of symlinks')
    links.prepare_args(ExportLinks)

    args = CLI.parse_args(args)
    try:
        args.main(args)
    except ArgumentError, e:
        CLI.error(e)
