from __future__ import absolute_import

from sys import stdout

from .tree import Filesystem, Path
from .bithorde import proto


def prepare_args(parser, config):
    parser.add_argument("path", nargs='+',
                        help="One or more files to stream to stdout")
    parser.set_defaults(main=main)


class NotFoundError(Exception):
    pass


class Cat(object):
    def __init__(self, fs, bithorde):
        self.fs = fs
        self.bithorde = bithorde

    def __call__(self, path):
        print "x", path
        f = self.fs.lookup(path)
        with self.bithorde.open(f.ids().proto_ids()) as asset:
            status = asset.status()
            if status.status != proto.SUCCESS:
                raise NotFoundError(
                    "File %s (%s) not found in BitHorde (%s)" % (path, ",".join(f.ids()), status))
            for chunk in asset:
                yield chunk


def main(args, config, db):
    from bithorde import Client, parseConfig

    bithorde = Client(parseConfig(config.items('BITHORDE')))
    c = Cat(Filesystem(db), bithorde)

    for path in args.path:
        for chunk in c(Path(path)):
            stdout.write(chunk)
