import config
from db import DB


def prepare_args(parser):
    parser.set_defaults(main=main)


def main(args):
    db = DB(config.read().get('DB', 'file'))

    db.vacuum()
    db.commit()
