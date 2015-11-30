def prepare_args(parser, config):
    parser.set_defaults(main=main)


def main(args, config, db):
    db.vacuum()
    db.commit()
