from time import time
import logging

from bhindex.util import validAvailability, Counter
from distdb import Key

log = logging.getLogger("vacuum")


def prepare_args(parser, config):
    parser.add_argument("--wipe", metavar="SCORE", action="store", dest="wipe", type=int,
                        help="Wipe assets below negative SCORE availability. Typically '100000'")
    parser.set_defaults(main=main)


def wipe(config, db, availability):
    t = time()

    total = Counter()
    wiped = Counter()
    with db.transaction() as tr:
        for obj in db.query(Key('bh_availability').any()):
            total.inc()
            if (validAvailability(obj, t) or 0) < availability:
                tr.delete(obj, t)
                wiped.inc()
        tr.yield_from(1)
    log.info("Wiped %d objects out of %d (availability < %d)", wiped, total, availability)


def main(args, config, db):
    if args.wipe:
        wipe(config, db, -args.wipe)

    db.vacuum()
