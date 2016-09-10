from time import time
import logging

from bhindex.util import validAvailability, Counter
from distdb import ANY

log = logging.getLogger("vacuum")


def prepare_args(parser, config):
    parser.add_argument("--wipe", metavar="DAYS", action="store", dest="wipe", type=int,
                        help="Wipe assets unavailable for at least given DAYS")
    parser.set_defaults(main=main)


def wipe(config, db, days):
    seconds = -(3600 * 24 * days)
    t = time()

    total = Counter()
    wiped = Counter()
    for obj in db.query({'bh_availability': ANY}):
        total.inc()
        if (validAvailability(obj, t) or 0) < seconds:
            del db[obj]
            wiped.inc()
    log.info("Wiped %d objects out of %d (unavailable for > %d days)", wiped, total, days)


def main(args, config, db):
    if args.wipe:
        wipe(config, db, args.wipe)

    db.vacuum()
    db.commit()
