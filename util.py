from bithorde_eventlet import parseHashIds, message
from time import time
from db import ValueSet

ASSET_WAIT_FACTOR = 0.01
def cachedAssetLiveChecker(bithorde, assets, db=None):
    t = time()

    def hasValidStatus(dbAsset):
        try:
            dbStatus = dbAsset['bh_status']
            dbConfirmedStatus = dbAsset['bh_status_confirmed']
        except KeyError:
            return None
        stable = dbConfirmedStatus.t - dbStatus.t
        nextCheck = dbConfirmedStatus.t + (stable * ASSET_WAIT_FACTOR)
        if t < nextCheck:
            return dbStatus
        else:
            return None

    def checkAsset(dbAsset):
        dbStatus = hasValidStatus(dbAsset)
        if dbStatus:
            return dbAsset, bool(dbStatus.any())

        ids = parseHashIds(dbAsset['xt'])
        if not ids:
            return dbAsset, None

        with bithorde.open(ids) as bhAsset:
            status = bhAsset.status()
            status_ok = status.status == message.SUCCESS
            dbAsset[u'bh_status'] = ValueSet((unicode(status_ok),), t=t)
            dbAsset[u'bh_status_confirmed'] = ValueSet((unicode(t),), t=t)
            if status.size is not None:
                dbAsset[u'filesize'] = ValueSet((unicode(status.size),), t=t)
            if db:
                with db.transaction():
                    db.update(dbAsset)

            return dbAsset, status_ok

    return bithorde.pool().imap(checkAsset, assets)

class Counter(object):
    def __init__(self):
        self.i = 0

    def reset(self):
        self.i = 0

    def inc(self, i = 1):
        self.i += i
        return self.i

    def __int__(self):
        return self.i

    def inGibi(self):
        return self.i / (1024*1024*1024.0)


