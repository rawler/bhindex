from types import MethodType
from threading import Thread

import pyhorde.bithorde as bithorde
from pyhorde.bithorde import connectUNIX, reactor, message, b32decode
import pyhorde.bithorde_pb2 as message

import config

CONFIG = config.read()
PRESSURE = int(CONFIG.get('BITHORDE', 'pressure'))

class Client(bithorde.Client):
    def onDisconnected(self, reason):
        if not self.closed:
            print "Disconnected; '%s'" % reason
            try: reactor.stop()
            except: pass

    def onFailed(self, reason):
        print "Failed to connect to BitHorde; '%s'" % reason
        reactor.stop()

class Querier(object):
    def __init__(self, client, callback):
        self._client = client
        self._callback = callback
        self._queue = list()
        self._requestCount = 0

    def submit(self, hashIds, key):
        if self._requestCount < PRESSURE:
            reactor.callFromThread(self._request, hashIds, key)
        else:
            self._queue.append((hashIds, key))

    def clear(self):
        self._queue = list()

    def _request(self, hashIds, key):
        asset = bithorde.Asset()
        asset.key = key
        asset.onStatusUpdate = MethodType(self._gotResponse, asset, bithorde.Asset)
        self._client.allocateHandle(asset)
        asset.bind(hashIds)

        self._requestCount += 1

    def _gotResponse(self, asset, status):
        bithorde.Asset.onStatusUpdate(asset, status)
        self._callback(asset, status, asset.key)
        asset.close() # TODO: Improve performance by re-using asset.

        self._requestCount -= 1
        if self._queue:
            (hashIds, key) = self._queue.pop(0)
            self._request(hashIds, key) # Request more, if needed

class BitHordeIteratorClient(Client):
    def __init__(self, assets, onStatusUpdate):
        self.assets = assets
        self.onStatusUpdate = onStatusUpdate

    def onConnected(self):
        self.ai = bithorde.AssetIterator(self, self.assets, self.onStatusUpdate, self.whenDone, parallel=PRESSURE)

    def whenDone(self):
        self.close()
        reactor.stop()

class QueryThread(Client, Thread):
    def __init__(self, config=CONFIG):
        self.config = config
        Thread.__init__(self)
        self.daemon = True
        self.start()

    def run(self):
        bithorde.connectUNIX(self.config.get('BITHORDE', 'unixsocket'), self)
        bithorde.reactor.run(installSignalHandlers=0)

    def onConnected(self):
        self._querier = Querier(self, self._callback)

    def _callback(self, asset, status, key):
        callback, key = key
        callback(asset, status, key)

    def start(self):
        from time import sleep
        Thread.start(self)
        while not hasattr(self, '_querier'):
            sleep(0.05)

    def __call__(self, tiger_id, callback, key):
        self._querier.submit({bithorde.message.TREE_TIGER: bithorde.b32decode(tiger_id)}, (callback, key))

    def clear(self):
        self._querier.clear()

if __name__ == '__main__':
    from threading import Thread
    from time import sleep
    import sys

    def onResult(asset, status, key):
        print asset, status, key


    if len(sys.argv) > 1:
        assetIds = (({message.TREE_TIGER: b32decode(asset)}, asset) for asset in sys.argv[1:])

        querier = QueryThread()

        querier = c.querier
        for hashId, key in assetIds:
            querier.submit(hashId, key)
            sleep(0.05)
        sleep(5)
    else:
        print "Usage: %s <tiger tree hash: base32> ..." % sys.argv[0]
