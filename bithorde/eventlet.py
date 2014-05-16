from __future__ import  absolute_import

import socket, re
from cStringIO import StringIO
from collections import deque
from base64 import b32decode

from .protocol import decodeMessage, encodeMessage, message

import eventlet

class Connection:
    def __init__(self, tgt, name=None):
        if isinstance(tgt, eventlet.greenio.GreenSocket):
            self._socket = tgt
        else:
            self._connect(tgt)
        self.buf = ""
        self.auth(name)
        self.peername = ""

    host_port_re = re.compile(r"\s*(\S+):(\d+)\s*")
    def _connect(self, tgt):
        m = self.host_port_re.match(tgt)
        if m:
            tgt = (m.group(1), int(m.group(2)))
        if isinstance(tgt, tuple):
            family = socket.AF_INET
        else:
            family = socket.AF_UNIX
        self._socket = eventlet.connect(tgt, family)

    def send(self, msg):
        msgbuf = encodeMessage(msg)
        self._socket.send(msgbuf)

    def close(self):
        self._socket.close()

    def __iter__(self):
        return self

    def next(self):
        while True:
            try:
                msg, consumed = decodeMessage(self.buf)
                self.buf = self.buf[consumed:]
                return msg
            except IndexError:
                self.buf += self._read()

    def _read(self):
        new = self._socket.recv(128*1024)
        if not new:
            raise StopIteration
        return new

    def auth(self, name):
        self.send(message.HandShake(name=name, protoversion=2))
        peerauth = self.next()
        self.peername = peerauth.name

class _Allocator:
    def __init__(self):
        self._next = iter(range(1024))
        self._freelist = deque()

    def alloc(self):
        if self._freelist:
            return self._freelist.popleft()
        else:
            return self._next.next()

    def free(self, x):
        self._freelist.append(x)

class Asset:
    def __init__(self, client, handle):
        self._client = client
        self._handle = handle
        self._status = None
        self._statusWatch = eventlet.event.Event()

    def close(self):
        if self._handle:
            self._client._close(self._handle)
        del self._client
        del self._handle

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def status(self):
        if self._statusWatch:
            return self._statusWatch.wait()
        else:
            return self._status

    def _processStatus(self, status):
        self._status = status
        if self._statusWatch:
            self._statusWatch.send(status)
            self._statusWatch = None

class Client:
    def __init__(self, config):
        self.config = config
        self._assets = {}
        self._handleAllocator = _Allocator()
        self._pool = eventlet.GreenPool(config['pressure'])
        self._connection = Connection(config['address'], config.get('myname', 'bhindex'))
        self._reader = eventlet.spawn(self._reader)

    def open(self, hashIds):
        handle = self._handleAllocator.alloc()
        asset = Asset(self, handle)
        self._assets[handle] = asset
        self._connection.send(message.BindRead(handle=handle, ids=hashIds, timeout=self.config['asset_timeout']))
        return asset

    def pool(self):
        return self._pool

    def _close(self, handle):
        self._assets[handle] = None
        self._connection.send(message.BindRead(handle=handle, timeout=1000*3600))

    def _reader(self):
        for msg in self._connection:
            if isinstance(msg, message.AssetStatus):
                self._processStatus(msg)
            elif isinstance(msg, message.Ping):
                self._connection.send(message.Ping())
            else:
                print "Unhandled message: ", type(msg), msg

    def _processStatus(self, status):
        handle = status.handle
        try:
            asset = self._assets[handle]
        except KeyError:
            print "Warning: got status about unkown asset", status
            return

        if asset:
            asset._processStatus(status)
        else:
            if status.ids or status.status == message.SUCCESS:
                # print "Ignoring late " + message._STATUS.values_by_number[status.status].name + " response on closing asset"
                pass
            else:
                del self._assets[handle]
                self._handleAllocator.free(handle)

tiger_hash = re.compile(r'tiger:(\w{39})')
def parseHashIds(ids):
    if not hasattr(ids, '__iter__'):
        ids = (ids,)

    res = list()
    for id in ids:
        m = tiger_hash.search(id)
        if m:
            try:
                id = b32decode(m.group(1)+'=')
                res.append(message.Identifier(type=message.TREE_TIGER, id=id))
            except TypeError:
                pass
        else:
            pass
    return res

def parseConfig(c):
    return dict(
        pressure = int(c['pressure']),
        address = c['address'],
        asset_timeout = int(c['asset_timeout']),
    )

if __name__ == '__main__':
    import sys
    client = Client({
        'pressure': 10,
        'address': '/tmp/bithorde.source',
        'asset_timeout': 3000
    })

    def check_asset(arg):
        ids = parseHashId(arg)
        if ids:
            with client.open(ids) as asset: # Bithorde assets are context-managers, YAY!
                return asset.status()
        else:
            print "Warning, failed to parse: ", addr

    # Run a bunch in parallel, controlled by a pool
    for status in client.pool().imap(check_asset, sys.argv[1:]):
        if status:
            print status
