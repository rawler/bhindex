from __future__ import absolute_import

import socket, re, logging
from cStringIO import StringIO
from collections import deque

from .protocol import decodeMessage, encodeMessage, message

import concurrent

logger = logging.getLogger(__name__)

class Connection:
    def __init__(self, tgt, name=None):
        if hasattr(tgt, 'recv'):
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
        self._socket = concurrent.connect(tgt, family)

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
        if new:
            return new
        else:
            raise StopIteration

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
        self._statusWatch = concurrent.Event()
        self._pendingReads = dict()

    def close(self):
        if self._handle is not None:
            self._client._close(self._handle)
        self._client = self._handle = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __delete__(self):
        self.close()

    def status(self):
        if self._statusWatch:
            if not self._client:
                return None

            with concurrent.Timeout((self._client.config['asset_timeout']+100) / 1000.0, False):
                return self._statusWatch.wait()

            logger.debug("Status() timeout on %s:%d", self._client, self._handle)
            return None
        else:
            return self._status

    def _processStatus(self, status):
        self._status = status
        if self._statusWatch:
            self._statusWatch.send(status)
            self._statusWatch = None

    def read(self, offset, size):
        assert self._client, self._handle

        timeout = (self._client.config['asset_timeout'] * 3)
        request = message.Read.Request(handle = self._handle, offset=offset, size=size, timeout=timeout)

        response = self._client._read(request)

        return response and response.content

class Client:
    def __init__(self, config, autoconnect=True):
        self.config = config
        self._assets = {}
        self._handleAllocator = _Allocator()

        self._reqIdAllocator = _Allocator()
        self._pendingReads = dict()

        self._pool = concurrent.Pool(config['pressure'])

        if autoconnect:
            self.connect()

    def connect(self, address=None):
        config = self.config
        if not address:
            address = config['address']
        self._connection = Connection(address, config.get('myname', 'bhindex'))
        self._reader_greenlet = concurrent.spawn(self._reader)

    def __repr__(self):
        return "Client(peername=%s)" % self._connection.peername

    def open(self, hashIds):
        handle = self._handleAllocator.alloc()
        asset = Asset(self, handle)
        self._assets[handle] = asset
        self._connection.send(message.BindRead(handle=handle, ids=hashIds, timeout=self.config['asset_timeout']))
        return asset

    def _read(self, request):
        response = None
        retries = 0
        reqId = self._reqIdAllocator.alloc()
        try:
            request.reqId = reqId
            respond = concurrent.Event()
            self._pendingReads[reqId] = respond
            self._connection.send(request)
            timeout = (request.timeout + 250) / 1000.0

            while not response and retries < 3:
                retries += 1
                with concurrent.Timeout(timeout, False):
                    response = respond.wait()
        finally:
            del self._pendingReads[reqId]
            self._reqIdAllocator.free(reqId)

        return response

    def pool(self):
        return self._pool

    def _close(self, handle):
        self._assets[handle] = None
        self._connection.send(message.BindRead(handle=handle, timeout=1000*3600))

    def _reader(self):
        for msg in self._connection:
            if isinstance(msg, message.AssetStatus):
                self._processStatus(msg)
            elif isinstance(msg, message.Read.Response):
                self._processReadResponse(msg)
            elif isinstance(msg, message.Ping):
                self._connection.send(message.Ping())
            else:
                print "Unhandled message: ", type(msg), msg
        # TODO: handle closing assets on connection close

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
                logger.debug("Ignoring late %s response on closing asset", message._STATUS.values_by_number[status.status].name)
            else:
                del self._assets[handle]
                self._handleAllocator.free(handle)

    def _processReadResponse(self, response):
        try:
            event = self._pendingReads[response.reqId]
            self._pendingReads[response.reqId] = None
        except KeyError:
            logger.warn("Ignoring unrecognized %s ReadResponse", str(response)[:1024])
            return
        if event:
            event.send(response)
        else:
            logger.warn("ReadResponse %d recieved twice", response.reqId)

def parseConfig(c):
    return dict(
        pressure = int(c['pressure']),
        address = c['address'],
        asset_timeout = int(c['asset_timeout']),
    )
