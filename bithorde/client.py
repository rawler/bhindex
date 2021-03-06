from __future__ import absolute_import

from collections import deque
from contextlib import closing
from time import time
import logging
import os
import re
import socket

from .protocol import decodeMessage, encodeMessage, message
from .util import fsize, read_in_chunks
from thread_io import sleep, spawn, Promise, Timeout, ThreadPool

logger = logging.getLogger(__name__)

RECONNECT_INTERVAL = 2


class Connection:
    HOST_PORT_RE = re.compile(r"\s*(\S+):(\d+)\s*")

    def __init__(self, tgt, name):
        if hasattr(tgt, 'recv'):
            self._socket = tgt
        else:
            self._connect(tgt)
        self._address = tgt
        self.buf = ""
        self.auth(name)

    @property
    def address(self):
        if isinstance(self._address, basestring):
            return self._address
        else:
            return self._address.getpeername()

    def _connect(self, tgt):
        m = self.HOST_PORT_RE.match(tgt)
        if m:
            tgt = (m.group(1), int(m.group(2)))
        if isinstance(tgt, tuple):
            family = socket.AF_INET
        else:
            family = socket.AF_UNIX
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.connect(tgt)
        self._socket = sock

    def send(self, msg):
        msgbuf = encodeMessage(msg)
        self._socket.sendall(msgbuf)

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
        new = self._socket.recv(128 * 1024)
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


class BaseAsset(object):
    def __init__(self, client, handle):
        self._client = client
        self._handle = handle

    def close(self):
        if self._handle is not None:
            closer = ClosingAsset(self._client, self._handle, self._status.get())
            self._client._register_closer(self._handle, closer)
        self._client = self._handle = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __delete__(self):
        self.close()


class Asset(BaseAsset):
    def __init__(self, client, handle, hashIds):
        super(Asset, self).__init__(client, handle)
        self._status = Promise()
        self._pendingReads = dict()
        self.requested_ids = hashIds

    def status(self):
        if not self._client:
            return None

        timeout = (self._client.config['asset_timeout'] + 200) / 1000.0
        try:
            return self._status.wait(timeout)
        except Timeout:
            logger.debug("Status() timeout on %s:%d", self._client, self._handle)
            return None

    def _processStatus(self, status):
        old_status = self._status.get()
        old_status = old_status and old_status.status
        self._status.set(status)
        new_status = status and status.status
        client = self._client
        if client and old_status != message.SUCCESS and new_status == message.SUCCESS:
            client._request_pending_reads(self)

    def read(self, offset, size):
        assert self._client, self._handle

        timeout = (self._client.config['asset_timeout'] * 3)
        request = message.Read.Request(handle=self._handle, offset=offset, size=size, timeout=timeout)

        response = self._client._read(request)

        return response and response.content

    def __iter__(self):
        offset = 0
        size = self.status().size
        while offset < size:
            chunk = self.read(offset, 128 * 1024)
            yield chunk
            offset += len(chunk)


class UploadAsset(BaseAsset):
    def __init__(self, client, handle):
        super(UploadAsset, self).__init__(client, handle)
        self.status = self._status = Promise()
        self.result = Promise()
        self.progress = 0

    def wait_for_ok(self):
        ack = self.status.wait()
        if ack.status != message.SUCCESS:
            stat = message._STATUS.values_by_number[ack.status].name
            raise Exception("Upload start failed with %s" % stat)

    def write_all(self, f):
        offset = 0
        for block in read_in_chunks(f, 64 * 1024):
            self._client._connection.send(message.DataSegment(handle=self._handle, offset=offset, content=block))
            offset += len(block)

    def wait_for_result(self, timeout=None):
        result = self.result.wait(timeout)
        if result.status != message.SUCCESS:
            stat = message._STATUS.values_by_number[result.status].name
            raise Exception("Upload start failed with %s" % stat)
        return result

    def _processStatus(self, status):
        self.status.set(status)
        if status.ids or status.status != message.SUCCESS:
            self.result.set(status)


class ClosingAsset(object):
    def __init__(self, client, handle, status):
        self.client = client
        self.handle = handle
        self.status = status
        self.remind()

    def _processStatus(self, status):
        if status.ids or status.status != message.NOTFOUND:
            if self.status and self.status.status != status.status:
                logger.debug("Ignoring late %s response on closing asset (%s)", message._STATUS.values_by_number[status.status].name)
        else:
            self.client._release(self.handle)

    def remind(self):
        self.next_reminder = time() + 1
        self.client._send(message.BindRead(handle=self.handle, timeout=1000 * 3600))


class Client:
    def __init__(self, config, autoconnect=True):
        self.config = config
        self._connection = None
        self._assets = {}
        self._handleAllocator = _Allocator()

        self._reqIdAllocator = _Allocator()
        self._pendingReads = dict()

        self._pool = ThreadPool(config['pressure'])

        if autoconnect:
            self.connect()

    def connect(self, address=None):
        c = self._connect(address)
        self._on_connected(c)

    def _connect(self, address):
        config = self.config
        if not address:
            address = config['address']
        return Connection(address, config.get('myname', 'bhindex'))

    def _on_connected(self, connection):
        assert self._connection is None
        self._connection = connection
        logger.info("Connected to: %s", connection.peername)
        for handle, a in self._assets.iteritems():
            assert(isinstance(a, Asset))
            self._bind(handle, a.requested_ids)
        spawn(self._reader)

    def _on_disconnected(self, connection):
        if connection is not self._connection:
            return
        self._connection = None
        logger.warn("Disconnected: %s", connection.peername)
        self._clear_asset_statuses()
        self._reconnect(connection.address)

    def _clear_asset_statuses(self):
        for handle, a in self._assets.items():
            if a is None or isinstance(a, ClosingAsset):
                self.release(handle)
            elif isinstance(a, UploadAsset):
                self.release(handle)
                a.status.set(None)
                a.result.set(None)
            else:
                a._status.set(None)

    def _reconnect(self, address):
        while True:
            try:
                c = self._connect(address)
            except IOError:
                sleep(self.config.get('reconnect_interval', RECONNECT_INTERVAL))
            else:
                return self._on_connected(c)

    def __repr__(self):
        if self._connection:
            return "Client(peername=%s)" % self._connection.peername
        else:
            return "Client(disconnected)"

    def _send(self, msg):
        c = self._connection
        if not c:
            return
        try:
            c.send(msg)
        except IOError:
            self._on_disconnected(c)

    def _bind(self, handle, hashIds):
        self._send(message.BindRead(handle=handle, ids=hashIds, timeout=self.config['asset_timeout']))

    def open(self, hashIds):
        handle = self._handleAllocator.alloc()
        asset = Asset(self, handle, hashIds)
        self._assets[handle] = asset
        self._bind(handle, hashIds)
        return asset

    def _prepare_upload(self):
        handle = self._handleAllocator.alloc()
        asset = UploadAsset(self, handle)
        self._assets[handle] = asset
        return asset

    def _release(self, handle):
        del self._assets[handle]
        self._handleAllocator.free(handle)

    def upload(self, f):
        if isinstance(f, basestring):
            f = open(f, 'rb')
        with closing(f), self._prepare_upload() as asset:
            self._connection.send(message.BindWrite(handle=asset._handle, size=fsize(f)))
            asset.wait_for_ok()
            asset.write_all(f)
            return asset.wait_for_result()

    def link(self, path):
        with self._prepare_upload() as asset:
            path = os.path.abspath(path)
            self._connection.send(message.BindWrite(handle=asset._handle, linkpath=path))
            asset.wait_for_ok()
            return asset.wait_for_result()

    def _read(self, request):
        retries = 0
        reqId = self._reqIdAllocator.alloc()
        try:
            request.reqId = reqId
            response = Promise()
            response.request = request
            self._pendingReads[reqId] = response
            timeout = (request.timeout + 250) / 1000.0

            while retries < 3:
                self._send(request)
                try:
                    return response.wait(timeout)
                except Timeout:
                    retries += 1
        finally:
            del self._pendingReads[reqId]
            self._reqIdAllocator.free(reqId)

    def _request_pending_reads(self, asset):
        for pr in self._pendingReads.itervalues():
            if pr and self._assets.get(pr.request.handle) is asset:
                self._send(pr.request)

    def pool(self):
        return self._pool

    def _register_closer(self, handle, closer):
        self._assets[handle] = closer
        now = time()
        for a in self._assets.values():
            try:
                if a.next_reminder < now:
                    a.remind()
            except AttributeError:
                pass

    def _reader(self):
        c = self._connection
        msgs = iter(c)
        while True:
            try:
                msg = next(msgs)
            except IOError:
                return self._on_disconnected(c)
            except StopIteration:
                return self._on_disconnected(c)

            if isinstance(msg, message.AssetStatus):
                self._processStatus(msg)
            elif isinstance(msg, message.Read.Response):
                self._processReadResponse(msg)
            elif isinstance(msg, message.Ping):
                self._send(message.Ping())
            else:
                logger.warn("Unhandled message: %s %s", type(msg), msg)

    def _processStatus(self, status):
        handle = status.handle
        try:
            asset = self._assets[handle]
        except KeyError:
            logger.warn("got status about unkown asset: %s", status)
            return

        asset._processStatus(status)

    def _processReadResponse(self, response):
        try:
            promise = self._pendingReads[response.reqId]
            self._pendingReads[response.reqId] = None
        except KeyError:
            logger.warn("Ignoring unrecognized %s ReadResponse", str(response)[:1024])
            return
        if promise:
            promise.set(response)
        else:
            logger.warn("ReadResponse %d recieved twice", response.reqId)


def parseConfig(c):
    return dict(
        pressure=int(c['pressure']),
        address=c['address'],
        asset_timeout=int(c['asset_timeout']),
    )
