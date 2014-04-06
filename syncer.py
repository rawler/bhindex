#!/usr/bin/env python

import db, config
import sync_pb2
from cStringIO import StringIO

import eventlet
from eventlet.green import socket
from google.protobuf.message import Message

from pyhorde.bithorde import encoder, decodeMessage

FIELD_MAP=sync_pb2._STREAM.fields_by_name

from datetime import datetime

class SyncConnection(object):
    def __init__(self, db, name, sock):
        self._sock = sock
        self._db = db
        self.name = name
        self._buf = ''

        self.peername = None
        self._last_serial_received = 0

    def handshake(self):
        assert not self.peername
        self._sendmsg('hello', sync_pb2.Hello(name=self.name))
        peerhello = self.read_msg()
        self.peername = peerhello.name
        self._last_serial_received = self._db.get_sync_state(self.peername)['last_received']
        print "Last last_received", self._last_serial_received
        self._sendmsg('setup', sync_pb2.Setup(
            last_serial_received=self._last_serial_received,
            last_serial_in_db=self._db.last_serial(),
        ))

        peersetup = self.read_msg()
        if peersetup.last_serial_received <= self._db.last_serial():
            self._last_serial_sent = peersetup.last_serial_received
        else:
            print "Warning: detecting local db was reset"
            self._last_serial_sent = 0

        if peersetup.last_serial_in_db < self._last_serial_received:
            print "Warning: detecting remote db was reset"
            self._last_serial_received = 0

        return self

    def _sendmsg(self, field, msg):
        enc = encoder.MessageEncoder(FIELD_MAP[field].number, False, False)
        buf = StringIO()
        if hasattr(msg, '__iter__'):
            for msg in msg:
                enc(buf.write, msg)
        else:
            enc(buf.write, msg)
        self._sock and self._sock.sendall(buf.getvalue())

    def _fill_buf(self):
        res = self._sock and self._sock.recv(64*1024)
        if not res:
            self.close()
            raise StopIteration
        self._buf += res

    def _dequeue_msg(self):
        msg, consumed = decodeMessage(self._buf, msg_map=sync_pb2._STREAM.fields_by_number)
        self._buf = self._buf[consumed:]
        return msg

    def read_chunked(self):
        while True:
            self._fill_buf()
            chunk = []

            try:
                while True:
                    chunk.append(self._dequeue_msg())
            except IndexError:
                pass
            yield chunk

    def read_msg(self):
        while True:
            try:
                return self._dequeue_msg()
            except IndexError:
                self._fill_buf()

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def run(self):
        last_serial=self._last_serial_received
        for chunk in self.read_chunked():
            chunk_applied = 0
            for msg in chunk:
                if self._db.update_attr(msg.obj, msg.key, db.ValueSet(msg.values, t=msg.tstamp)):
                    chunk_applied += 1
                last_serial = max(last_serial, msg.serial)

            print datetime.now(), "Commit %d/%d" % (chunk_applied, len(chunk))
            self._last_serial_received = last_serial
            self._db.set_sync_state(self.peername, last_received=last_serial)
            self._db.commit()
            eventlet.sleep()

    def db_push(self):
        last_serial = self._last_serial_sent
        msgs = list()
        for obj, key, tstamp, serial, values in self._db.get_public_mappings_after(last_serial):
            msgs.append(sync_pb2.Update(obj=obj, key=key, tstamp=int(tstamp), serial=serial, values=values))
            last_serial = max(last_serial, serial)
        self._sendmsg('update', msgs)
        self._last_serial_sent = last_serial

class SyncServer(object):
    def __init__(self, db, name, port, connectAddresses, db_poll_interval=0.5):
        self._db = db
        self.name = name
        self.port = port
        self.connections = dict()
        self.connectAddresses = connectAddresses
        self._sock = eventlet.listen(('0.0.0.0', port))
        self._server = eventlet.spawn(eventlet.serve, self._sock, self._spawn)
        self._pusher = eventlet.spawn(self._db_push)
        self._connector = eventlet.spawn(self._connector)

    def _connectPeer(self, conn, connectAddress=None):
        peername = conn.peername
        connected = self.connections.get(peername, None)
        if connected: # Was already connected
            if connectAddress and not getattr(connected, 'connectAddress', None):
                connected.connectAddress = connectAddress
                self.connectAddresses.discard(connectAddress)
            return None
        else:
            conn.connectAddress = connectAddress
            self.connectAddresses.discard(connectAddress)
            self.connections[peername] = conn
            return conn

    def _disconnectPeer(self, conn):
        self.connections.pop(conn.peername, None)
        connectAddress = getattr(conn, 'connectAddress', None)
        if connectAddress:
            self.connectAddresses.add(connectAddress)

    def _spawn(self, sock, client_addr, connectAddress=None):
        conn = SyncConnection(self._db, self.name, sock)
        try:
            conn.handshake()
        except Exception, e:
            print "Handshake failed: %s" % e
            return

        peername = conn.peername
        if client_addr == connectAddress:
            print 'connected to %s %s' % (peername, client_addr)
        else:
            print '%s connected from %s' % (peername, client_addr)
        if not self._connectPeer(conn, connectAddress):
            print '%s already connected. Dropping.' % peername
            return

        try:
            conn.run()
        except Exception, e:
            print "%s died with %s" % (peername, e)

        print '%s disconnected from %s' % (conn.peername, client_addr)
        self._disconnectPeer(conn)

    def _connector(self):
        def _connect(addr):
            print addr
            try:
                return addr, eventlet.connect(addr)
            except Exception, e:
                print e
            return addr, None

        connectPool = eventlet.GreenPool(20)
        while True:
            for address, sock in connectPool.imap(_connect, set(self.connectAddresses)):
                if sock:
                    eventlet.spawn(self._spawn, sock, address, connectAddress=address)
            eventlet.greenthread.sleep(30)

    def _db_push(self):
        while True:
            for conn in self.connections.values():
                try:
                    conn.db_push()
                except Exception, e:
                    print "%s push hit error %s" % (conn.peername, e)
                    conn.close()
            eventlet.greenthread.sleep(0.5)

    def wait(self):
        self._server.wait()
        self._pusher.wait()
        self._connector.wait()

def parse_addr(addr):
    addr = addr.strip()
    if not addr:
        return None
    if addr[0] == '/':
        return addr
    addr = addr.rsplit(':', 1)
    if len(addr) == 2:
        return addr[0], int(addr[1])
    else:
        return addr[0]

config = config.read()
sync_config = config.items('LIVESYNC')
sync_config = {
    'name': sync_config['name'],
    'port': int(sync_config['port']),
    'connectAddresses': set(parse_addr(addr) for addr in sync_config['connect'].split(",")),
    'db_poll_interval': float(sync_config['db_poll_interval']),
}
SyncServer(db=db.open(config), **sync_config).wait()
