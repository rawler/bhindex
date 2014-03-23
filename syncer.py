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
            self._db.set_sync_state(self.peername, last_received=self._last_serial_received)

        return True

    def _sendmsg(self, field, msg):
        enc = encoder.MessageEncoder(FIELD_MAP[field].number, False, False)
        buf = StringIO()
        if hasattr(msg, '__iter__'):
            for msg in msg:
                enc(buf.write, msg)
        else:
            enc(buf.write, msg)
        self._sock.sendall(buf.getvalue())

    def _fill_buf(self):
        res = self._sock.recv(512*1024)
        if not res:
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
        self._sock.close()

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

    def db_push(self):
        last_serial = self._last_serial_sent
        msgs = list()
        for obj, key, tstamp, serial, values in self._db.get_public_mappings_after(last_serial):
            msgs.append(sync_pb2.Update(obj=obj, key=key, tstamp=int(tstamp), serial=serial, values=values))
            last_serial = max(last_serial, serial)
        self._sendmsg('update', msgs)
        self._last_serial_sent = last_serial

class SyncServer(object):
    def __init__(self, db, name, port, connect, db_poll_interval=0.5):
        self._db = db
        self.name = name
        self.port = port
        self.connections = dict()
        self.connectors = connect
        self._sock = eventlet.listen(('0.0.0.0', port))
        self._server = eventlet.spawn(eventlet.serve, self._sock, self._spawn)
        self._pusher = eventlet.spawn(self._db_push)
        self._connector = eventlet.spawn(self._connector)

    def _spawn(self, sock, client_addr):
        conn = SyncConnection(self._db, self.name, sock)
        if conn.handshake():
            peername = conn.peername
            print '%s connected from %s' % (peername, client_addr)
            self.connections[peername] = conn
            try:
                conn.run()
            finally:
                self.connections.pop(peername, None)

    def _connector(self):
        def _spawn(sock, addr):
            try:
                self._spawn(sock, addr)
            finally:
                self.connectors.add(addr)

        while True:
            for addr in set(self.connectors):
                sock = None
                try:
                    print addr
                    sock = eventlet.connect(addr)
                except Exception, e:
                    print e
                    pass
                if sock:
                    self.connectors.remove(addr)
                    eventlet.spawn(_spawn, sock, addr)
            eventlet.greenthread.sleep(3)


    def _db_push(self):
        while True:
            for conn in self.connections.values():
                try:
                    conn.db_push()
                except Exception, e:
                    conn.close()
                    self.connections.pop(conn.peername, None)
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
sync_config = dict(x for x in config.items('LIVESYNC'))
sync_config = {
    'name': sync_config['name'],
    'port': int(sync_config['port']),
    'connect': set(parse_addr(addr) for addr in sync_config['connect'].split(",")),
    'db_poll_interval': float(sync_config['db_poll_interval']),
}
SyncServer(db=db.open(config), **sync_config).wait()
