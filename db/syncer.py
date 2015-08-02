import logging
from cStringIO import StringIO
from obj import ValueSet

import concurrent
from concurrent import socket

from db.serialize import MESSAGE_DECODER, MESSAGE_ENCODER, MessageQueue
from db import sync_pb2

FORMAT = "%(levelname)-8s %(asctime)-15s <%(name)s> %(message)s"
logging.basicConfig(level=logging.DEBUG, format=FORMAT)


class SyncConnection(object):
    def __init__(self, db, name, sock):
        self._sock = sock
        self._db = db
        self.name = name
        self._msg_queue = MessageQueue(MESSAGE_DECODER)
        self._echo_prevention = set()
        self._log = logging.getLogger('[anon]')

        self.peername = None
        self._last_serial_received = 0

        # Configure TCP keepalive
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 15)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def handshake(self):
        assert not self.peername
        self._sendmsg(['hello', sync_pb2.Hello(name=self.name)])
        peerhello = self.read_msg()
        self.peername = peerhello.name
        self._last_serial_received = self._db.get_sync_state(self.peername)['last_received']
        self._sendmsg(['setup', sync_pb2.Setup(
            last_serial_received=self._last_serial_received,
            last_serial_in_db=self._db.last_serial(),
        )])

        self._log = logging.getLogger(self.peername)

        peersetup = self.read_msg()
        if peersetup.last_serial_received <= self._db.last_serial():
            self._last_serial_sent = peersetup.last_serial_received
        else:
            self._log.warn("detecting local db was reset")
            self._last_serial_sent = 0

        if peersetup.last_serial_in_db < self._last_serial_received:
            self._log.warn("detecting remote db was reset")
            self._last_serial_received = 0

        self._log.info("%s is requesting from my #%d (is %d behind) ", self.peername, self._last_serial_sent, self._db.last_serial() - self._last_serial_sent)
        self._log.info("%s is currently at #%d (I'm %d behind)", self.peername, peersetup.last_serial_in_db, peersetup.last_serial_in_db - self._last_serial_received)

        return self

    def _sendmsg(self, *msg_groups):
        if not self._sock:
            return
        buf = StringIO()
        enc = MESSAGE_ENCODER(buf.write)
        for field, msg in msg_groups:
            enc(field, msg)
        self._sock.sendall(buf.getvalue())

    def _read_and_queue(self):
        try:
            res = self._sock and self._sock.recv(64*1024)
        except IOError, e:
            self._log.info("Failed to receive: errno: %s (%s)", e.errno, e.strerror)
            self.close()
            raise StopIteration

        if res:
            self._msg_queue(res)
        else:
            self.close()
            raise StopIteration

    def read_chunked(self):
        while True:
            chunk = self._msg_queue.clear()
            if chunk:
                yield chunk
            self._read_and_queue()

    def read_msg(self):
        while True:
            try:
                return self._msg_queue.pop()
            except IndexError:
                self._read_and_queue()

    def shutdown(self):
        if self._sock:
            self._sock.shutdown(socket.SHUT_WR)

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def _process_update_chunk(self, chunk):
        chunk_applied = 0
        chunk_had = 0
        last_serial = self._last_serial_received
        for msg in chunk:
            if isinstance(msg, sync_pb2.Update):
                chunk_had += 1
                serial = self._db.update_attr(msg.obj, msg.key, ValueSet(msg.values, t=msg.tstamp))
                if serial:
                    self._echo_prevention.add(serial)
                    chunk_applied += 1
            elif isinstance(msg, sync_pb2.Checkpoint):
                last_serial = max(last_serial, msg.serial)
            else:
                raise "Unknown message"

        if chunk_had:
            self._log.debug("Commit %d/%d", chunk_applied, chunk_had)
        if self._last_serial_received != last_serial:
            self._log.debug("Checkpoint %d", last_serial)
            self._last_serial_received = last_serial
            self._db.set_sync_state(self.peername, last_received=self._last_serial_received)

    def run(self):
        for chunk in self.read_chunked():
            with self._db.transaction():
                self._process_update_chunk(chunk)
            concurrent.sleep()

    def db_push(self):
        last_serial = self._last_serial_sent
        msgs = list()
        for obj, key, tstamp, serial, values in self._db.get_public_mappings_after(last_serial):
            if serial in self._echo_prevention:
                self._echo_prevention.discard(serial)
            else:
                msgs.append(sync_pb2.Update(obj=obj, key=key, tstamp=int(tstamp), values=values))
            last_serial = max(last_serial, serial)
        msg_groups = [['update', msgs]]
        if last_serial > self._last_serial_sent:
            msg_groups.append(['checkpoint', sync_pb2.Checkpoint(serial=last_serial)])
        self._sendmsg(*msg_groups)
        self._last_serial_sent = last_serial

class Syncer(object):
    def __init__(self, db, name, port, connectAddresses, db_poll_interval=0.5):
        self._db = db
        self.name = name
        self.port = port
        self.connections = dict()
        self.connectAddresses = connectAddresses
        self._sock = concurrent.listen(('0.0.0.0', port))
        self._server = concurrent.spawn(concurrent.serve, self._sock, self._spawn)
        self._pusher = concurrent.spawn(self._db_push)
        self._connector = concurrent.spawn(self._connector)

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
        with SyncConnection(self._db, self.name, sock) as conn:
            try:
                logging.debug("Handshaking for sock %s with connectAddress %s", sock, connectAddress or 'Unknown')
                with self._db.transaction():
                    conn.handshake()
            except StopIteration:
                logging.debug("Handshake ended prematurely for %s with connectAddress %s", sock, connectAddress or 'Unknown')
                return
            except Exception:
                logging.exception("Handshake failed for sock %s with connectAddress %s", sock, connectAddress or 'Unknown')
                return

            peername = conn.peername
            if client_addr == connectAddress:
                logging.info('connected to %s %s', peername, client_addr)
            else:
                logging.info('%s connected from %s', peername, client_addr)
            if not self._connectPeer(conn, connectAddress):
                logging.info('%s already connected. Dropping.',  peername)
                return

            try:
                conn.run()
            except Exception:
                logging.exception("%s died", peername)

            logging.info('%s disconnected from %s', conn.peername, client_addr)
            self._disconnectPeer(conn)

    def _connector(self):
        def _connect(addr):
            try:
                return addr, concurrent.connect(addr)
            except Exception:
                return addr, None

        connectPool = concurrent.Pool(20)
        while True:
            for address, sock in connectPool.imap(_connect, set(self.connectAddresses)):
                if sock:
                    concurrent.spawn(self._spawn, sock, address, connectAddress=address)
            concurrent.sleep(30)

    def _db_push(self):
        while True:
            with self._db.transaction():
                for conn in self.connections.values():
                    try:
                        conn.db_push()
                    except IOError:
                        logging.debug("Failed to push to %s, disconnecting", conn.peername)
                        self.connections.pop(conn.peername, None)
                    except Exception:
                        logging.exception("%s push hit error", conn.peername)
                        self.connections.pop(conn.peername, None)
                        conn.shutdown()
            concurrent.sleep(0.5)

    def wait(self):
        self._server.wait()
        self._pusher.wait()
        self._connector.wait()
