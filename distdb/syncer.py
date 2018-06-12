from cStringIO import StringIO
from obj import TimedValues
from uuid import uuid4 as uuid
import logging
import socket

from thread_io import connect, listen, serve, sleep, spawn, ThreadPool

from distdb.serialize import MESSAGE_DECODER, MESSAGE_ENCODER, MessageQueue
from distdb import sync_pb2, Transaction

from time import time

HANDSHAKE_TIMEOUT = 5
WRITE_TIMEOUT = 3


class Deadline(object):
    def __init__(self, timeout):
        self.value = timeout and time() + timeout

    def timeleft(self):
        return self.value and self.value - time()


class SyncConnection(object):
    def __init__(self, db, name, sock, uuid):
        self.db = db
        self.name = name
        self._sock = sock
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
        peerhello = self.read_msg(timeout=HANDSHAKE_TIMEOUT)
        if not peerhello:
            return
        self.peername = peerhello.name
        with self.db.lock, self.db.transaction():
            self._last_serial_received = self.db.get_sync_state(self.peername)['last_received']
            self._sendmsg(['setup', sync_pb2.Setup(
                last_serial_received=self._last_serial_received,
                last_serial_in_db=self.db.last_serial(),
            )])

        self._log = logging.getLogger(self.peername)

        peersetup = self.read_msg(timeout=HANDSHAKE_TIMEOUT)
        if not peersetup:
            return
        with self.db.lock, self.db.transaction():
            if peersetup.last_serial_received <= self.db.last_serial():
                self._last_serial_sent = peersetup.last_serial_received
            else:
                self._log.warn("detecting local db was reset")
                self._last_serial_sent = 0

            if peersetup.last_serial_in_db < self._last_serial_received:
                self._log.warn("detecting remote db was reset")
                self._last_serial_received = 0

        self._log.info("%s is requesting from my #%d (is %d behind) ", self.peername,
                       self._last_serial_sent, self.db.last_serial() - self._last_serial_sent)
        self._log.info("%s is currently at #%d (I'm %d behind)", self.peername, peersetup.last_serial_in_db,
                       peersetup.last_serial_in_db - self._last_serial_received)

        return self

    def _sendmsg(self, *msg_groups):
        if not self._sock:
            return
        buf = StringIO()
        enc = MESSAGE_ENCODER(buf.write)
        res = 0
        for field, msg in msg_groups:
            res += enc(field, msg)
        self._sock.send_within(WRITE_TIMEOUT, buf.getvalue())
        return res

    def _read_and_queue(self, timeout=None):
        try:
            sock = self._sock
            if not sock:
                return False

            sock.settimeout(timeout)
            if self._msg_queue(sock.recv(64 * 1024)):
                return True
            else:
                self.close()
                return False

        except IOError, e:
            self._log.info("Failed to receive: errno: %s (%s)", e.errno, e.strerror)
            self.close()
            return False

    def read_chunk(self):
        chunk = self._msg_queue.clear()
        while (not chunk) and self._read_and_queue():
            chunk = self._msg_queue.clear()
        return chunk

    def read_msg(self, timeout=None):
        deadline = Deadline(timeout)
        while True:
            try:
                return self._msg_queue.pop()
            except IndexError:
                if not self._read_and_queue(timeout=deadline.timeleft()):
                    return None

    def shutdown(self):
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_WR)
            except socket.error:
                pass

    def close(self):
        if self._sock:
            sock = self._sock
            self.shutdown()
            self._sock = None
            sock.close()

    def closed(self):
        return self._sock is None

    def _process_updates(self, chunk, transaction):
        chunk_applied = 0
        chunk_had = 0
        last_serial = self._last_serial_received
        for msg in chunk:
            if isinstance(msg, sync_pb2.Update):
                chunk_had += 1
                serial = transaction.update_attr(msg.obj, msg.key, TimedValues(msg.values, t=msg.tstamp))
                if serial:
                    self._echo_prevention.add(serial)
                    chunk_applied += 1
            elif isinstance(msg, sync_pb2.Checkpoint):
                last_serial = max(last_serial, msg.serial)
            else:
                raise TypeError("Unknown message")

        if chunk_had:
            self._log.debug("Commit %d/%d", chunk_applied, chunk_had)
        if self._last_serial_received != last_serial:
            self._log.debug("Checkpoint %d", last_serial)
            self._last_serial_received = last_serial
            self.db.set_sync_state(self.peername, last_received=self._last_serial_received)

    def _step(self):
        chunk = self.read_chunk()
        if chunk:
            with self.db.lock, self.db.transaction(Transaction.IMMEDIATE) as t:
                self._process_updates(chunk, t)
            return True
        else:
            return False

    def run(self):
        while self._step():
            sleep(0.0001)

    def db_push(self):
        def poll_updates(last_serial):
            updates = list()
            for obj, key, tstamp, serial, values in self.db.get_public_mappings_after(last_serial):
                if serial in self._echo_prevention:
                    self._echo_prevention.discard(serial)
                else:
                    updates.append(sync_pb2.Update(obj=obj, key=key, tstamp=int(tstamp), values=values))
                last_serial = max(last_serial, serial)
            return updates, last_serial

        def send_messages(updates, last_serial):
            msg_groups = list()
            if updates:
                msg_groups.append(['update', updates])
            if last_serial > self._last_serial_sent:
                msg_groups.append(['checkpoint', sync_pb2.Checkpoint(serial=last_serial)])
            if msg_groups:
                try:
                    self._sendmsg(*msg_groups)
                except socket.timeout:
                    logging.getLogger('syncer').warning("Peer %s is blocking on writes (too slow?). Disconnecting.", self.peername)
                    self.close()

        updates, last_serial = poll_updates(self._last_serial_sent)
        send_messages(updates, last_serial)
        self._last_serial_sent = last_serial

        return len(updates)


class OwnedKeyDict:
    def __init__(self):
        self._dict = dict()

    def __getitem__(self, k):
        return self._dict[k]

    def __contains__(self, k):
        return k in self._dict

    def itervalues(self):
        return self._dict.itervalues()

    def values(self):
        return self._dict.values()

    def key_context(self):
        return self.KeyContext(self._dict)

    class KeyContext:
        def __init__(self, dict):
            self.dict = dict
            self.keys = set()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            for key in self.keys:
                try:
                    del self.dict[key]
                except KeyError:
                    pass

        def set_key(self, key, value):
            if key in self.dict:
                return None   # Slot was already occupied
            else:
                # Slot was empty, we set a new key
                self.keys.add(key)
                self.dict[key] = value
                return value


class P2P(object):
    def __init__(self, listen_addr, handler, connect_addresses=set(), connect_interval=30):
        if isinstance(listen_addr, int):
            listen_addr = ('0.0.0.0', listen_addr)
        self.uuid = uuid()
        self.handler = handler
        self.connections = OwnedKeyDict()
        self.connect_interval = connect_interval
        self._connectAddresses = dict((addr, True) for addr in connect_addresses)
        self._sock = listen(listen_addr)
        self._server = spawn(serve, self._sock, self._connectionWrapper)
        self._connectThread = spawn(self._connector)
        logging.getLogger('syncer').info("Listening on %s", listen)

    def add_address(self, addr):
        if addr not in self._connectAddresses:
            self._connectAddresses[addr] = True

    def remove_address(self, addr):
        del self._connectAddresses[addr]

    def _connector(self):
        def _connect(addr):
            try:
                return addr, connect(addr)
            except Exception:
                return addr, None

        connectPool = ThreadPool(20)
        while self._sock:
            addresses = [addr for addr, uuid in self._connectAddresses.iteritems() if uuid and (uuid not in self.connections)]
            for address, sock in connectPool.imap(_connect, addresses):
                if sock:
                    self._connectAddresses[address] = False

                    def run():
                        try:
                            self._connectionWrapper(sock, address, address)
                        finally:
                            if not self._connectAddresses[address]:
                                self._connectAddresses[address] = True
                    spawn(run)
            sleep(self.connect_interval)

    def _connectionWrapper(self, sock, client_address, connectAddress=None):
        if connectAddress:
            logging.debug("Connection established to %s", sock.getpeername())
        else:
            logging.debug("Connection established from %s", sock.getpeername())

        with self.connections.key_context() as o:
            def set_session(uuid, s):
                if not self._sock:
                    return False
                if connectAddress in self._connectAddresses:
                    self._connectAddresses[connectAddress] = uuid
                return o.set_key(uuid, s)
            self.handler(sock, set_session)

    def local_addr(self):
        return self._sock.getsockname()

    def wait(self):
        try:
            self._server.wait()
        except:
            pass
        try:
            self._connectThread.wait()
        except:
            pass

    def close(self):
        if self._sock:
            sock = self._sock
            self._sock = None
            sname = sock.getsockname()
            sock.close()

            # Connect to break possibly hung accept()-call
            try:
                connect(sname)
            except socket.error:
                pass
        self.wait()


class Syncer(P2P):
    def __init__(self, db, name, port, connect_addresses=set(), db_poll_interval=5, connect_interval=30):
        self._db = db
        self.name = name
        super(Syncer, self).__init__(port, self._spawn, connect_addresses, connect_interval)
        self.running = True
        self._pusher = spawn(self._db_push, db_poll_interval)

    def _spawn(self, sock, set_session):
        peer = sock.getpeername()
        with SyncConnection(self._db, self.name, sock, self.uuid) as conn:
            try:
                logging.debug("Handshaking with remote address %s", peer)
                if not conn.handshake():
                    logging.debug("Handshake ended prematurely with %s", peer)
                    return
            except Exception:
                logging.exception("Handshake failed with %s", peer)
                return

            peername = conn.peername
            logging.info("connection established with %s (%s)", peername, peer)
            if not set_session(peername, conn):
                logging.info('%s already connected. Dropping.', peername)
                return

            try:
                conn.run()
            except Exception:
                logging.exception("%s died", peername)
            else:
                logging.info('%s disconnected from %s', conn.peername, peer)

    def _db_push(self, db_poll_interval):
        while self.running:
            sent = 0
            with self._db.lock, self._db.transaction():
                for conn in self.connections.values():
                    try:
                        sent += conn.db_push()
                    except IOError:
                        logging.debug("Failed to push to %s, disconnecting", conn.peername)
                    except Exception:
                        logging.exception("%s push hit error", conn.peername)
                        conn.shutdown()
            if sent:
                sleep(0)
            else:
                sleep(db_poll_interval)

    def close(self):
        self.running = False
        for connection in self.connections.itervalues():
            connection.close()
        super(Syncer, self).close()

    def wait(self):
        super(Syncer, self).wait()
        try:
            self._pusher.wait()
        except:
            pass
