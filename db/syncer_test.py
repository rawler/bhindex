from nose.tools import *

from db.serialize import *
from db.syncer import *
from db import DB, Object, ValueSet, syncer, sync_pb2

import concurrent


def run_parallel(*jobs):
    running = [concurrent.spawn(x) for x in jobs]
    return [x.wait() for x in running]


def socket_pair():
    s = concurrent.socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    try:
        s.listen(1)
        t = concurrent.spawn(s.accept)
        c1 = concurrent.connect(s.getsockname())
        c2, _ = t.wait()
        return c1, c2
    except:
        return None, None
    finally:
        s.close()


def wait_for(condition, timeout=1):
    i = 0
    interval = 0.01
    while not condition():
        i += interval
        if i > timeout:
            raise Exception("Timeout in wait_for(%s)" % condition)
        concurrent.sleep(interval)


class TestSyncConnection():
    def setup(self):
        self.obj = Object(u'some_obj', {u'apa': ValueSet(u'banan', t=0)})

        self.db1 = DB(':memory:')
        self.db2 = DB(':memory:')

        self.connect()

    def connect(self):
        c1, c2 = socket_pair()
        self.syncer1 = SyncConnection(self.db1, 'syncer1', c1, None)
        self.syncer2 = SyncConnection(self.db2, 'syncer2', c2, None)

    def wait_for_equal(self, id):
        wait_for(lambda: self.db1[id] == self.db2[id])

    def assert_equal(self, id):
        assert_equal(self.db1[id], self.db2[id])

    def handshake(self):
        return run_parallel(
            self.syncer1.handshake,
            self.syncer2.handshake,
        )

    def test_setup(self):
        res1, res2 = self.handshake()
        assert_is(res1, self.syncer1)
        assert_is(res2, self.syncer2)

    def test_handshake_timeout(self):
        c1, c2 = socket_pair()
        syncer.HANDSHAKE_TIMEOUT = 0.2
        self.syncer1 = SyncConnection(self.db1, 'syncer1', c1, None)
        assert_raises(StopIteration, self.syncer1.handshake)

    def test_handshake_timeout2(self):
        c1, c2 = socket_pair()
        syncer.HANDSHAKE_TIMEOUT = 0.2
        self.syncer1 = SyncConnection(self.db1, 'syncer1', c1, None)
        MESSAGE_ENCODER(c2.send)('hello', sync_pb2.Hello(name="apa"))
        assert_raises(StopIteration, self.syncer1.handshake)

    def test_context_manager(self):
        assert_false(self.syncer1.closed())
        assert_equal(self.syncer1._sendmsg(), 0)
        with self.syncer1:
            pass

        assert_true(self.syncer1.closed())
        assert_is_none(self.syncer1._sendmsg())
        assert_false(self.syncer1.read_chunk())
        assert_is_none(self.syncer1.read_msg())

    def test_simple_step(self):
        self.handshake()

        self.assert_equal(self.obj.id)
        self.db1.update(self.obj)
        assert_in(u'apa', self.db1[self.obj.id])
        assert_not_in(u'apa', self.db2[self.obj.id])

        self.syncer1.db_push()
        self.syncer2._step()
        self.assert_equal(self.obj.id)

    @raises(TypeError)
    def test_unknown_message(self):
        self.syncer1._process_updates(["This is not a message"])

    def test_simple_sync(self):
        self.handshake()

        self.assert_equal(self.obj.id)
        self.db1.update(self.obj)
        assert_in(u'apa', self.db1[self.obj.id])
        assert_not_in(u'apa', self.db2[self.obj.id])

        # Let syncer1 push update to syncer2
        self.syncer1.db_push()
        self.syncer1.close()
        self.syncer2.run()

        self.assert_equal(self.obj.id)

    def test_attribute_deletion(self):
        self.handshake()

        self.obj[u'deleted'] = ValueSet([u"Something"], t=445)
        self.db1.update(self.obj)
        self.syncer1.db_push()
        self.syncer2._step()

        self.assert_equal(self.obj.id)

        del self.obj[u'deleted']
        self.db1.update(self.obj)
        self.syncer1.db_push()
        self.syncer2._step()

        self.assert_equal(self.obj.id)

    def test_object_deletion(self):
        self.handshake()

        self.db1.update(self.obj)
        self.syncer1.db_push()
        self.syncer2._step()

        self.assert_equal(self.obj.id)

        del self.db1[self.obj]
        self.syncer1.db_push()
        self.syncer2._step()

        self.assert_equal(self.obj.id)

    def test_echo_prevention(self):
        self.handshake()
        self.db1.update(self.obj)

        # Let syncer1 push update to syncer2
        self.syncer1.db_push()
        self.syncer2._step()
        assert_true(self.syncer2._echo_prevention)

        # Verify syncer2 does not echo
        self.syncer2.db_push()
        assert_false(self.syncer2._echo_prevention)

        # Syncer1 should get nothing
        self.syncer2.shutdown()
        assert_false([x for x in self.syncer1.read_chunk() if isinstance(x, sync_pb2.Update)])

    def test_db_reset(self):
        self.test_simple_step()
        self.syncer1.close()
        self.syncer2.close()

        self.db1 = DB(':memory:')
        self.connect()
        self.handshake()
        assert_equal(self.syncer1._last_serial_received, 0)
        assert_equal(self.syncer2._last_serial_sent, 0)
        assert_not_in(u'apa', self.db1[self.obj.id])

        self.syncer2.db_push()
        self.syncer1._step()
        assert_in(u'apa', self.db1[self.obj.id])
        self.assert_equal(self.obj.id)


class TestP2P():
    def setup(self):
        self.s = P2P(0, self.spawn, connect_interval=0.05)

    def spawn(self, sock, set_session):
        msg = sock.recv(1024)
        try:
            while msg and len(msg):
                if set_session(msg, True):
                    sock.send(msg)
                else:
                    sock.send("")
                    sock.shutdown(socket.SHUT_RDWR)
                    break
                msg = sock.recv(1024)
        except socket.error:
            pass

    def test_setup(self):
        assert_is_not_none(self.s.local_addr())

    def test_connect(self):
        s = concurrent.connect(self.s.local_addr())
        assert_is_not_none(s)
        s.send("apa")
        assert_equal(s.recv(1024), "apa")
        s.close()

    def test_connected(self):
        s = concurrent.connect(self.s.local_addr())
        s.send("apa")
        wait_for(lambda: "apa" in self.s.connections)
        s.close()

    def test_sessionTermination(self):
        s = concurrent.connect(self.s.local_addr())
        s.send("apa")
        wait_for(lambda: "apa" in self.s.connections)
        s.shutdown(socket.SHUT_WR)
        s.close()
        wait_for(lambda: "apa" not in self.s.connections)

    def test_already_connected(self):
        s = concurrent.connect(self.s.local_addr())
        s.send("apa")
        wait_for(lambda: "apa" in self.s.connections)

        s1 = concurrent.connect(self.s.local_addr())
        s1.send("apa")
        assert_equal(s1.recv(1024), "")

    def test_connector(self):
        s = concurrent.listen(('', 0))
        s.listen(3)
        self.s.add_address(s.getsockname())
        s.settimeout(0.4)
        client, _ = s.accept()
        uuid = "apa"
        client.send(uuid)
        wait_for(lambda: uuid in self.s._connectAddresses.values())

    def test_connector_does_not_reconnect(self):
        s = concurrent.listen(('', 0))
        s.listen(3)
        self.s.add_address(s.getsockname())
        s.settimeout(0.2)
        client, _ = s.accept()

        assert_raises(concurrent.socket.error, s.accept)

        uuid = "apa"
        client.send(uuid)
        assert_raises(concurrent.socket.error, s.accept)

        client.shutdown(socket.SHUT_RDWR)
        client, _ = s.accept()

    def test_close(self):
        self.s.close()


class TestSyncServer():
    def setup(self):
        self.db = DB(':memory:')
        self.s = Syncer(self.db, 'Syncer1', 0, db_poll_interval=0.1, connect_interval=0.2)

    def wait_for_connection(self, other):
        wait_for(lambda: str(self.s.name) in other.connections)
        wait_for(lambda: str(other.name) in self.s.connections)

    def connect(self, name="Syncer2"):
        db = DB(':memory:')
        s = concurrent.connect(self.s.local_addr())
        return SyncConnection(db, name, s, None)

    def test_simple_sync(self):
        db2 = DB(':memory:')
        s2 = Syncer(db2, 'Syncer2', 0, set([self.s.local_addr()]), db_poll_interval=0.1, connect_interval=0.2)
        self.s.add_address(s2.local_addr())

        self.wait_for_connection(s2)

        with self.db.transaction():
            self.db.update(Object('apa', init={u"test": ValueSet(u"4", t=5)}))
        wait_for(lambda: 'test' in db2['apa'])
        with db2.transaction():
            db2.update(Object('apa', init={u"test": ValueSet(u"7")}))
        wait_for(lambda: self.db['apa']['test'] == ValueSet(u"7"))

    def test_disconnect(self):
        s = self.connect()
        s.handshake()
        self.s.close()
        s.run()

    def test_handshake_timeout(self):
        s = self.connect()
        syncer.HANDSHAKE_TIMEOUT = 0.2
        assert_equal(s.read_msg(), sync_pb2.Hello(name="Syncer1"))
        assert_equal(s.read_msg(), None)
