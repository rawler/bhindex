from mock import Mock, call
from nose.tools import *

from db.syncer import *
from db import DB, Object, ValueSet

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


class TestSyncConnection():
    def setup(self):
        self.obj = Object(u'some_obj', {u'apa': ValueSet(u'banan')})

        self.db1 = DB(':memory:')
        self.db2 = DB(':memory:')

        self.connect()

    def connect(self):
        c1, c2 = socket_pair()
        self.syncer1 = SyncConnection(self.db1, 'syncer1', c1)
        self.syncer2 = SyncConnection(self.db2, 'syncer2', c2)

    def wait_for_equal(self, id):
        i = 0
        interval = 0.02
        while self.db1[id] != self.db2[id]:
            i += interval
            if i > 0.2:
                raise Exception("Timeout in wait_for_equal(%s)" % id)
            concurrent.sleep(interval)

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
