from cStringIO import StringIO
from mock import Mock, call
from nose.tools import *

from db.serialize import *
from db.sync_pb2 import Hello

hello_world = Hello(name='World')
hello_other = Hello(name='Other World')


class test_Message_DECODER_ENCODER():
    def setup(self):
        self.tgt = Mock()
        self._push = MESSAGE_DECODER(self.tgt)

    def test_simple(self):
        x = MESSAGE_ENCODER(self._push)('hello', hello_world)
        assert_equal(x, 1)
        self.tgt.assert_called_with(hello_world)

    def test_multi_push(self):
        x = MESSAGE_ENCODER(self._push)('hello', [hello_world, hello_other])
        assert_equal(x, 2)
        self.tgt.assert_has_calls([call(hello_world), call(hello_other)])

    def test_empty_push(self):
        MESSAGE_DECODER(self.tgt)(b'')
        self.tgt.assert_has_calls([])

    def test_buffering(self):
        buf = StringIO()
        MESSAGE_ENCODER(buf.write)('hello', hello_world)

        msg = buf.getvalue()
        split = len(msg) / 2
        buf1 = msg[:split]
        buf2 = msg[split:]

        decoder = MESSAGE_DECODER(self.tgt)

        decoder(buf1)
        self.tgt.assert_has_calls([])

        decoder(buf2)
        self.tgt.assert_called_with(hello_world)


class test_MessageQueue():
    def setup(self):
        self.q = MessageQueue(MESSAGE_DECODER)
        MESSAGE_ENCODER(self.q)('hello', [hello_world, hello_other])

    @raises(IndexError)
    def test_pop(self):
        assert_equal(self.q.pop(), hello_world)
        assert_equal(self.q.pop(), hello_other)
        self.q.pop()

    def test_clear(self):
        assert_equal(self.q.clear(), [hello_world, hello_other])

