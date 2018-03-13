from collections import deque

from bithorde.protocol import decodeMessage, encodeMessage
from distdb import sync_pb2


class StringBuf(object):
    def __init__(self):
        self._buf = ''

    def append(self, data):
        self._buf += data

    def clear(self, count):
        self._buf = self._buf[count:]

    def __str__(self):
        return self._buf


class MessageDecoder(object):
    def __init__(self, msg_map):
        self.msg_map = msg_map

    def __call__(self, tgt):
        buf = StringBuf()

        def decoder(data):
            buf.append(str(data))

            while True:
                try:
                    msg, consumed = decodeMessage(str(buf), msg_map=self.msg_map)
                except IndexError:
                    break

                buf.clear(consumed)
                tgt(msg)

        return decoder


class MessageEncoder(object):
    def __init__(self, msg_map):
        self.msg_type_map = dict(
            (name, field.number) for name, field in msg_map.iteritems()
        )

    def __call__(self, tgt):
        def encoder(name, data):
            msgtype = self.msg_type_map[name]
            x = 0
            if hasattr(data, '__iter__'):
                for msg in data:
                    tgt(encodeMessage(msg, msgtype=msgtype))
                    x += 1
            else:
                tgt(encodeMessage(data, msgtype=msgtype))
                x = 1
            return x
        return encoder


MESSAGE_DECODER = MessageDecoder(sync_pb2._STREAM.fields_by_number)
MESSAGE_ENCODER = MessageEncoder(sync_pb2._STREAM.fields_by_name)


class MessageQueue(object):
    def __init__(self, decoder=MESSAGE_DECODER):
        self._queue = deque()
        self._write = decoder(self._queue.append)

    def __call__(self, data):
        self._write(data)
        return len(data)

    def pop(self):
        return self._queue.popleft()

    def clear(self):
        buf = list()
        try:
            while True:
                buf.append(self._queue.popleft())
        except IndexError:
            pass
        return buf
