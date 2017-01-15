# -*- coding: utf-8 -*-
'''
Helpers to code/decode the bithorde framing protocol
'''

import bithorde_pb2 as message

from google.protobuf.internal import decoder

# Protocol imports and definitions
MSG_MAP = message.Stream.DESCRIPTOR.fields_by_number
MSG_TYPE_MAP = {
    message.HandShake: 1,
    message.BindRead: 2,
    message.AssetStatus: 3,
    message.Read.Request: 5,
    message.Read.Response: 6,
    message.BindWrite: 7,
    message.DataSegment: 8,
    message.HandShakeConfirmed: 9,
    message.Ping: 10,
}


def decodeMessage(buf, msg_map=MSG_MAP):
    '''Decodes a single message from buffer
    @return (msg, bytesConsumed)
    @raises IndexError if buffer did not contain complete message
    '''
    id, newpos = decoder._DecodeVarint32(buf, 0)
    size, newpos = decoder._DecodeVarint32(buf, newpos)
    id = id >> 3
    msgend = newpos + size
    if msgend > len(buf):
        raise IndexError('Incomplete message')
    msg = msg_map[id].message_type._concrete_class()
    msg.ParseFromString(buf[newpos:msgend])
    return msg, msgend


def encode_varint(arr, i):
    if i > 127:
        arr.append((i & 127) | 128)
        encode_varint(arr, i >> 7)
    else:
        arr.append(i)


def encodeMessage(msg, msgtype=None, msg_map=MSG_TYPE_MAP):
    if not msgtype:
        msgtype = msg_map[type(msg)]
    msg = msg.SerializePartialToString()

    hdr = []
    encode_varint(hdr, msgtype << 3 | 2)
    encode_varint(hdr, len(msg))
    return bytearray(hdr) + msg
