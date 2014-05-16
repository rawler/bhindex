# -*- coding: utf-8 -*-
'''
Helpers to code/decode the bithorde framing protocol
'''

import socket
import os, os.path
from base64 import b32decode as _b32decode
from cStringIO import StringIO

import bithorde_pb2 as message

from google.protobuf import descriptor
from google.protobuf.internal import encoder, decoder

# Protocol imports and definitions
MSG_MAP = message.Stream.DESCRIPTOR.fields_by_number
MSG_TYPE_MAP = {
    message.HandShake:     1,
    message.BindRead:      2,
    message.AssetStatus:   3,
    message.Read.Request:  5,
    message.Read.Response: 6,
    message.BindWrite:     7,
    message.DataSegment:   8,
    message.HandShakeConfirmed: 9,
    message.Ping: 10,
}

def decodeMessage(buf, msg_map=MSG_MAP):
    '''Decodes a single message from buffer
    @return (msg, bytesConsumed)
    @raises IndexError if buffer did not contain complete message
    '''
    id, newpos = decoder._DecodeVarint32(buf,0)
    size, newpos = decoder._DecodeVarint32(buf,newpos)
    id = id >> 3
    msgend = newpos+size
    if msgend > len(buf):
        raise IndexError, 'Incomplete message'
    msg = msg_map[id].message_type._concrete_class()
    msg.ParseFromString(buf[newpos:msgend])
    return msg, msgend

def encodeMessage(msg, msgtype=None, msg_map=MSG_TYPE_MAP):
    if not msgtype:
        msgtype = msg_map[type(msg)]
    buf = StringIO()
    enc = encoder.MessageEncoder(msgtype, False, False)
    enc(buf.write, msg)
    return buf.getvalue()
