from __future__ import absolute_import

from base64 import b32encode, b32decode
from warnings import warn

from bithorde import message as proto


def _pad_base32(b32):
    overflow = len(b32) % 5
    if overflow:
        return b32 + '=' * (5 - overflow)
    else:
        return b32


class Identifiers(frozenset):
    @staticmethod
    def _normalize(id):
        if isinstance(id, proto.Identifier):
            if id.type == proto.TREE_TIGER:
                return u"tree:tiger:%s" % b32encode(id.id).rstrip('=')
            else:
                raise ValueError("Unsupported ID-type: %r" % id.type)
        elif isinstance(id, basestring):
            return id
        else:
            raise ValueError("Unsupported ID-type: %r" % id)

    def __new__(cls, ids):
        ids = (cls._normalize(id) for id in ids)
        return super(Identifiers, cls).__new__(cls, ids)

    def xt(self):
        return self

    @staticmethod
    def _to_proto_id(xt):
        if xt.startswith('tree:tiger:'):
            id_b32 = _pad_base32(xt[11:])
            return proto.Identifier(type=proto.TREE_TIGER, id=b32decode(id_b32))
        else:
            warn("Unsupported id format: %s" % xt)

    def proto_ids(self):
        return tuple(self._to_proto_id(xt) for xt in self)

    def add_to(self, db, size, t=None):
        xt = self.xt()
        obj = db[','.join(sorted(xt))]
        obj.set('xt', xt, t)
        obj.set('filesize', unicode(size), t)

        return obj
