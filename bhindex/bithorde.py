from __future__ import absolute_import

from base64 import b32encode

from db import ValueSet
from bithorde import message as proto


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


def obj_from_ids(db, ids, t=None):
    xt = ValueSet(ids.xt(), t=t)
    obj = db[','.join(sorted(xt))]
    obj[u'xt'] = xt

    return obj
