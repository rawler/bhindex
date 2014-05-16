import re
from base64 import b32decode as _b32decode

from .protocol import message

def b32decode(string):
    l = len(string)
    string = string + "="*(7-((l-1)%8)) # Pad with = for b32decodes:s pleasure
    return _b32decode(string, True)

tiger_hash = re.compile(r'tiger:(\w{39})')
def parseHashIds(ids):
    if not hasattr(ids, '__iter__'):
        ids = (ids,)

    res = list()
    for id in ids:
        m = tiger_hash.search(id)
        if m:
            try:
                id = b32decode(m.group(1))
                res.append(message.Identifier(type=message.TREE_TIGER, id=id))
            except TypeError:
                pass
        else:
            pass
    return res
