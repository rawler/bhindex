import os
import re
from base64 import b32decode as _b32decode

from .protocol import message

TIGER_HASH_RE = re.compile(r'tiger:(\w{39})')


def b32decode(string):
    l = len(string)
    string = string + "=" * (7-((l-1) % 8))  # Pad with = for b32decodes:s pleasure
    return _b32decode(string, True)


def parseHashIds(ids):
    if not hasattr(ids, '__iter__'):
        ids = (ids,)

    res = list()
    for id in ids:
        m = TIGER_HASH_RE.search(id)
        if m:
            try:
                id = b32decode(m.group(1))
                res.append(message.Identifier(type=message.TREE_TIGER, id=id))
            except TypeError:
                pass
        else:
            pass
    return res


def fsize(f):
    orig = f.tell()
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(orig, os.SEEK_SET)
    return size


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data
