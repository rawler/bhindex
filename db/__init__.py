# -*- coding: utf-8 -*-

from database import ANY, Starts, DB
from object import ValueSet, Object

def open(path, sync=True):
    return DB(path, sync=sync)

if __name__ == '__main__':
    from time import time
    db = DB(':memory:')

    obj = db['myasset']
    obj[u'name'] = ValueSet(u'monkeyman', t=time())
    print "Yeah, I got", str(obj), obj._dirty

    db.update(obj)
    print "Yeah, I got", str(obj), obj._dirty

    obj = db['myasset']
    print "Yeah, I got", str(obj), obj._dirty

    for obj in db.query({'name': 'monkeyman'}):
        print obj

    for k,c in db.list_keys():
        print k,c

    db.commit()
