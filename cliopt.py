from optparse import OptionParser
from time import time
import db

def parse_attrs(attrs, t=None):
    t = t or time()
    res = {}
    for v in attrs or ():
        k,v = unicode(v, 'utf8').split(':',1)
        if k not in res:
            res[k] = db.ValueSet(v, t)
        else:
            res[k].add[v]
    return res
