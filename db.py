# -*- coding: utf-8 -*-
import anydbm

from ConfigParser import ConfigParser
from urlparse import urlparse, parse_qs
from urllib import quote_plus as urlquote
import cPickle as pickle
import time

def path_str2lst(str):
    return [x for x in str.split("/") if x]

def path_lst2str(list):
    return "/".join(list)

class Asset(object):
    def __init__(self, name, hashIds):
        self.name = name
        self.hashIds = dict([id.rsplit(':', 1) for id in hashIds])
        self.timestamp = time.time()

    def magnetURL(self, name=None):
        # Generate name
        if not name: name = self.name or ''
        if name: name = 'dn=%s&' % urlquote(name)

        # Merge with hashIds
        return "magnet:?%s%s" % (name, '&'.join(['xt=%s:%s'%(k,v) for k,v in self.hashIds.iteritems()]))
    __str__ = magnetURL

    def update(self, other):
        if other.name and self.name != other.name:
            self.name = other.name
            self.timestamp = time.time()
        if self.hashIds != other.hashIds:
            self.hashIds.update(other.hashIds)
            self.timestamp = time.time()

    def indexes(self):
        retval = ['%s:%s'%(k,v) for k,v in self.hashIds.iteritems()]
        if self.name:
            return retval+["dn:"+self.name]
        else:
            return retval

    @classmethod
    def fromMagnet(cls, magnetURL):
        url = urlparse(magnetURL)
        info = parse_qs(url.path[1:])

        if 'dn' in info:  name = info['dn'][0]
        else:             name = None

        asset = cls(name, info['xt'])

        return asset

class DB(object):
    def __init__(self, config):
        self.db = anydbm.open(config.get('DB', 'file'), 'c')

    def __getitem__(self, key):
        return pickle.loads(self.db[key])

    def by_name(self, name):
        try:
            return self['dn:'+name]
        except KeyError:
            return None

    def merge(self, asset):
        if isinstance(asset, basestring):
            asset = Asset.fromMagnet(asset)
        idxs = [str(x) for x in asset.indexes()]
        for idx in idxs:
            try: oldAsset = self[idx]
            except KeyError: pass
            else:
                if oldAsset:
                    oldAsset.update(asset)
                    asset = oldAsset
        for idx in idxs:
            self.db[idx] = pickle.dumps(asset)

    def commit(self):
        self.db.sync()

    def iteritems(self, prefix=""):
        for k,v in self.db.iteritems():
            if k.startswith(prefix):
                yield k[len(prefix):], pickle.loads(v)

    def filter(self, **criteria):
        '''Allows iteration of selected items. The filter-criteria is specified using
        "prop=val"-kwargs. The value is compared using startswith.

        @example:
          for asset in db.filter(path='trailers') # Will match "trailers*"
             print asset
        '''
        for attr, val in criteria.iteritems():
            assert attr == "path", "Sorry, only path supported ATM."
            if isinstance(val, (list,tuple)):
                val = "/".join(val)
            criteria[attr] = "dn:"+val+"/"
        for k,v in self.db.iteritems():
            match = True
            for attr, val in criteria.iteritems():
                if not k.startswith(val):
                    match = False
            if match:
                yield k, pickle.loads(v)

    def dir(self, attr, prefix=[], **criteria):
        '''Return the sub-partitions of attr from items in DB. I.E. for a DB containing
        prefix=(a/a, a/b, b/a, c/a), db.partition("prefix") will return set('a','b','c').'''
        assert attr=="path", "Sorry, only path supported ATM."
        assert attr not in criteria
        result = dict()
        if prefix:
            criteria[attr] = prefix
            plen = len(prefix)
        else:
            plen = 0

        for k,v in self.filter(**criteria):
            if not k.startswith("dn:"):
                continue
            k = path_str2lst(k[3:])
            k = k[plen]
            if k in result:
                result[k] += 1
            else:
                result[k] = 1

        return result

def open(config):
    return DB(config)