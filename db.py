# -*- coding: utf-8 -*-
import anydbm

from ConfigParser import ConfigParser
from urlparse import urlparse, parse_qs
import cPickle as pickle

class Asset(object):
    def __init__(self, name, hashIds):
        self.name = name
        self.hashIds = dict([id.rsplit(':', 1) for id in hashIds])

    def magnetURL(self):
        if self.name:
            name = 'dn=%s&' % self.name
        else:
            name = ''
        return "magnet:?%s%s" % (name, '&'.join(['xt=%s:%s'%(k,v) for k,v in self.hashIds.iteritems()]))
    __str__ = magnetURL

    def update(self, other):
        if other.name and not self.name:
            self.name = other.name
        self.hashIds.update(other.hashIds)

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

        return cls(name, info['xt'])

class DB(object):
    def __init__(self, config):
        self.db = anydbm.open(config.get('DB', 'file'), 'c')

    def __getitem__(self, key):
        return pickle.loads(self.db[key])

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

def open(config):
    if isinstance(config, basestring):
        config = ConfigParser()
        config.read(config)
    return DB(config)