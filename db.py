# -*- coding: utf-8 -*-
import anydbm

from ConfigParser import ConfigParser
from urlparse import urlparse, parse_qs
import cPickle as pickle

class Asset(object):
    def __init__(self, name, hashIds):
        self.name = name
        self.hashIds = hashIds

    def magnetURL(self):
        pass

    def indexes(self):
        return self.hashIds+[self.name]

    @classmethod
    def fromMagnet(cls, magnetURL):
        url = urlparse(magnetURL)
        info = parse_qs(url.path[1:])
        return cls(info['dn'], info['xt'])

class DB(object):
    def __init__(self, config):
        self.db = anydbm.open(config.get('DB', 'file'), 'c')
        self.__getitem__ = self.db.__getitem__
        self.__setitem__ = self.db.__setitem__
        self.iteritems = self.db.iteritems
        self.__iter__ = self.db.__iter__

    def merge(self, asset):
        for idx in asset.indexes():
            self.db[str(idx)] = pickle.dumps(asset)

    def commit():
        self.db.sync()

def open(config):
    if isinstance(config, basestring):
        config = ConfigParser()
        config.read(config)
    return DB(config)