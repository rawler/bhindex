import os.path, re
from urlparse import urlparse, parse_qs
from time import time

import db

XT_PREFIX_TIGER = 'tree:tiger:'

class REGEX(object):
    def __init__(self, pattern, flags=0):
        self._re = re.compile(pattern, flags)

    def match(self, string):
        r = self._re.match(string)
        if r:
            return r.groupdict()
        else:
            return None

class SETIF(object):
    def __init__(self, key, value, rule):
        self._key = key
        self._value = value
        self._rule = rule

    def match(self, string):
        m = self._rule.match(string)
        if not m is None:
            m[self._key] = self._value
        return m

class IS(unicode):
    def match(self, string):
        if self == string:
            return {}
        else:
            return None

RULES = [
    ({"ext": SETIF(u"type", u"video", REGEX(r'(mkv|avi|mpg|ts|mp4|wmv|mov)'))}, None),
    ({"type": IS(u"video"), u"path": REGEX(r'(?P<category>Movies|TV)/(?!XXX)')}, None),
    ({"type": IS(u"video"), u"path": REGEX(r'Movies/(?P<category>XXX)')}, None),
    ({"path": REGEX(r'Movies/(?P<title>.*) \((?P<year>\d{4})\)/')}, None),
    ({"path": REGEX(r'Movies/(?!XXX)(?P<title>[^/]+)[. ](720p|1080p|bdrip|dvdrid|dvdr|PAL|xvid|\.)*', re.I)}, None),
    ({"path": REGEX(r'TV/(?P<series>[^/]+)/Season (?P<season>\d+)/.* \d{1,2}?x(?P<episode>\d{2}).*\.(mkv|avi|mpg|ts|mp4|wmv)$')}, None),
    ({"path": REGEX(r'.*(?P<quality>720p|1080p|480p|PAL|NTSC)', re.I)}, unicode.lower),
]

def applyRules(asset, t):
    for rule, filter in RULES:
        collection = {}
        filter = filter or (lambda x: x)
        for key, subrule in rule.iteritems():
            foundmatch = False
            for value in asset.get(key, ()):
                m = subrule.match(value)
                if not m is None:
                    collection.update((unicode(k),filter(v)) for k,v in m.iteritems())
                    foundmatch = True
            if not foundmatch:
                collection = {}
                break
        for k,v in collection.iteritems():
            if k in asset:
                asset[k].add(v)
            else:
                asset[k] = db.ValueSet(v, t)

def objectFromMagnet(magnetLink, t=None):
    x = parse(magnetLink)
    if x:
        if not t:
            t = time()
        asset = db.Object(x['xt'])
        asset[u'path'] = db.ValueSet(x['path'], t)
        asset[u'name'] = db.ValueSet(x['name'], t)
        asset[u'xt'] = db.ValueSet(x['xt'], t)
        asset[u'ext'] = db.ValueSet(x['ext'], t)
        for path in x['path']:
            applyRules(asset, t)
        return asset

    return None

def parse(magnetLink):
    _,q = magnetLink.split('?',1)
    attrs = parse_qs(q)
    if 'dn' in attrs:
        fname = os.path.basename(attrs['dn'][0])
        attrs['path'] = attrs['dn']
        attrs['name'], ext = os.path.splitext(fname)
        attrs['ext'] = ext.lstrip('.')
        del attrs['dn']
    if 'xt' in attrs:
        for xt in attrs['xt']:
            if xt.startswith('urn:'+XT_PREFIX_TIGER):
                attrs['xt'] = xt[4:]
                return attrs
    return False

def fromDbObject(o):
    return "magnet:?xt=urn:%s" % o['xt'].any()
