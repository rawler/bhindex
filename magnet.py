import os.path, re
from urlparse import urlparse, parse_qs
from time import time

import db

XT_PREFIX_TIGER = 'tree:tiger:'


class REGEX(object):
    '''Matches provided attribute against a regex.
       Returns: the contained named groups, if a match is found
    '''
    def __init__(self, pattern, flags=0):
        self._re = re.compile(pattern, flags)

    def match(self, string):
        r = self._re.match(string)
        if r:
            return r.groupdict()
        else:
            return None

class SETIF(object):
    '''Matches provided attribute against an embedded rule, adding to the result
       Returns: {key: value}, if embedded rule matched
    '''
    def __init__(self, rule, attrs):
        self._rule = rule
        self._attrs = attrs

    def match(self, string):
        m = self._rule.match(string)
        if not m is None:
            m.update(self._attrs)
        return m

class IS(unicode):
    '''Exact match against provided attribute.
       Returns: empty map if successful, otherwise none.
    '''
    def match(self, string):
        if self == string:
            return {}
        else:
            return None

# Rules are evaluated in order. Each rule are on the form (rule-set, filter)
# Filters are used to change any resulting attribute-values for this rule-set, such as downcasing.
#
# Each rule-set are a list on the form (attr: matcher, ...)
# Attr is the attr on the object to evaluate, and matcher performs the evaluation, returning a
# result-map which is then merged into the object.
#
# Whenever a rule fails to match the selected attribute, the current rule-set is aborted, and the next one evaluated.
RULES = [
    ({"ext": SETIF(REGEX(r'(mkv|avi|mpg|ts|mp4|wmv|mov)'), {u"type": u"video"})}, None),
    ({"type": IS(u"video"), u"path": REGEX(r'(?P<category>Movies|TV)/(?!XXX)')}, None),
    ({"type": IS(u"video"), u"path": REGEX(r'Movies/(?P<category>XXX)')}, None),
    ({"path": REGEX(r'Movies/(?P<title>.*) \((?P<year>\d{4})\)/')}, None),
    ({"path": REGEX(r'Movies/(?!XXX)(?P<title>[^/]+)[. ](720p|1080p|bdrip|dvdrid|dvdr|PAL|xvid|\.)*', re.I)}, None),
    ({"path": REGEX(r'TV/(?P<series>[^/]+)/Season (?P<season>\d+)/.*(\d{1,2})x(?P<episode>\d{2})')}, None),
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

def objectFromMagnet(magnetLink, t=None, fullPath=None):
    x = parse(magnetLink)
    if x:
        if not t:
            t = time()
        asset = db.Object(x['xt'])
        asset[u'path'] = db.ValueSet(fullPath or x['path'], t)
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
