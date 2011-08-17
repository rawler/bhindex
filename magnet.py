import os.path, re
from urlparse import urlparse, parse_qs
from time import time

import db

XT_PREFIX_TIGER = 'tree:tiger:'

PATH_RULES = [
    (re.compile(r'(?P<category>Movies|TV)/(?!XXX)'), None),
    (re.compile(r'Movies/(?P<category>XXX)'), None),
    (re.compile(r'Movies/(?P<title>.*) \((?P<year>\d{4})\)/'), None),
    (re.compile(r'Movies/(?!XXX)(?P<title>[^/]+)[. ](720p|1080p|bdrip|dvdrid|dvdr|PAL|xvid|\.)*.*/', re.I), None),
    (re.compile(r'TV/(?P<series>[^/]+)/Season (?P<season>\d+)/.* \d{1,2}?x(?P<episode>\d{2})'), None),
    (re.compile(r'.*(?P<quality>720p|1080p|480p|PAL|NTSC)', re.I), unicode.lower),
]

def mapPath(path, asset, t):
    for rule, filter in PATH_RULES:
        m = rule.match(path)
        if m:
            for k, v in m.groupdict().iteritems():
                k = unicode(k, 'utf8')
                if filter:
                    v = filter(v)
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
        asset[u'filetype'] = db.ValueSet(x['filetype'], t)
        for path in x['path']:
            mapPath(path, asset, t)
        return asset

    return None

def parse(magnetLink):
    _,q = magnetLink.split('?',1)
    attrs = parse_qs(q)
    if 'dn' in attrs:
        fname = os.path.basename(attrs['dn'][0])
        attrs['path'] = attrs['dn']
        attrs['name'], attrs['filetype'] = os.path.splitext(fname)
        del attrs['dn']
    if 'xt' in attrs:
        for xt in attrs['xt']:
            if xt.startswith('urn:'+XT_PREFIX_TIGER):
                attrs['xt'] = xt[4:]
                return attrs
    return False

def fromDbObject(o):
    return "magnet:?xt=urn:%s" % o['xt'].any()