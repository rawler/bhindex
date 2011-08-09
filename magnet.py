
from urlparse import urlparse, parse_qs

XT_PREFIX_TIGER = 'tree:tiger:'

def parse(magnetLink):
    _,q = magnetLink.split('?',1)
    attrs = parse_qs(q)
    if 'dn' in attrs:
        _, fname = attrs['dn'][0].rsplit('/', 1)
        attrs['path'] = attrs['dn']
        attrs['name'], attrs['filetype'] = fname.rsplit('.', 1)
        del attrs['dn']
    if 'xt' in attrs:
        for xt in attrs['xt']:
            if xt.startswith('urn:'+XT_PREFIX_TIGER):
                attrs['xt'] = xt[4:]
                return attrs
    return False

def fromDbObject(o):
    return "magnet:?xt=urn:%s" % o['xt'].any()