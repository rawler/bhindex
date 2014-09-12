from time import time

class ValueSet(set):
    def __init__(self, v, t=None):
        if isinstance(v, unicode):
            set.__init__(self, [v])
        else:
            set.__init__(self, v)
            for x in self:
                assert isinstance(x, unicode)
        if not t:
            t = time()
        self.t = t

    def update(self, v, t=None):
        if not t:
            t = time()
        self.t = max([t, self.t])
        set.update(self, v)

    def any(self):
        for x in self:
            return x

    def join(self, sep=u', '):
        return unicode(sep).join(self)
    __unicode__ = __str__ = join

class Object(object):
    def __init__(self, objid, init={}):
        self.id = objid
        self._dirty = set()
        self._dict = dict()
        self.update(init)

    def update(self, other):
        for k,v in other.iteritems():
            self[k] = v

    def __getitem__(self, key):
        return self._dict[key]

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def any(self, key, default=None):
        values = self._dict.get(key)
        return values and values.any() or default

    def __contains__(self, key):
        return key in self._dict

    def __setitem__(self, key, value):
        assert isinstance(key, unicode)
        assert isinstance(value, ValueSet)

        key = key.lower()
        if key not in self._dict or (self._dict[key] != value and value.t >= self._dict[key].t):
            self._dirty.add(key)
            self._dict[key] = value

    def __delitem__(self, key):
        assert isinstance(key, unicode)

        if key in self._dict:
            self._dirty.add(key)
            self._dict[key].clear()
            self._dict[key].t = time()

    def iteritems(self):
        return self._dict.iteritems()

    def matches(self, criteria):
        for key, value in criteria.iteritems():
            if key not in self._dict:
                return False
            if value not in (None, ANY) and value not in self._dict[key]:
                return False
        return True

    def update_key(self, key, values, t=None):
        if not t:
            t=time()
        if isinstance(values, unicode):
            values = set([values])

        # Try to prune upcased properties
        lkey = key.lower()
        if lkey != key:
            del self[key]
            key = lkey

        if key in self:
            self[key].update(values, t)
            self._dirty.add(key)
        else:
            self[key] = ValueSet(values, t)

    def timestamp(self):
        return max(x.t for x in self._dict.itervalues())

    def __repr__(self):
        return repr(self._dict)

    def __unicode__(self):
        return u"db.Object {\n%s\n}" % u'\n'.join(u" %s: %s" % x for x in self.iteritems())
