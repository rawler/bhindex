from base64 import urlsafe_b64encode as b64encode
from time import time
from uuid import uuid4

ANY = object()


class ValueSet(set):
    # TODO: Rethink API surrounding ValueSet. Should it really be a set, not a frozenset? Should time really be mapped here?

    def __init__(self, v, t=None):
        if isinstance(v, unicode):
            set.__init__(self, [v])
        else:
            set.__init__(self, v)
            for x in self:
                assert isinstance(x, unicode)
        if t is None:
            t = time()
        self.t = t

    def _touch(self, t):
        if t is None:
            t = time()
        self.t = max((t, self.t))

    def add(self, value, t=None):
        self._touch(t)
        super(ValueSet, self).add(value)

    def discard(self, value, t=None):
        self._touch(t)
        super(ValueSet, self).discard(value)

    def update(self, v, t=None):
        self._touch(t)
        super(ValueSet, self).update(self, v)

    def any(self, default=None):
        for x in self:
            return x
        return default

    def join(self, sep=u', '):
        return unicode(sep).join(self)
    __unicode__ = __str__ = join


class Object(object):
    def __init__(self, objid, init={}):
        self.id = objid
        self._dirty = set()
        self._dict = dict()
        self._deleted = dict()
        self.update(init)

    @classmethod
    def new(cls, prefix=None):
        id = b64encode(uuid4().bytes).strip('=')
        if prefix is not None:
            id = '%s:%s' % (prefix, id)
        return cls(id)

    def update(self, other):
        for k, v in other.iteritems():
            self[k] = v

    def __getitem__(self, key):
        return self._dict[key]

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def any(self, key, default=None):
        values = self._dict.get(key)
        if values:
            return values.any(default)
        else:
            return default

    def __eq__(self, other):
        return self.id == other.id \
           and self._dict == other._dict

    def __contains__(self, key):
        return key in self._dict

    def __setitem__(self, key, value):
        assert isinstance(value, ValueSet)

        key = key.lower()
        if key not in self._dict or (value.t >= self._dict[key].t):
            self._dirty.add(key)
            self._dict[key] = value

    def __delitem__(self, key):
        if key in self._dict:
            self._dirty.add(key)
            self._deleted[key] = time()
            del self._dict[key]

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
        if isinstance(values, unicode):
            values = set([values])

        if key in self:
            self[key].update(values, t)
            self._dirty.add(key)
        else:
            self[key] = ValueSet(values, t)

    def timestamp(self, default=None):
        timestamps = [x.t for x in self._dict.itervalues()]
        if timestamps:
            return max(timestamps)
        else:
            return default

    def __repr__(self):
        return repr(self._dict)

    def __unicode__(self):
        return u"db.Object {\n%s\n}" % u'\n'.join(u" %s: %s" % x for x in self.iteritems())
