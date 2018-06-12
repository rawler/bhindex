from base64 import urlsafe_b64encode as b64encode
from time import time as _sys_time
from uuid import uuid4

ANY = object()


def _time(t):
    if t is None:
        return _sys_time()
    else:
        return t


class Set(frozenset):
    def any(self, default=None):
        for x in self:
            return x
        return default


class TimedValues:
    __slots__ = ("v", "t")

    def __init__(self, v, t=None):
        if v is None:
            self.v = Set()
        elif isinstance(v, unicode):
            self.v = Set([v])
        else:
            self.v = Set(v)
            for x in self.v:
                assert isinstance(x, unicode)
        self.t = _time(t)

    def __nonzero__(self):
        return bool(self.v)

    def __eq__(self, other):
        if isinstance(other, TimedValues):
            return self.v == other.v
        else:
            return self.v == other

    def any(self, default=None):
        return self.v.any(default)

    def join(self, sep=u', '):
        return unicode(sep).join(self.v)

    def __repr__(self):
        return repr(self.v)

    __unicode__ = __str__ = join


class Object(object):
    __slots__ = ('id', '_dirty', '_dict')

    def __init__(self, objid, init={}):
        self.id = objid
        self._dirty = set()
        self._dict = dict()
        self.update(init)

    @classmethod
    def new(cls, prefix=None):
        id = b64encode(uuid4().bytes).strip('=')
        if prefix is not None:
            id = '%s:%s' % (prefix, id)
        return cls(id)

    def __getitem__(self, key):
        return self._dict[key].v

    def __eq__(self, other):
        return self.id == other.id \
            and self._dict == other._dict

    def __contains__(self, key):
        return bool(self._dict.get(key, None))

    def item(self, key):
        return self._dict[key]

    def get(self, key, default=None):
        try:
            return self._dict[key].v
        except (KeyError, AttributeError):
            return default

    def getitem(self, key, default=None):
        try:
            return self._dict[key]
        except (KeyError, AttributeError):
            return default

    def any(self, key, default=None):
        values = self._dict.get(key)
        if values:
            return values.any(default)
        else:
            return default

    def iteritems(self):
        return ((k, v.v) for k, v in self._dict.iteritems())

    def keys(self):
        return self._dict.keys()

    def empty(self):
        return len(self._dict) == 0

    def matches(self, criteria):
        for key, value in criteria.iteritems():
            if key not in self._dict:
                return False
            if value not in (None, ANY) and value not in self._dict[key].v:
                return False
        return True

    def timestamp(self, default=None):
        try:
            return max(x.t for x in self._dict.itervalues())
        except ValueError:
            return default

    def dirty(self):
        return self._dirty

    def set(self, key, values, t=None):
        self[key] = TimedValues(values, t)

    def __setitem__(self, key, value):
        key = key.lower()

        if not isinstance(value, TimedValues):
            value = TimedValues(value, _sys_time())

        current = self._dict.get(key, None)
        if (not current) or value.t >= current.t:
            self._dict[key] = value
            self._dirty.add(key)

    def __delitem__(self, key):
        if key in self:
            self._dict[key] = TimedValues((), self._dict[key].t + 1)
            self._dirty.add(key)

    def update(self, other):
        for k, v in other.iteritems():
            self[k] = v

    def __repr__(self):
        return repr(self._dict)

    def __unicode__(self):
        return u"db.Object {\n%s\n}" % u'\n'.join(u" %s: %s" % x for x in self._dict.iteritems())
