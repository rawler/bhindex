def _mkset(x):
    if isinstance(x, frozenset):
        return x
    elif isinstance(x, (set, tuple)):
        return frozenset(x)
    else:
        return frozenset((x,))


def _mktuple(x):
    if isinstance(x, tuple):
        return x
    elif isinstance(x, (list)):
        return tuple(x)
    else:
        return tuple((x,))


class ConditionExpr(tuple):
    def __new__(cls, sources, expr, params, condition_on_key):
        sources = _mkset(sources)
        params = _mktuple(params)
        return super(ConditionExpr, cls).__new__(cls, (sources, expr, params, condition_on_key))

    @property
    def condition_on_key(self):
        return self[3]

    def merge(self, other):
        (s1, e1, p1, ck1) = self
        (s2, e2, p2, ck2) = other
        if isinstance(e1, basestring):
            e1 = (e1,)
        if isinstance(e2, basestring):
            e2 = (e2,)
        return ConditionExpr(s1 | s2, e1 + e2, p1 + p2, ck1 or ck2)


class ObjId:
    def apply(self):
        return ConditionExpr('obj', '', (), False)

    def __eq__(self, v):
        return Matcher.build((), self, (Equals, 'obj', v))

    def startswith(self, v):
        return Matcher.build((), self, (Starts, 'obj', v))


ObjId = ObjId()


class Condition(object):
    def __init__(self, *args):
        self.children = tuple(args)

    def __eq__(self, other):
        return type(self) == type(other) \
            and len(self.children) == len(other.children) \
            and all(a == b for a, b in zip(self.children, other.children))

    def __len__(self):
        return len(self.children)

    def __iter__(self):
        return iter(self.children)

    def __repr__(self):
        return "%s%r" % (type(self).__name__, self.children)

    def requires_key(self, key):
        if len(self.children) > 0:
            x = self.children[0]
            if isinstance(x, Key) and x.v == key:
                return True
            else:
                return False
        else:
            return False


class Starts(Condition):
    def apply(self):
        key, value = self
        expr = "%s GLOB '%s*'" % (key, value.replace("'", "''"))
        return ConditionExpr((), expr, (), False)


class Equals(Condition):
    def apply(self):
        key, value = self
        expr = "%s=?" % key
        return ConditionExpr((), expr, value, False)


class TimedBefore(Condition):
    def apply(self):
        key, timestamp = self
        sources, key_expr, key_params, condition_on_key = key.apply()
        return ConditionExpr(sources, "%slistid IS NOT NULL AND timestamp < ?" % key_expr, key_params + (timestamp,), condition_on_key)


class KeyAny(Condition):
    def apply(self):
        key, = self
        sources, key_expr, key_params, condition_on_key = key.apply()
        return ConditionExpr(sources, "%slistid IS NOT NULL" % key_expr, key_params, condition_on_key)


class KeyMissing(Condition):
    def apply(self):
        key, = self
        key_expr, key_params = Query(('objid',), key.any()).apply()
        return ConditionExpr((), "objid NOT IN (%s)" % key_expr, key_params, False)

    def requires_key(self, _):
        return False


class Key(object):
    def __init__(self, v):
        self.v = v

    def apply(self):
        if isinstance(self.v, int):
            return ConditionExpr((), "LIKELIHOOD(keyid=?, 0.2) AND ", (self.v,), True)
        else:
            return ConditionExpr('key', "LIKELIHOOD(key=?, 0.2) AND ", (self.v,), True)

    def __eq__(self, v):
        return Matcher.build('list', self, (Equals, 'value', v))

    def startswith(self, v):
        return Matcher.build('list', self, (Starts, 'value', v))

    def timed_before(self, t):
        return TimedBefore(self, t)

    def any(self):
        return KeyAny(self)

    def missing(self):
        return KeyMissing(self)

    def __repr__(self):
        return "Key(%r)" % self.v


class Matcher(Condition):
    def apply(self):
        extra_sources, key, comparer = self
        key_sources, key_prefix, key_params, condition_on_key1 = key.apply()
        sources, expression, params, condition_on_key2 = comparer.apply()

        sources = key_sources | extra_sources | sources
        expression = "%s(%s)" % (key_prefix, expression)
        params = key_params + params
        condition_on_key = condition_on_key1 | condition_on_key2
        return ConditionExpr(sources, expression, params, condition_on_key)

    @classmethod
    def build(cls, extra_sources, what, compare_args):
        comparer = cls.build_comparer(*compare_args)
        return cls(_mkset(extra_sources), what, comparer)

    @classmethod
    def build_comparer(cls, func, key, value):
        if hasattr(value, '__iter__'):
            comparers = (cls.build_comparer(func, key, v) for v in value)
            return OrCondition(*comparers)
        else:
            return func(key, value)


class ListCondition(Condition):
    def __init__(self, *args):
        def check(x):
            assert isinstance(x, Condition)
            return x
        super(ListCondition, self).__init__(*(check(x) for x in args))

    def apply(self):
        res = ConditionExpr((), (), (), False)
        for x in self:
            res = res.merge(self.expression_for(res, x))

        sources, expressions, params, condition_on_key = res
        expression = "%s" % (self.join(expressions))
        return ConditionExpr(sources, expression, params, condition_on_key)

    def expression_for(self, res, x):
        return x.apply()


class AndCondition(ListCondition):
    join = ' AND '.join

    def expression_for(self, res, x):
        sources, expression, params, condition_on_key = super(AndCondition, self).expression_for(res, x)
        if res.condition_on_key and condition_on_key:
            cond, params = Query(('objid',), x).apply()
            expression = '(objid IN (%s))' % cond
            sources = _mkset(())
        else:
            expression = "(%s)" % expression

        return ConditionExpr(sources, expression, params, condition_on_key)


class OrCondition(ListCondition):
    join = ' OR '.join


class Sort:
    ASCENDING = "ASC"
    DESCENDING = "DESC"

    @staticmethod
    def value():
        return "value", ()

    @staticmethod
    def split(character):
        return lambda: ("substr(value, instr(value, ?))", (character,))

    @staticmethod
    def timestamp():
        return "timestamp", ()


class Query(object):
    def __init__(self, columns, criteria=()):
        self.columns = list(columns)
        self.sources = set(('map',))
        self.sorting = ()

        if isinstance(criteria, Condition):
            self.where(criteria)
        else:
            self.where(*criteria)

        if 'obj' in columns:
            self.sources.add('obj')
        if 'value' in columns:
            self.sources.add('list')
        if 'key' in columns:
            self.sources.add('key')

    def apply(self):
        cols = ', '.join(self.columns)
        extra_sources, where_expr, params, _ = self.criteria.apply()
        sources = ' NATURAL JOIN '.join(self.sources | frozenset(extra_sources))

        expr = "SELECT %s FROM %s" % (cols, sources)
        if where_expr:
            expr += " WHERE %s" % where_expr

        if self.sorting:
            meth, direction = self.sorting
            sort_key, sort_params = meth()
            expr += " ORDER BY %s %s""" % (sort_key, direction)
            params += sort_params

        return expr, params

    def where(self, *conditions):
        for x in conditions:
            assert isinstance(x, Condition)
        if len(conditions) == 1:
            self.criteria = conditions[0]
        else:
            self.criteria = AndCondition(*conditions)
        return self

    def order_by(self, meth, direction=Sort.ASCENDING):
        self.sorting = (meth, direction)
        return self
