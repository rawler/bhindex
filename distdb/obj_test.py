from nose.tools import *
from distdb.obj import ValueSet, Object

@raises(TypeError)
def test_ValueSet_void_ctor():
    ValueSet()

def test_ValueSet_empty():
    vs = ValueSet([])
    assert_is_none(vs.any(), None)
    assert_is(vs.any('default'), 'default')
    assert_equals(vs.join(), u'')
    assert_is_not_none(vs.t, None)

def test_ValueSet_single():
    vs = ValueSet(u'apa')
    assert_is(vs.any(), u'apa')
    assert_equals(vs.join(), u'apa')

def test_ValueSet_list():
    vs = ValueSet([u'a',u'2'])
    assert_true(vs.any())
    assert_equals(vs.join(), u'a, 2')
    assert_equals(vs.join('#'), u'a#2')

def test_ValueSet_update():
    vs = ValueSet(u'a', t=0)
    assert_equals(vs, set([u'a']))

    vs.update([u'b'])
    assert_equals(vs, set([u'a', u'b']))

    t = int(vs.t)
    vs.update([], 0)
    assert_equals(vs, set([u'a', u'b']))
    assert_equals(int(vs.t), t)

    vs.update([], t+5)
    assert_equals(int(vs.t), t+5)

def test_Object_new():
    assert_regexp_matches(Object.new().id, r'[A-Za-z0-9-\-\_]{22}'),
    assert_regexp_matches(Object.new('').id, r':[A-Za-z0-9-\-\_]{22}'),
    assert_regexp_matches(Object.new('dir').id, r'dir:[A-Za-z0-9-\-\_]{22}'),

@raises(TypeError)
def test_Object_void_ctor():
    Object()

def test_Object_empty():
    o = Object('aia')

    assert_raises(KeyError, lambda: o['apa'])
    assert_is_none(o.get('apa'))
    assert_equals(o.get('apa', 'default'), 'default')
    assert_is_none(o.any('apa'))
    assert_equals(o.any('apa', 'default'), 'default')
    assert_not_in('apa', o)
    assert_equals(dict(o.iteritems()), dict())
    assert_equals(repr(o), repr(dict()))
    assert_equals(unicode(o), u"db.Object {\n\n}")
    assert_is_none(o.timestamp())
    assert_is(o.timestamp(5), 5)
    assert_true(o.matches({}))
    assert_false(o.matches({u'apa': u'something'}))

def test_Object_accessor():
    apa = ValueSet([u'banan', u'citron'])
    o = Object('aia', {u'apa': apa})

    assert_equals(o['apa'], apa)
    assert_equals(o.get('apa'), apa)
    assert_equals(o.any('apa'), 'banan')
    assert_in('apa', o)
    assert_not_in('banan', o)
    assert_equals(dict(o.iteritems()), dict(apa=apa))
    assert_equals(repr(o), repr({u'apa': apa}))
    assert_equals(unicode(o), u"db.Object {\n apa: banan, citron\n}")
    assert_almost_equal(o.timestamp(), apa.t)
    assert_false(o.matches({u'apa': u'something'}))
    assert_true(o.matches({u'apa': u'citron'}))

def test_Object_mutation():
    apa = ValueSet([u'banan', u'citron'])
    o = Object('aia')

    assert_not_in('apa', o)
    o[u'apa'] = apa
    assert_in(u'apa', o)
    assert_equals(o[u'apa'], apa)

    del o[u'apa']
    assert_not_in(u'apa', o)

    o.update_key(u'apa', u'banan')
    assert_equals(o[u'apa'], ValueSet(u'banan'))

    o.update_key(u'apa', u'citron')
    assert_equals(o[u'apa'], ValueSet([u'banan', u'citron']))
