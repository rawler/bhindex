from nose.tools import *
from distdb.query import *
from distdb.query import _mkset as fz


def test_simple_queries():
    baseq = 'SELECT objid, value FROM map NATURAL JOIN list '

    assert_equal(
        Query(('objid', 'value')).where(Key('xt').any()).apply(),
        (baseq + 'NATURAL JOIN key WHERE LIKELIHOOD(key=?, 0.2) AND listid IS NOT NULL', ('xt',)),
    )

    assert_equal(
        Query(('objid', 'value')).where(Key(14).any()).apply(),
        (baseq + 'WHERE LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL', (14,)),
    )


def test_Key():
    assert_equal(
        Key('xt').any().apply(),
        ConditionExpr(fz('key'), 'LIKELIHOOD(key=?, 0.2) AND listid IS NOT NULL', ('xt'), True),
    )

    assert_equal(
        Key(14).any().apply(),
        ConditionExpr(fz(()), 'LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL', (14), True),
    )

    assert_equal(
        (Key(14) == 'monkey').apply(),
        ConditionExpr(fz('list'), 'LIKELIHOOD(keyid=?, 0.2) AND (value=?)', (14, 'monkey'), True),
    )

    assert_equal(
        (Key(14).startswith('monkey')).apply(),
        ConditionExpr(fz('list'), "LIKELIHOOD(keyid=?, 0.2) AND (value GLOB 'monkey*')", (14), True),
    )

    assert_equal(
        (Key(14).startswith(('monkey', 'banana'))).apply(),
        ConditionExpr(fz('list'), "LIKELIHOOD(keyid=?, 0.2) AND (value GLOB 'monkey*' OR value GLOB 'banana*')", (14), True),
    )

    assert_equal(
        (Key(14).timed_before(42)).apply(),
        ConditionExpr(fz(()), "LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL AND timestamp < ?", (14, 42), True),
    )

    assert_equal(
        (Key(14).missing()).apply(),
        ConditionExpr(fz(()), "objid NOT IN (SELECT objid FROM map WHERE LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL)", (14), False),
    )


def test_multiple_And_Key_Crit():
    assert_equal(
        AndCondition(Key('xt').any(), Key(32).any()).apply(),
        ConditionExpr(
            fz('key'),
            '(LIKELIHOOD(key=?, 0.2) AND listid IS NOT NULL) AND (objid IN'
            ' (SELECT objid FROM map WHERE LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL))',
            ('xt', 32),
            True
        ),
    )

    assert_equal(
        AndCondition(ObjId == 'apa', Key(32).any()).apply(),
        ConditionExpr(
            fz(('obj')),
            '((obj=?)) AND (LIKELIHOOD(keyid=?, 0.2) AND listid IS NOT NULL)',
            ('apa', 32),
            True
        ),
    )


def test_ObjId():
    assert_equal(
        (ObjId == 'monkey').apply(),
        ConditionExpr(fz('obj'), '(obj=?)', ('monkey',), False),
    )

    assert_equal(
        (ObjId.startswith('monkey')).apply(),
        ConditionExpr(fz('obj'), "(obj GLOB 'monkey*')", (), False),
    )


def test_Sorting():
    baseq = 'SELECT objid, value FROM map NATURAL JOIN list '

    assert_equal(
        Query(('objid', 'value')).where(Key('xt').any()).order_by(Sort.value, Sort.ASCENDING).apply(),
        (baseq + 'NATURAL JOIN key WHERE LIKELIHOOD(key=?, 0.2) AND listid IS NOT NULL ORDER BY value ASC', ('xt',)),
    )
