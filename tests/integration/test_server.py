from tests.helpers import fresh_redis


def test_flushall():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.flushall() is True

    assert r0.keys('*') == []
    assert r1.keys('*') == []
