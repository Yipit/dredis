from tests.helpers import fresh_redis


def test_select():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.keys('*') == ['test1']
    assert r1.keys('*') == ['test2']
