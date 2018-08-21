import redis
from tests.helpers import HOST, PORT


def test_types():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    r.set('mystr', 'test')
    r.incr('myint')
    r.sadd('myset', 'test')
    r.zadd('myzset', 0, 'test')

    assert r.type('mystr') == 'string'
    assert r.type('myint') == 'string'
    assert r.type('myset') == 'set'
    assert r.type('myzset') == 'zset'


def test_keys():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    r.set('mystr', 'test')
    r.incr('myint')
    r.sadd('myset', 'test')
    r.zadd('myzset', 0, 'test')

    assert r.keys('myi*') == ['myint']

    # order isn't guaranteed
    all_keys = r.keys('*')
    assert len(all_keys) == 4
    assert sorted(all_keys) == sorted(['mystr', 'myint', 'myset', 'myzset'])
    assert sorted(r.keys('my*set')) == sorted(['myset', 'myzset'])
    assert r.keys('my?et') == ['myset']
