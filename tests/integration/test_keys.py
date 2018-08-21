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
