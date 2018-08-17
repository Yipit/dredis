import redis

from tests.helpers import HOST, PORT


def test_set_string():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.set('foo', 'bar') is True
    assert r.set('foo', 'bar') is True


def test_get_string():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.get('foo') is None
    r.set('foo', 'bar')
    assert r.get('foo') == 'bar'


def test_set_and_get_bytes():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.set('foo', b'\x05\x02\x03') is True
    assert r.get('foo') == b'\x05\x02\x03'


def test_set_and_get_integer():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    r.set('foo', 1)
    assert r.get('foo') == '1'


def test_incr_integers():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.incr('a') == 1
    assert r.incr('a') == 2

    r.set('b', 10)
    assert r.incr('b') == 11
    assert r.incr('b') == 12
