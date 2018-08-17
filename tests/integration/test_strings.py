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
