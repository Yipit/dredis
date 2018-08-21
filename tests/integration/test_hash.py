import redis
from tests.helpers import HOST, PORT


def test_hset_and_hget():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.hset('myhash', 'key1', 'value1') == 1
    assert r.hset('myhash', 'key1', 'value1') == 0
    assert r.hset('myhash', 'key2', 'value2') == 1

    assert r.hget('myhash', 'key1') == 'value1'
    assert r.hget('myhash', 'key2') == 'value2'
    assert r.hget('myhash', 'notfound') is None


def test_hkeys():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')

    # order isn't guaranteed
    result = r.hkeys('myhash')
    assert len(result) == 2
    assert sorted(result) == sorted(['key1', 'key2'])


def test_hvals():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')

    # order isn't guaranteed
    result = r.hvals('myhash')
    assert len(result) == 2
    assert sorted(result) == sorted(['value1', 'value2'])


def test_hlen():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')
    assert r.hlen('myhash') == 2
    assert r.hlen('notfound') == 0
