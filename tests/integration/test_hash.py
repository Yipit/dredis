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
