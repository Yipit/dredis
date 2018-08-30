from tests.helpers import fresh_redis


def test_hset_and_hget():
    r = fresh_redis()

    assert r.hset('myhash', 'key1', 'value1') == 1
    assert r.hset('myhash', 'key1', 'value1') == 0
    assert r.hset('myhash', 'key2', 'value2') == 1

    assert r.hget('myhash', 'key1') == 'value1'
    assert r.hget('myhash', 'key2') == 'value2'
    assert r.hget('myhash', 'notfound') is None


def test_hkeys():
    r = fresh_redis()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')

    # order isn't guaranteed
    result = r.hkeys('myhash')
    assert len(result) == 2
    assert sorted(result) == sorted(['key1', 'key2'])


def test_hvals():
    r = fresh_redis()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')

    # order isn't guaranteed
    result = r.hvals('myhash')
    assert len(result) == 2
    assert sorted(result) == sorted(['value1', 'value2'])


def test_hlen():
    r = fresh_redis()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')
    assert r.hlen('myhash') == 2
    assert r.hlen('notfound') == 0


def test_hsetnx():
    r = fresh_redis()

    assert r.hsetnx('myhash', 'key1', 'value1') == 1
    assert r.hsetnx('myhash', 'key1', 'value2') == 0
    assert r.hget('myhash', 'key1') == 'value1'


def test_hdel():
    r = fresh_redis()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')

    assert r.hdel('myhash', 'key1', 'key2') == 2
    assert r.hget('myhash', 'key1') is None
    assert r.hget('myhash', 'key2') is None
