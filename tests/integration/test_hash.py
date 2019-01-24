import pytest
import redis

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
    assert r.hdel('myhash', 'notfound') == 0


def test_hincrby():
    r = fresh_redis()

    assert r.hincrby('myhash', 'key1', 0) == 0
    assert r.hincrby('myhash', 'key1', 1) == 1

    r.hset('myhash', 'key2', 10)
    assert r.hincrby('myhash', 'key2', 5) == 15
    assert r.hget('myhash', 'key2') == '15'


def test_hgetall():
    r = fresh_redis()

    r.hset('myhash', 'key1', 'value1')
    r.hset('myhash', 'key2', 'value2')
    r.hset('myhash', 'key3', 'value3')

    assert r.hgetall('myhash') == {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}


def test_empty_hash_shouldnt_be_in_keyspace():
    r = fresh_redis()

    r.hset('myhash', 'key1', 'value1')
    r.hdel('myhash', 'key1')

    assert r.keys() == []


def test_hset_should_accept_multiple_key_value_pairs():
    r = fresh_redis()

    assert r.execute_command('HSET', 'myhash', 'k1', 'v1', 'k2', 'v2') == 2
    assert r.hgetall('myhash') == {'k1': 'v1', 'k2': 'v2'}

    with pytest.raises(redis.ResponseError) as exc:
        r.execute_command('HSET', 'myhash', 'k1', 'v1', 'k2')
    assert str(exc.value) == 'wrong number of arguments for HMSET'
