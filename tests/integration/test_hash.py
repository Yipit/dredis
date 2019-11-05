import pytest
import redis

from tests.helpers import fresh_redis


HASH_MAX_ZIPLIST_ENTRIES = 512  # redis uses a ziplist for hashes of 512 or fewer entries
HASH_MIN_HASHTABLE_ENTRIES = HASH_MAX_ZIPLIST_ENTRIES + 100


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


def test_hscan_with_all_elements_returned():
    r = fresh_redis()

    pairs = [
        ('test1', 'a'),
        ('test2', 'b'),
        ('test3', 'c'),
        ('test4', 'd'),
        ('test5', 'e'),
    ]
    for key, value in pairs:
        r.hset('myhash', key, value)
    r.hset('myhash1', 'test', 'test')

    cursor, elements = r.hscan('myhash', 0)
    assert cursor == 0
    assert elements == dict(pairs)


def test_hscan_with_a_subset_of_elements_returned():
    r = fresh_redis()

    # adding lots of elements to prevent real Redis from using a compact data structure
    # and returning all elements regardless of `COUNT`
    pairs = {'key{}'.format(i): 'value{}'.format(i) for i in range(HASH_MIN_HASHTABLE_ENTRIES)}
    for key, value in pairs.items():
        r.hset('myhash', key, value)

    cursor1, elems1 = r.hscan('myhash', 0, count=len(pairs) + 100)
    assert cursor1 == 0
    assert elems1 == dict(pairs)

    cursor2, elems2 = r.hscan('myhash', 0, count=2)
    assert cursor2 != 0

    for k, v in elems2.items():
        assert k in pairs
        assert pairs[k] == v


def test_hscan_with_a_subset_of_matching_elements_returned():
    r = fresh_redis()

    pairs = {
        'a-test1': 'a',
        'b-test2': 'b',
        'a-test3': 'c',
        'a-test4': 'd',
        'b-test5': 'e',
    }
    # adding lots of elements to prevent real Redis from using a compact data structure
    # and returning all elements regardless of `COUNT`
    pairs.update({'c-test{}'.format(i): 'c-value-{}'.format(i) for i in range(6, HASH_MIN_HASHTABLE_ENTRIES)})
    for key, value in pairs.items():
        r.hset('myhash', key, value)

    # MATCH is applied just before the elements are returned to the client, which means that you may need
    # multiple iterations to find a matching subset of elements
    cursor1 = 0
    matching_elems_found = {}
    while True:
        cursor1, elems1 = r.hscan('myhash', cursor1, match='a-*', count=2)
        assert len(elems1) <= 2
        matching_elems_found.update(elems1)
        if cursor1 == 0:
            break
    assert matching_elems_found == {
        'a-test1': 'a',
        'a-test3': 'c',
        'a-test4': 'd',
    }


def test_hscan_invalid_cursor():
    r = fresh_redis()
    with pytest.raises(redis.ResponseError) as exc:
        r.hscan('myhash', 'a1')
    assert str(exc.value) == "invalid cursor"


def test_hscan_invalid_count():
    r = fresh_redis()
    r.hset('myhash', 'test-key', 'test-value')
    with pytest.raises(redis.ResponseError) as exc:
        r.hscan('myhash', 0, count='a')
    assert str(exc.value) == "value is not an integer or out of range"


def test_hscan_with_a_cursor_that_doesnt_exist():
    r = fresh_redis()
    assert r.hscan('myhash', 123) == (0, {})
