import pytest
import redis

from tests.helpers import fresh_redis


def test_types():
    r = fresh_redis()

    r.set('emptystr', '')
    r.set('mystr', 'test')
    r.incr('myint')
    r.sadd('myset', 'test')
    r.zadd('myzset', 0, 'test')
    r.hset('myhash1', 'field', 'value')
    r.hsetnx('myhash2', 'field', 'value')

    assert r.type('emptystr') == 'string'
    assert r.type('mystr') == 'string'
    assert r.type('myint') == 'string'
    assert r.type('myset') == 'set'
    assert r.type('myzset') == 'zset'
    assert r.type('myhash1') == 'hash'
    assert r.type('myhash2') == 'hash'
    assert r.type('notfound') == 'none'


def test_keys():
    r = fresh_redis()

    r.set('mystr', 'test')
    r.incr('myint')
    r.sadd('myset', 'test')
    r.zadd('myzset', 0, 'test')
    r.hset('myhash', 'test', 'testvalue')

    assert r.keys('myi*') == ['myint']

    # order isn't guaranteed
    all_keys = r.keys('*')
    assert len(all_keys) == 5
    assert sorted(all_keys) == sorted(['mystr', 'myint', 'myset', 'myzset', 'myhash'])
    assert sorted(r.keys('my*set')) == sorted(['myset', 'myzset'])
    assert r.keys('my?et') == ['myset']


def test_exists():
    r = fresh_redis()

    assert r.exists('notfound') == 0

    r.set('mystr', 'test')
    assert r.exists('mystr') == 1

    r.sadd('myset', 'elem1')
    assert r.exists('myset') == 1

    r.zadd('myzset', 0, 'elem1')
    assert r.exists('myzset') == 1

    r.hset('myhash', 'testkey', 'testvalue')
    assert r.exists('myhash') == 1

    # redis-py doesn't support multiple args to `r.exists()`
    assert r.execute_command('EXISTS', 'mystr', 'notfound') == 1


def test_delete():
    r = fresh_redis()

    r.set('mystr', 'test')
    r.sadd('myset', 'elem1')
    r.zadd('myzset', 0, 'elem1')
    r.hset('myhash', 'testkey', 'testvalue')

    assert r.delete('mystr', 'myset', 'myzset', 'myhash', 'notfound') == 4
    assert r.keys('*') == []


def test_dump():
    r = fresh_redis()

    r.set('str', 'test')
    assert r.dump('str') in [
        b'\x00\x04test\x07\x00~\xa2zSd;e_',  # RDB 7 (dredis & redis 3.x)
        b'\x00\x04test\t\x00Qb\xfel8w\xd3\xf4',  # RDB 9 (redis 5.x) (`make fulltests-real-redis`)
    ]


def test_restore():
    r = fresh_redis()

    r.set('str1', 'test')
    payload = r.dump('str1')
    r.set('str1', 'test2')
    r.restore('str1', 0, payload, replace=True)
    r.restore('str2', 0, payload, replace=False)

    assert r.get('str1') == 'test'
    assert r.get('str2') == 'test'


def test_restore_with_valid_params():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError) as exc:
        r.execute_command('RESTORE', 'str1')
    assert str(exc.value) == "wrong number of arguments for 'restore' command"

    with pytest.raises(redis.ResponseError) as exc:
        r.execute_command('RESTORE', 'str1', 'a')
    assert str(exc.value) == "wrong number of arguments for 'restore' command"

    with pytest.raises(redis.ResponseError) as exc:
        r.execute_command('RESTORE', 'str1', '0', 'payload', 'repl')
    assert str(exc.value) == "syntax error"


def test_rename():
    r = fresh_redis()

    r.set('mystr', 'test')
    r.sadd('myset', 'a', 'b', 'c')
    r.hset('myhash', 'field1', 'value1')
    r.hset('myhash', 'field2', 'value2')
    r.zadd('myzet', a=0, b=1)

    assert r.rename('mystr', 'mystr2')
    assert r.rename('myset', 'myset2')
    assert r.rename('myhash', 'myhash2')
    assert r.rename('myzet', 'myzset2')

    assert r.get('mystr2') == 'test'
    assert r.smembers('myset2') == set(['a', 'b', 'c'])
    assert r.hgetall('myhash2') == {'field1': 'value1', 'field2': 'value2'}
    assert r.zrange('myzset2', 0, -1, withscores=True) == [('a', 0), ('b', 1)]


def test_rename_when_key_doesnt_exist():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError, match='no such key'):
        r.rename('notfound', 'newname')


def test_rename_when_new_key_already_exists():
    r = fresh_redis()

    r.set('mystr1', 'testvalue')
    r.set('mystr2', 'another testvalue')
    assert r.rename('mystr1', 'mystr2')
    assert r.get('mystr2') == 'testvalue'


def test_rename_with_same_name():
    r = fresh_redis()

    r.set('str', 'test')

    assert r.rename('str', 'str')


def test_expire_command_exists_but_is_noop():
    r = fresh_redis()

    r.set('str', 'test')
    assert r.expire('str', 1) == 1
    assert r.expire('another-str', 1) == 0


def test_ttl_command():
    r = fresh_redis()

    r.set('str', 'test')

    assert r.ttl('str') == -1
    assert r.ttl('another-str') == -2

    # TODO: after implementing proper key expiration, the following test should work
    #  r.expire('str', 1)
    #  assert r.ttl('str') == 1
