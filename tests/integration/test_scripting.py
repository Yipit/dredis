import pytest
import redis
from tests.helpers import fresh_redis


def test_basic_lua_evaluation():
    r = fresh_redis()

    assert r.eval("return 123", 0) == 123
    assert r.eval("return KEYS", 2, "key1", "key2", "arg1") == ['key1', 'key2']
    assert r.eval("return ARGV", 2, "key1", "key2", "arg1") == ['arg1']


def test_lua_with_redis_call():
    r = fresh_redis()

    assert r.eval("""\
redis.call('set', KEYS[1], KEYS[2])
return redis.call('get', KEYS[1])""", 2, "testkey", "testvalue") == "testvalue"


def test_lua_with_redis_error_call():
    r = fresh_redis()
    with pytest.raises(redis.ResponseError) as exc:
        r.eval("""return redis.call('cmd_not_found')""", 0)
    assert str(exc.value).strip().endswith('Unknown Redis command called from Lua script')


def test_lua_with_redis_error_pcall():
    r = fresh_redis()
    with pytest.raises(redis.ResponseError) as exc:
        r.eval("""return redis.pcall('cmd_not_found')""", 0)
    assert str(exc.value).strip().endswith('Unknown Redis command called from Lua script')


def test_commands_should_be_case_insensitive_inside_lua():
    r = fresh_redis()

    assert r.eval("""\
redis.call('SeT', KEYS[1], KEYS[2])
return redis.call('Get', KEYS[1])""", 2, "testkey", "testvalue") == "testvalue"


def test_array_of_arrays_in_lua():
    r = fresh_redis()
    assert r.eval('return {{"a","one"}, {"b","two"}, {"c","three"}}', 0) == [['a', 'one'], ['b', 'two'], ['c', 'three']]


def test_python_objects_inside_lua():
    # this is a regression test due to a problem using python objects inside of lua
    r = fresh_redis()
    assert r.eval('''local list = redis.call('zrange', 'notfound', 0, 1); return #list''', 0) == 0
    assert r.eval('''return redis.call('set', 'foo', 'bar')''', 0) == 'OK'
    assert r.eval('''return redis.call('zcard', 'myzset')''', 0) == 0
    assert r.eval('''return redis.call('get', 'notfound')''', 0) is None
    assert r.eval('''
        local list = redis.call('zrange', 'notfound', 0, 1)
        return {#KEYS, #ARGV, #list}''', 1, "foo", [1], "a", 1, 2.0) == [1, 4, 0]
    # NOTE: The Redis protocol doesn't have booleans, so True is converted to `1` and `false` to `None`
    assert r.eval('''return 1 == 1''', 0) == 1
    assert r.eval('''return 1 == 2''', 0) is None


def test_should_convert_redis_reply_to_lua_types_inside_script():
    # Reference:
    # https://github.com/antirez/redis/blob/5b4bec9d336655889641b134791dfdd2adc864cf/src/scripting.c#L106-L201
    r = fresh_redis()

    assert r.eval('return redis.call("zrank", "mykey", "myvalue") == false', 0) == 1
    assert r.eval('return redis.call("set", "s", "bar")["ok"]', 0) == 'OK'
    assert r.eval('return redis.call("get", "s") == "bar"', 0) == 1
    assert r.eval('return redis.call("incr", "i") == 1', 0) == 1
    assert r.eval('''
        local result = redis.pcall("cmdnotfound")
        return string.find(result["err"], "Unknown Redis command called from Lua script")
    ''', 0) > 0
    assert r.eval('return #redis.call("zrange", "myzset", 0, 0) == 0', 0) == 1


def test_should_convert_lua_types_to_redis_reply():
    # Reference:
    # https://github.com/antirez/redis/blob/5b4bec9d336655889641b134791dfdd2adc864cf/src/scripting.c#L106-L201
    r = fresh_redis()

    assert r.eval('return redis.call("zrank", "mykey", "myvalue")', 0) is None
    assert r.eval('return redis.call("set", "s", "bar")', 0) == 'OK'
    assert r.eval('return redis.call("get", "s")', 0) == "bar"
    assert r.eval('return redis.call("incr", "i")', 0) == 1
    with pytest.raises(
            redis.exceptions.ResponseError,
            message="@user_script: Unknown Redis command called from Lua script"):
        r.eval('return redis.pcall("cmdnotfound")', 0)
    r.zadd('myzset', 0, 'value1')
    r.zadd('myzset', 1, 'value2')
    assert r.eval('return redis.call("zrange", "myzset", 0, -1)', 0) == ['value1', 'value2']
