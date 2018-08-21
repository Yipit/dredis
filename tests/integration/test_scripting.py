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
    assert exc.value.message == '@user_script: Unknown Redis command called from Lua script'


def test_lua_with_redis_error_pcall():
    r = fresh_redis()
    with pytest.raises(redis.ResponseError) as exc:
        r.eval("""return redis.pcall('cmd_not_found')""", 0)
    assert exc.value.message == (
        'Error running script: @user_script: Unknown Redis command called from Lua script')


def test_commands_should_be_case_insensitive_inside_lua():
    r = fresh_redis()

    assert r.eval("""\
redis.call('SeT', KEYS[1], KEYS[2])
return redis.call('Get', KEYS[1])""", 2, "testkey", "testvalue") == "testvalue"


def test_array_of_arrays_in_lua():
    r = fresh_redis()
    assert r.eval('return {{"a","one"}, {"b","two"}, {"c","three"}}', 0) == [['a', 'one'], ['b', 'two'], ['c', 'three']]
