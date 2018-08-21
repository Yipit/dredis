import pytest
import redis

from tests.helpers import HOST, PORT


def test_basic_lua_evaluation():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.eval("return 123", 0) == 123
    assert r.eval("return KEYS", 2, "key1", "key2", "arg1") == ['key1', 'key2']
    assert r.eval("return ARGV", 2, "key1", "key2", "arg1") == ['arg1']


def test_lua_with_redis_call():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.eval("""\
redis.call('set', KEYS[1], KEYS[2])
return redis.call('get', KEYS[1])""", 2, "testkey", "testvalue") == "testvalue"


def test_lua_with_redis_error_call():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()
    with pytest.raises(redis.ResponseError) as exc:
        r.eval("""return redis.call('cmd_not_found')""", 0)
    assert exc.value.message == '@user_script: Unknown Redis command called from Lua script'


def test_lua_with_redis_error_pcall():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()
    with pytest.raises(redis.ResponseError) as exc:
        r.eval("""return redis.pcall('cmd_not_found')""", 0)
    assert exc.value.message == (
        'Error running script: @user_script: Unknown Redis command called from Lua script')
