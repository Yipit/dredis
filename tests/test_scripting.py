import redis
from tests.helpers import HOST, PORT


def test_basic_lua_evaluation():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.eval("return 123", 0) == 123
    assert r.eval("return KEYS[1]", 0, "test") == "test"


def test_lua_with_redis_call():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()

    assert r.eval("""\
redis.call('set', KEYS[1], KEYS[2])
return redis.call('get', KEYS[1])""", 2, "testkey", "testvalue") == "testvalue"
