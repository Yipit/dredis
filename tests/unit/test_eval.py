import tempfile

import pytest

from dredis.keyspace import Keyspace
from dredis.lua import RedisScriptError
from dredis.lua import LuaRunner

test_dir = tempfile.mkdtemp(prefix="redis-test-")


def test_eval_with_error_call():
    k = Keyspace()

    with pytest.raises(RedisScriptError) as exc:
        k.eval("""return redis.call('cmd_not_found')""", [], [])
    assert str(exc.value) == '@user_script: Unknown Redis command called from Lua script'


def test_eval_with_error_pcall():
    k = Keyspace()

    with pytest.raises(ValueError, message='ERR Error running script: @user_script: Unknown Redis command called from Lua script'):
        k.eval("""return redis.pcall('cmd_not_found')""", [], [])


def test_lua_return_redis_types_run():
    k = Keyspace()
    runner = LuaRunner(k)
    lua_script = """return {'test', true, false, 10, 20.3, {4}}"""

    assert runner.run(lua_script, [], []) == ['test', 1, None, 10, 20, [4]]


def test_lua_table_with_error_run():
    k = Keyspace()
    runner = LuaRunner(k)
    lua_script_err = """return {err='This is a ValueError'}"""

    with pytest.raises(ValueError) as e:
        runner.run(lua_script_err, [], [])

    assert str(e.value) == 'This is a ValueError'


def test_lua_table_with_ok_run():
    k = Keyspace()
    runner = LuaRunner(k)

    lua_script_ok = """return {ok='Everything is OK'}"""

    assert runner.run(lua_script_ok, [], []) == 'Everything is OK'
