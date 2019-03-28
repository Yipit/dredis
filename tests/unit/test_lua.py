import pytest

from lupa._lupa import LuaRuntime
from dredis.keyspace import Keyspace
from dredis.lua import RedisScriptError
from dredis.lua import LuaRunner, RedisLua


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


def test_redislua_return_lua_types_call():
    k = Keyspace()
    lua_runtime = LuaRuntime(unpack_returned_tuples=True)
    redis_lua = RedisLua(k, lua_runtime)
    lua_script = """return {'test', true, false, 10, 20.3, {'another string'}, redis.call('ping')}"""
    table = redis_lua.call('EVAL', lua_script, 0, [])

    assert table[1] == 'test'
    assert table[2] == 1
    assert table[3] is False
    assert table[4] == 10
    assert table[5] == 20
    assert table[6][1] == 'another string'
    assert table[7] == 'PONG'


def test_redislua_return_lua_types_pcall():
    k = Keyspace()
    lua_runtime = LuaRuntime(unpack_returned_tuples=True)
    redis_lua = RedisLua(k, lua_runtime)
    lua_script = """return {'test', true, false, 10, 20.3, {'another string'}, redis.call('ping')}"""
    table = redis_lua.pcall('EVAL', lua_script, 0, [])

    assert table[1] == 'test'
    assert table[2] == 1
    assert table[3] is False
    assert table[4] == 10
    assert table[5] == 20
    assert table[6][1] == 'another string'
    assert table[7] == 'PONG'


def test_redislua_with_error_call():
    k = Keyspace()
    lua_runtime = LuaRuntime(unpack_returned_tuples=True)
    redis_lua = RedisLua(k, lua_runtime)

    with pytest.raises(RedisScriptError) as exc:
        redis_lua.call('GET')

    assert str(exc.value) == "wrong number of arguments for 'get' command"


def test_redislua_with_error_pcall():
    k = Keyspace()
    lua_runtime = LuaRuntime(unpack_returned_tuples=True)
    redis_lua = RedisLua(k, lua_runtime)
    table = redis_lua.pcall('GET')

    assert table['err'] == "wrong number of arguments for 'get' command"


def test_redislua_with_command_error_call():
    k = Keyspace()
    lua_runtime = LuaRuntime(unpack_returned_tuples=True)
    redis_lua = RedisLua(k, lua_runtime)

    with pytest.raises(RedisScriptError) as exc:
        redis_lua.call('cmd_not_found')

    assert str(exc.value) == '@user_script: Unknown Redis command called from Lua script'


def test_redislua_with_command_error_pcall():
    k = Keyspace()
    lua_runtime = LuaRuntime(unpack_returned_tuples=True)
    redis_lua = RedisLua(k, lua_runtime)
    table = redis_lua.pcall('cmd_not_found')

    assert table['err'] == '@user_script: Unknown Redis command called from Lua script'
