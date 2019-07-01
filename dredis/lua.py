import json

from lupa._lupa import LuaRuntime

from dredis.commands import run_command, SimpleString
from dredis.exceptions import CommandNotFound, RedisScriptError, DredisError


class RedisLua(object):

    def __init__(self, keyspace, lua_runtime):
        self._keyspace = keyspace
        self._lua_runtime = lua_runtime

    def call(self, cmd, *args):
        try:
            result = run_command(self._keyspace, cmd, args)
        except CommandNotFound:
            raise RedisScriptError('@user_script: Unknown Redis command called from Lua script')
        except DredisError as exc:
            raise RedisScriptError(exc.msg)
        else:
            return self._convert_redis_types_to_lua_types(result)

    def pcall(self, cmd, *args):
        try:
            return self.call(cmd, *args)
        except Exception as exc:
            table = self._lua_runtime.table()
            table['err'] = str(exc)
            return table

    def _convert_redis_types_to_lua_types(self, result):
        """
        Redis reply should be converted to the equivalent lua type
        The official implementation converts:
          * $-1 and *-1 to `false`
          * errors  to `{err=ERRORMSG}`
          * simple strings to `{ok=STRING}`
          * integers to numbers
          * arrays to lua tables following the previous conversions

        The implementation can be found at:
        https://github.com/antirez/redis/blob/5b4bec9d336655889641b134791dfdd2adc864cf/src/scripting.c#L106-L201
        """

        if isinstance(result, (tuple, list, set)):
            table = self._lua_runtime.table()
            for i, elem in enumerate(result, start=1):
                table[i] = self._convert_redis_types_to_lua_types(elem)
            return table
        elif result is None:
            return False
        elif result is True:
            return 1
        elif isinstance(result, SimpleString):
            table = self._lua_runtime.table()
            table["ok"] = result
            return table
        else:
            return result


class LuaRunner(object):
    def __init__(self, keyspace):
        self._runtime = LuaRuntime(unpack_returned_tuples=True)
        self._lua_table_type = type(self._runtime.table())
        self._redis_obj = RedisLua(keyspace, self._runtime)

    def run(self, script, keys, argv):
        self._runtime.execute('KEYS = {%s}' % ', '.join(map(json.dumps, keys)))
        self._runtime.execute('ARGV = {%s}' % ', '.join(map(json.dumps, argv)))
        script_function = self._runtime.eval('function(redis) {} end'.format(script))
        result = script_function(self._redis_obj)
        return self._convert_lua_types_to_redis_types(result)

    def _convert_lua_types_to_redis_types(self, result):
        def convert(value):
            """
            str -> str
            true -> 1
            false -> None
            number -> int
            table -> {
                if 'err' key is present, raise an error
                else if 'ok' key is present, return its value
                else convert to a list using the previous rules
            }

            Reference:
            https://github.com/antirez/redis/blob/5b4bec9d336655889641b134791dfdd2adc864cf/src/scripting.c#L273-L340

            """
            if isinstance(value, self._lua_table_type):
                if 'err' in value:
                    raise RedisScriptError(value['err'])
                elif 'ok' in value:
                    return value['ok']
                else:
                    return map(convert, value.values())
            elif isinstance(value, (tuple, list, set)):
                return map(convert, value)
            elif value is True:
                return 1
            elif value is False:
                return None
            elif isinstance(value, float):
                return int(value)
            else:
                # assuming string at this point
                return value

        return convert(result)
