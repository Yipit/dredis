from dredis.commands import run_command, SimpleString


class RedisScriptError(Exception):
    """Indicate error from calls to redis.call()"""


class RedisLua(object):

    def __init__(self, keyspace, lua_runtime):
        self._keyspace = keyspace
        self._lua_runtime = lua_runtime

    def call(self, cmd, *args):
        try:
            result = run_command(self._keyspace, cmd, args)
        except KeyError:
            raise RedisScriptError('@user_script: Unknown Redis command called from Lua script')
        except Exception as exc:
            raise RedisScriptError(str(exc))
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
