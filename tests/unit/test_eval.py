import pytest

from dredis.keyspace import Keyspace
from dredis.exceptions import RedisScriptError


def test_eval_with_error_call():
    k = Keyspace()

    with pytest.raises(RedisScriptError) as exc:
        k.eval("""return redis.call('cmd_not_found')""", [], [])
    assert str(exc.value) == '@user_script: Unknown Redis command called from Lua script'


def test_eval_with_error_pcall():
    k = Keyspace()

    with pytest.raises(RedisScriptError, message='Error running script: @user_script: Unknown Redis command called from Lua script'):
        k.eval("""return redis.pcall('cmd_not_found')""", [], [])
