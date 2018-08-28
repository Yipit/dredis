import pytest

from dredis.keyspace import RedisScriptError, DiskKeyspace


def test_eval_with_error_call():
    k = DiskKeyspace()

    with pytest.raises(RedisScriptError) as exc:
        k.eval("""return redis.call('cmd_not_found')""", 0, ())
    assert exc.value.message == '@user_script: Unknown Redis command called from Lua script'


def test_eval_with_error_pcall():
    k = DiskKeyspace()

    with pytest.raises(ValueError, message='ERR Error running script: @user_script: Unknown Redis command called from Lua script'):
        k.eval("""return redis.pcall('cmd_not_found')""", 0, ())
