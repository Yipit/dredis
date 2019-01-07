import tempfile

import pytest

from dredis.keyspace import DiskKeyspace
from dredis.lua import RedisScriptError

test_dir = tempfile.mkdtemp(prefix="redis-test-")


def test_eval_with_error_call():
    k = DiskKeyspace(test_dir)

    with pytest.raises(RedisScriptError) as exc:
        k.eval("""return redis.call('cmd_not_found')""", [], [])
    assert str(exc.value) == '@user_script: Unknown Redis command called from Lua script'


def test_eval_with_error_pcall():
    k = DiskKeyspace(test_dir)

    with pytest.raises(ValueError, message='ERR Error running script: @user_script: Unknown Redis command called from Lua script'):
        k.eval("""return redis.pcall('cmd_not_found')""", [], [])
