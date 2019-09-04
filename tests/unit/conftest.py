import pytest

from dredis import config
from dredis.db import DB_MANAGER
from dredis.keyspace import Keyspace


@pytest.fixture
def keyspace():
    DB_MANAGER.setup_dbs('', backend='memory', backend_options={})
    original_configs = config.get_all('*')
    yield Keyspace()
    for option, value in zip(original_configs[0::2], original_configs[1::2]):
        config.set(option, value)
