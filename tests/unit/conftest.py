import pytest

from dredis.db import DB_MANAGER
from dredis.keyspace import Keyspace


@pytest.fixture
def keyspace():
    DB_MANAGER.setup_dbs('', backend='memory', backend_options={})
    return Keyspace()
