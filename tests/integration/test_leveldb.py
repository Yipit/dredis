import tempfile

from dredis.gc import KeyGarbageCollector
from dredis.keyspace import Keyspace
from dredis.db import DB_MANAGER


def test_delete():
    tempdir = tempfile.mkdtemp(prefix="redis-test-")
    DB_MANAGER.setup_dbs(tempdir, backend='leveldb', backend_options={})
    keyspace = Keyspace()

    keyspace.select('0')
    keyspace.set('mystr', 'test')
    keyspace.sadd('myset', 'elem1')
    keyspace.zadd('myzset', 0, 'elem1')
    keyspace.hset('myhash', 'testkey', 'testvalue')

    keyspace.delete('mystr', 'myset', 'myzset', 'myhash', 'notfound')

    KeyGarbageCollector().collect()

    assert list(DB_MANAGER.get_db('0').iterator()) == []
