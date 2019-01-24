import tempfile

from dredis.keyspace import Keyspace
from dredis.ldb import LEVELDB


def test_delete():
    tempdir = tempfile.mkdtemp(prefix="redis-test-")
    LEVELDB.setup_dbs(tempdir)
    keyspace = Keyspace()

    keyspace.select('0')
    keyspace.set('mystr', 'test')
    keyspace.sadd('myset', 'elem1')
    keyspace.zadd('myzset', 0, 'elem1')
    keyspace.hset('myhash', 'testkey', 'testvalue')

    keyspace.delete('mystr', 'myset', 'myzset', 'myhash', 'notfound')

    assert list(LEVELDB.get_db('0').iterator()) == []
