import os
import os.path
from io import BytesIO

import pytest

import dredis
from dredis import rdb, crc64
from dredis.exceptions import DredisError
from dredis.keyspace import Keyspace
from tests.fixtures import reproduce_dump


FIXTURE_DUMP = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'fixtures',
    'dump.rdb',
)


def test_load_rdb_with_strings(keyspace):
    rdb_file = BytesIO(
        'REDIS0007'  # "REDIS" + version
        '\xfa\tredis-ver\x053.2.6'  # aux field
        '\xfa\nredis-bits\xc0@'  # aux field
        '\xfa\x05ctime\xc2\xdf\x0c\x04]'  # aux field
        '\xfa\x08used-mem\xc2\x80\xdd\x0e\x00'  # aux field
        '\xfe\x00'  # RDB_OPCODE_SELECTDB, current db
        '\xfb\x02\x00'  # RDB_OPCODE_RESIZEDB, db size, expires size
        '\x00\x04key1\x06value1'  # type, length of key, key, length of value, value
        '\x00\x04key2\x06value2'  # type, length of key, key, length of value, value
        '\xff'  # RDB_OPCODE_EOF
        ')\xd4\xff\x8c\x833W\x8a'  # checksum
    )

    rdb.load_rdb(keyspace, rdb_file)

    assert sorted(keyspace.keys('*')) == sorted(['key1', 'key2'])
    assert keyspace.get('key1') == 'value1'
    assert keyspace.get('key2') == 'value2'


def test_load_invalid_rdb(keyspace):
    with pytest.raises(DredisError) as exc:
        rdb_file = BytesIO('XREDIS0007')
        rdb.load_rdb(keyspace, rdb_file)
    assert str(exc).endswith('Wrong signature trying to load DB from file')

    with pytest.raises(DredisError) as exc:
        rdb_file = BytesIO('REDIS000X')
        rdb.load_rdb(keyspace, rdb_file)
    assert str(exc).endswith("Can't handle RDB format version 000X")

    with pytest.raises(DredisError) as exc:
        rdb_file = BytesIO('REDIS0011')
        rdb.load_rdb(keyspace, rdb_file)
    assert str(exc).endswith("Can't handle RDB format version 0011")


def test_save_rdb_with_strings(keyspace):
    expected_rdb_content = bytes(
        'REDIS0007'  # "REDIS" + version
        +
        ('\xfa\ndredis-ver%c%s' % (len(dredis.__version__), dredis.__version__))  # aux field
        +
        '\xfe\x00'  # RDB_OPCODE_SELECTDB, current db
        # dredis doesn't implement `RDB_OPCODE_RESIZEDB`
        '\x00\x04key1\x06value1'  # type, length of key, key, length of value, value
        '\x00\x04key2\x06value2'  # type, length of key, key, length of value, value
        '\xff'  # RDB_OPCODE_EOF
    )
    expected_rdb_content += crc64.checksum(expected_rdb_content)
    keyspace.set('key1', 'value1')
    keyspace.set('key2', 'value2')

    filename = 'test-dump.rdb'
    try:
        rdb.dump_rdb(keyspace, filename)
        content = open(filename, 'rb').read()
    finally:
        # cleanup to avoid extra test file
        os.remove(filename)

    assert content == expected_rdb_content


def test_rdb_load(keyspace):
    rdb.load_rdb(keyspace, open(FIXTURE_DUMP, 'rb'))

    new_keyspace = Keyspace()
    new_keyspace.select(1)
    reproduce_dump.run(new_keyspace)

    assert new_keyspace.keys('*') == keyspace.keys('*')

    for key in new_keyspace.keys('string_*'):
        assert new_keyspace.get(key) == keyspace.get(key)

    for key in new_keyspace.keys('set_*'):
        assert new_keyspace.smembers(key) == keyspace.smembers(key)

    for key in new_keyspace.keys('zset_*'):
        assert new_keyspace.zrange(key, 0, -1, with_scores=True) == keyspace.zrange(key, 0, -1, with_scores=True)

    for key in new_keyspace.keys('hash_*'):
        assert new_keyspace.hgetall(key) == keyspace.hgetall(key)
