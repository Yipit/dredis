import pytest

import dredis
from dredis import rdb, crc64


def test_load_rdb_with_strings(keyspace):
    rdb_content = (
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

    rdb.load_rdb(keyspace, rdb_content)

    assert sorted(keyspace.keys('*')) == sorted(['key1', 'key2'])
    assert keyspace.get('key1') == 'value1'
    assert keyspace.get('key2') == 'value2'


def test_load_invalid_rdb(keyspace):
    with pytest.raises(ValueError) as exc:
        rdb_content = 'XREDIS0007'
        rdb.load_rdb(keyspace, rdb_content)
    assert str(exc).endswith('Wrong signature trying to load DB from file')

    with pytest.raises(ValueError) as exc:
        rdb_content = 'REDIS000X'
        rdb.load_rdb(keyspace, rdb_content)
    assert str(exc).endswith("Can't handle RDB format version 000X")

    with pytest.raises(ValueError) as exc:
        rdb_content = 'REDIS0011'
        rdb.load_rdb(keyspace, rdb_content)
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
    rdb.dump_rdb(keyspace, filename)

    assert open(filename, 'rb').read() == expected_rdb_content
