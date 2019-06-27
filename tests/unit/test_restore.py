import struct
from io import BytesIO

import pytest

from dredis import crc64, rdb
from dredis.exceptions import BusyKeyError
from dredis.keyspace import to_float_string
from dredis.rdb import ObjectLoader


def test_should_raise_an_error_with_invalid_payload_size(keyspace):
    data = 'testvalue'
    with pytest.raises(ValueError) as exc:
        keyspace.restore('test', ttl=0, payload=data, replace=False)
    assert 'DUMP payload version or checksum are wrong' in str(exc)


def test_should_raise_an_error_with_incompatible_rdb_version(keyspace):
    data = '\x00'
    rdb_version = '\x08\x00'
    checksum = 'a' * 8
    with pytest.raises(ValueError) as exc:
        keyspace.restore('test', ttl=0, payload=data + rdb_version + checksum, replace=False)
    assert 'DUMP payload version or checksum are wrong' in str(exc)


def test_should_throw_an_error_with_invalid_checksum(keyspace):
    keyspace.set('test1', 'testvalue')
    payload = keyspace.dump('test1')[:-3] + 'bad'
    with pytest.raises(ValueError) as exc:
        keyspace.restore('test2', ttl=0, payload=payload, replace=False)
    assert 'DUMP payload version or checksum are wrong' in str(exc)


def test_should_raise_an_error_when_key_exists_and_replace_is_false(keyspace):
    keyspace.set('test', 'testvalue')
    with pytest.raises(BusyKeyError) as exc:
        keyspace.restore('test', ttl=0, payload='testvalue1', replace=False)
    assert 'BUSYKEY Target key name already exists' in str(exc)


def test_should_raise_an_error_with_bad_object_type(keyspace):
    keyspace.set('test', 'testvalue')
    partial_payload = 'X' + keyspace.dump('test')[:-10] + rdb.get_rdb_version()
    payload = partial_payload + crc64.checksum(partial_payload)

    with pytest.raises(ValueError) as exc:
        keyspace.restore('test', ttl=0, payload=payload, replace=True)
    assert 'Bad data format' in str(exc)


def test_should_be_able_to_restore_strings(keyspace):
    keyspace.set('test1', 'testvalue')
    payload = keyspace.dump('test1')

    keyspace.restore('test2', 0, payload, replace=False)

    assert keyspace.get('test2') == 'testvalue'


def test_should_be_able_to_restore_sets(keyspace):
    keyspace.sadd('set1', 'member1')
    keyspace.sadd('set1', 'member2')
    keyspace.sadd('set1', 'member3')
    payload = keyspace.dump('set1')

    keyspace.restore('set2', 0, payload, replace=False)

    assert keyspace.smembers('set2') == {'member1', 'member2', 'member3'}


def test_should_be_able_to_restore_sorted_sets(keyspace):
    flat_pairs = [
        'member3', 0,
        'member4', 1.5,
        'member5', 2 ** 64,
        'member2', float('+inf'),
        'member1', float('-inf'),
    ]
    for i in range(0, len(flat_pairs), 2):
        keyspace.zadd('zset1', value=flat_pairs[i], score=flat_pairs[i + 1])
    payload = keyspace.dump('zset1')

    keyspace.restore('zset2', 0, payload, replace=False)

    # using a `dict` to avoid ordered comparision of `flat_pairs`
    flat_pairs_dict = {}
    for i in range(0, len(flat_pairs), 2):
        # keyspace.zrange() returns scores as strings
        flat_pairs_dict[flat_pairs[i]] = to_float_string(flat_pairs[i + 1])
    zrange = keyspace.zrange('zset2', 0, -1, with_scores=True)
    assert dict(zip(zrange[::2], zrange[1::2])) == flat_pairs_dict


def test_should_be_able_to_restore_hashes(keyspace):
    keyspace.hset('hash1', 'field1', 'value1')
    keyspace.hset('hash1', 'field2', 'value2')
    keyspace.hset('hash1', 'field3', 'value3')
    payload = keyspace.dump('hash1')

    keyspace.restore('hash2', 0, payload, replace=False)

    assert keyspace.hgetall('hash2') == ['field1', 'value1', 'field2', 'value2', 'field3', 'value3']


def test_restore_should_remove_key_before_adding_new_values(keyspace):
    keyspace.hset('hash', 'field1', 'value1')
    keyspace.hset('hash', 'field2', 'value2')
    payload = keyspace.dump('hash')

    # added after the dump
    keyspace.hset('hash', 'field3', 'value3')

    keyspace.restore('hash', 0, payload, replace=True)
    assert keyspace.hgetall('hash') == ['field1', 'value1', 'field2', 'value2']


def test_encval_strings(keyspace):
    """
    int rdbEncodeInteger(long long value, unsigned char *enc) {
        if (value >= -(1<<7) && value <= (1<<7)-1) {
            enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT8;
            enc[1] = value&0xFF;
            return 2;
        } else if (value >= -(1<<15) && value <= (1<<15)-1) {
            enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT16;
            enc[1] = value&0xFF;
            enc[2] = (value>>8)&0xFF;
            return 3;
        } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
            enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT32;
            enc[1] = value&0xFF;
            enc[2] = (value>>8)&0xFF;
            enc[3] = (value>>16)&0xFF;
            enc[4] = (value>>24)&0xFF;
            return 5;
        } else {
            return 0;
        }
    }
    """

    # string "64" encoded as a signed 8 bit integer
    int8 = 64
    enc_8bit = struct.pack('<Bb', (rdb.RDB_ENCVAL << 6) | rdb.RDB_ENC_INT8, int8)
    object_loader = ObjectLoader(keyspace, BytesIO(enc_8bit))
    object_loader.load_string('int8')
    assert keyspace.get('int8') == str(int8)

    # string "250" encoded as a signed 16 bit integer
    int16 = 250
    enc_16bit = struct.pack('<Bh', (rdb.RDB_ENCVAL << 6) | rdb.RDB_ENC_INT16, int16)
    object_loader = ObjectLoader(keyspace, BytesIO(enc_16bit))
    object_loader.load_string('int16')
    assert keyspace.get('int16') == str(int16)

    # string "65000" encoded as signed 32 bit integer
    int32 = 65000
    enc_32bit = struct.pack('<Bi', (rdb.RDB_ENCVAL << 6) | rdb.RDB_ENC_INT32, int32)
    object_loader = ObjectLoader(keyspace, BytesIO(enc_32bit))
    object_loader.load_string('int32')
    assert keyspace.get('int32') == str(int32)


def test_zset_as_ziplist(keyspace):
    payload = (
        "\x0c"
        "\x1a\x1a\x00\x00"  # zlbytes
        "\x00\x16\x00\x00"  # zltail
        "\x00\x02"  # zllen
        "\x00\x00"  # previous length

        "\n"  # header and length
        "test value"  # value = "test value"

        "\x0c"  # previous length
        "\xfe"  # header
        "{"  # score = 123

        "\xff"  # end of ziplist
        "\a\x00\xeb\x1b\x1bim\xc5\n\x93"  # checksum
    )
    keyspace.restore('zset', ttl=0, payload=payload, replace=False)

    assert keyspace.zrange('zset', 0, -1, with_scores=True) == [
        "test value",
        "123",
    ]


def test_hash_as_ziplist(keyspace):
    payload = (
        "\r"
        "\x1b\x1b\x00\x00"  # zlbytes
        "\x00\x12\x00\x00"  # zltail
        "\x00\x02"  # zzlen
        "\x00\x00"  # previous length

        "\x06"  # header and length
        "field1"  # field name

        "\b"  # previous length
        "\x06"  # header and length
        "value1"  # field value

        "\xff"  # end of ziplist
        "\a\x00\xbd5\x1dA\xc2a\xf1!"  # checksum
    )
    keyspace.restore('testhash', ttl=0, payload=payload, replace=False)

    assert keyspace.hgetall('testhash') == ["field1", "value1"]
