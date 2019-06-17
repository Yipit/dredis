import struct

from dredis import rdb
from dredis.rdb import ObjectDumper


def test_dump_not_found_key(keyspace):
    assert keyspace.dump('notfound') is None


def test_dump_string(keyspace):
    # dredis serializes strings verbatim,
    # it doesn't encode int strings differently than raw strings,
    # and doesn't use LZF compresssion

    str1 = 'test'
    str2 = 'a' * (1 << 6)
    str3 = 'a' * (1 << 14)

    keyspace.set('str1', str1)
    keyspace.set('str2', str2)
    keyspace.set('str3', str3)

    assert keyspace.dump('str1') == b'\x00\x04' + str1 + b'\x07\x00~\xa2zSd;e_'
    assert keyspace.dump('str2') == b'\x00@@' + str2 + b'\x07\x00\xd2>\xaf>\x83X\xde\xe5'
    assert keyspace.dump('str3') == b'\x00\x80\x00\x00@\x00' + str3 + b'\x07\x00\xe9\x9e\x16)r\x8c\xac\x87'


def test_dump_hash(keyspace):
    keyspace.hset('hash', 'field1', 'value1')
    keyspace.hset('hash', 'field2', 'value2')

    # there's no ziplist encoding in dredis, only hash table encoding (OBJ_ENCODING_HT)
    assert keyspace.dump('hash') == '\x04\x02\x06field1\x06value1\x06field2\x06value2\x07\x00\xbf\xe3\xa8l\x05l\xbd\xf1'


def test_dump_sorted_set(keyspace):
    keyspace.zadd('zset', float('-inf'), 'value1')
    keyspace.zadd('zset', -1, 'value2')
    keyspace.zadd('zset', 0, 'value3')
    keyspace.zadd('zset', 1, 'value4')
    keyspace.zadd('zset', float('+inf'), 'value5')
    dump = keyspace.dump('zset')

    # header
    assert dump[:2] == b"\x03\x05"

    # the order of the zset isn't deterministic, thus the list checks
    values = [
        b"\x06value1\xff",
        b"\x06value2\x02-1",
        b"\x06value3\x010",
        b"\x06value4\x011",
        b"\x06value5\xfe",
    ]
    # everything but checksum
    object_value = dump[2:-10]
    assert len(dump) == 2 + sum(map(len, values)) + 10

    for value in values:
        assert value in object_value


def test_dump_set(keyspace):
    keyspace.sadd('set', 'a')
    keyspace.sadd('set', 'b')

    assert keyspace.dump('set') == b'\x02\x02\x01a\x01b\x07\x00\x01V\xf4\xd6\xe3\xc9\xe8\x17'


def test_dump_empty_value(keyspace):
    keyspace.set('empty', '')

    assert keyspace.dump('empty') == b'\x00\x00\a\x00\xa2\xa9\x89\xb3\a\xb5w\xc0'


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

    object_dumper = ObjectDumper(keyspace)

    int8 = 64
    # string "64" encoded as a signed 8 bit integer
    enc_8bit = struct.pack('<Bb', (rdb.RDB_ENCVAL << 6) | rdb.RDB_ENC_INT8, int8)
    keyspace.set('int8', str(int8))
    assert object_dumper.dump_string('int8') == enc_8bit

    int16 = 250
    # string "250" encoded as a signed 16 bit integer
    enc_16bit = struct.pack('<Bh', (rdb.RDB_ENCVAL << 6) | rdb.RDB_ENC_INT16, int16)
    keyspace.set('int16', str(int16))
    assert object_dumper.dump_string('int16') == enc_16bit

    int32 = 65000
    # string "65000" encoded as signed 32 bit integer
    enc_32bit = struct.pack('<Bi', (rdb.RDB_ENCVAL << 6) | rdb.RDB_ENC_INT32, int32)
    keyspace.set('int32', str(int32))
    assert object_dumper.dump_string('int32') == enc_32bit
