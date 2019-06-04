from dredis.keyspace import Keyspace


def test_serialize_not_found_key():
    k = Keyspace()
    assert k.dump('notfound') is None


def test_serialize_string():
    # dredis serializes strings verbatim,
    # it doesn't encode int strings differently than raw strings,
    # and doesn't use LZF compresssion

    str1 = 'test'
    str2 = 'a' * (1 << 6)
    str3 = 'a' * (1 << 14)

    k = Keyspace()
    k.set('str1', str1)
    k.set('str2', str2)
    k.set('str3', str3)

    assert k.dump('str1') == b'\x00\x04' + str1 + b'\x07\x00~\xa2zSd;e_'
    assert k.dump('str2') == b'\x00@@' + str2 + b'\x07\x00\xd2>\xaf>\x83X\xde\xe5'
    assert k.dump('str3') == b'\x00\x80\x00\x00@\x00' + str3 + b'\x07\x00\xe9\x9e\x16)r\x8c\xac\x87'


def test_serialize_hash():
    k = Keyspace()
    k.hset('hash', 'field1', 'value1')
    k.hset('hash', 'field2', 'value2')

    # there's no ziplist encoding in dredis, only hash table encoding (OBJ_ENCODING_HT)
    assert k.dump('hash') == '\x04\x02\x06field1\x06value1\x06field2\x06value2\x07\x00\xbf\xe3\xa8l\x05l\xbd\xf1'


def test_serialize_sorted_set():
    k = Keyspace()
    k.zadd('zset', float('-inf'), 'value1')
    k.zadd('zset', -1, 'value2')
    k.zadd('zset', 0, 'value3')
    k.zadd('zset', 1, 'value4')
    k.zadd('zset', float('+inf'), 'value5')
    dump = k.dump('zset')

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
