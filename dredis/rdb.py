"""
This module is based on the following Redis files:

* https://github.com/antirez/redis/blob/3.2.6/src/rdb.h
* https://github.com/antirez/redis/blob/3.2.6/src/rdb.c
* https://github.com/antirez/redis/blob/3.2.6/src/cluster.c

"""

import struct

from dredis import crc64

RDB_TYPE_STRING = 0
RDB_TYPE_SET = 2
RDB_TYPE_ZSET = 3
RDB_TYPE_HASH = 4

RDB_TYPES = {
    'string': RDB_TYPE_STRING,
    'set': RDB_TYPE_SET,
    'hash': RDB_TYPE_HASH,
    'zset': RDB_TYPE_ZSET,
}

RDB_6BITLEN = 0
RDB_14BITLEN = 1
RDB_32BITLEN = 2

RDB_VERSION = 7

BAD_DATA_FORMAT_ERR = ValueError("Bad data format")


def object_type(type_name):
    """
    :return: little endian encoded type
    """
    return struct.pack('<B', RDB_TYPES[type_name])


def object_value(keyspace, key, key_type):
    if key_type == 'string':
        string = keyspace.get(key)
        return save_raw_string(string)
    elif key_type == 'set':
        members = keyspace.smembers(key)
        length = len(members)
        result = save_len(length)
        for member in members:
            result += save_raw_string(member)
        return result
    elif key_type == 'hash':
        keys_and_values = keyspace.hgetall(key)
        length = len(keys_and_values) / 2
        result = save_len(length)
        while keys_and_values:
            hash_key = keys_and_values.pop(0)
            hash_value = keys_and_values.pop(0)
            result += save_raw_string(hash_key)
            result += save_raw_string(hash_value)
        return result
    elif key_type == 'zset':
        values_and_scores = keyspace.zrange(key, 0, -1, with_scores=True)
        length = len(values_and_scores) / 2
        result = save_len(length)
        while values_and_scores:
            value = values_and_scores.pop(0)
            score = values_and_scores.pop(0)
            result += save_raw_string(value)
            result += save_double(score)
        return result
    raise ValueError("Can't convert %r" % key_type)


def save_raw_string(string):
    return save_len(len(string)) + string


def save_len(len):
    """
    :return: big endian encoded length

    Original: https://github.com/antirez/redis/blob/3.2.6/src/rdb.c

    """
    if len < (1 << 6):
        return struct.pack('>B', (len & 0xFF) | (RDB_6BITLEN << 6))
    elif len < (1 << 14):
        return struct.pack('>BB', ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6), len & 0xFF)
    else:
        return struct.pack('>BL', (RDB_32BITLEN << 6), len)


def save_double(number):
    """
    :return: big endian encoded float. 255 represents -inf, 244 +inf, 253 NaN

    This function is based on rdbSaveDoubleValue() from rdb.c
    """
    number = float(number)
    if number == float('-inf'):
        return struct.pack('>B', 255)
    elif number == float('+inf'):
        return struct.pack('>B', 254)
    elif number == float('nan'):
        return struct.pack('>B', 253)
    else:
        string = '%.17g' % float(number)
        return struct.pack('>B', len(string)) + string


def get_rdb_version():
    """
    :return: little endian encoded 2-byte RDB version
    """
    return struct.pack('<BB', RDB_VERSION & 0xff, (RDB_VERSION >> 8) & 0xff)


def load_object(keyspace, key, payload):
    object_loader = ObjectLoader(keyspace, payload)
    object_loader.load(key)


class ObjectLoader(object):

    def __init__(self, keyspace, payload):
        self.keyspace = keyspace
        self.payload = payload
        self.index = 0

    def load(self, key):
        self.index = 0
        data = self.payload[:-10]  # ignore the RDB header (2 bytes) and the CRC64 checksum (8 bytes)
        if not data:
            raise BAD_DATA_FORMAT_ERR
        obj_type = self.get_type(data)
        if obj_type == RDB_TYPE_STRING:
            self.load_string(key, data[1:])
        elif obj_type == RDB_TYPE_SET:
            self.load_set(key, data[1:])
        elif obj_type == RDB_TYPE_ZSET:
            self.load_zset(key, data[1:])
        elif obj_type == RDB_TYPE_HASH:
            self.load_hash(key, data[1:])
        else:
            raise BAD_DATA_FORMAT_ERR

    def load_string(self, key, data):
        obj = self._read_string(data)
        self.keyspace.set(key, obj)

    def load_set(self, key, data):
        length = self.load_len(data)
        for _ in xrange(length):
            elem = self._read_string(data)
            self.keyspace.sadd(key, elem)

    def load_zset(self, key, data):
        length = self.load_len(data)
        for _ in xrange(length):
            value = self._read_string(data)
            score = self.load_double(data)
            self.keyspace.zadd(key, score, value)

    def load_hash(self, key, data):
        length = self.load_len(data)
        for _ in xrange(length):
            field = self._read_string(data)
            value = self._read_string(data)
            self.keyspace.hset(key, field, value)

    def load_double(self, data):
        length = struct.unpack('>B', data[self.index])[0]
        self.index += 1
        if length == 255:
            result = float('-inf')
        elif length == 254:
            result = float('+inf')
        elif length == 253:
            result = float('nan')
        else:
            result = float(data[self.index:self.index + length])
            self.index += length
        return result

    def get_type(self, data):
        return struct.unpack('<B', data[0])[0]

    def load_len(self, data):
        """
        :param data: str
        :return: (int, str). the length of the string and the data after the string

        Based on rdbLoadLen() in rdb.c
        """

        def get_byte(i):
            return struct.unpack('>B', data[self.index + i])[0]

        def get_long(start, end):
            return struct.unpack('>L', data[self.index + start:self.index + end])[0]

        len_type = (get_byte(0) & 0xC0) >> 6
        if len_type == RDB_6BITLEN:
            length = get_byte(0) & 0x3F
            self.index += 1
        elif len_type == RDB_14BITLEN:
            length = ((get_byte(0) & 0x3F) << 8) | get_byte(1)
            self.index += 2
        elif len_type == RDB_32BITLEN:
            length = get_long(1, 5)
            self.index += 5
        else:
            raise BAD_DATA_FORMAT_ERR
        return length

    def _read_string(self, data):
        length = self.load_len(data)
        obj = data[self.index:self.index + length]
        self.index += length
        return obj


def generate_payload(keyspace, key, key_type):
    payload = (
        object_type(key_type) +
        object_value(keyspace, key, key_type) +
        get_rdb_version()
    )
    checksum = crc64.checksum(payload)
    return payload + checksum


def verify_payload(payload):
    bad_payload = ValueError('DUMP payload version or checksum are wrong')
    if len(payload) < 10:
        raise bad_payload
    data, footer = payload[:-10], payload[-10:]
    rdb_version, crc = footer[:2], footer[2:]
    if rdb_version > get_rdb_version():
        raise bad_payload
    if crc64.checksum(data + rdb_version) != crc:
        raise bad_payload
