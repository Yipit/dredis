"""
This module is based on the following Redis files:

* https://github.com/antirez/redis/blob/3.2.6/src/rdb.h
* https://github.com/antirez/redis/blob/3.2.6/src/rdb.c
* https://github.com/antirez/redis/blob/3.2.6/src/cluster.c

"""

import struct

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
