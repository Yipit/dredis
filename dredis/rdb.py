"""
This module is based on the following Redis files:

* https://github.com/antirez/redis/blob/3.2.6/src/rdb.h
* https://github.com/antirez/redis/blob/3.2.6/src/rdb.c
* https://github.com/antirez/redis/blob/3.2.6/src/cluster.c

"""

import struct

RDB_TYPE_STRING = 0
RDB_TYPE_HASH = 4

RDB_TYPES = {
    'string': RDB_TYPE_STRING,
    'hash': RDB_TYPE_HASH,
}

RDB_6BITLEN = 0
RDB_14BITLEN = 1
RDB_32BITLEN = 2

RDB_VERSION = 7


def object_type(type_name):
    """
    :return little endian encoded type
    """
    return struct.pack('<B', RDB_TYPES[type_name])


def object_value(keyspace, key, key_type):
    if key_type == 'string':
        string = keyspace.get(key)
        return save_raw_string(string)
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
    raise ValueError("Can't convert %r" % key_type)


def save_raw_string(string):
    return save_len(len(string)) + string


def save_len(len):
    """
    :return big endian encoded length

    Original: https://github.com/antirez/redis/blob/3.2.6/src/rdb.c

    """
    if len < (1 << 6):
        return struct.pack('>B', (len & 0xFF) | (RDB_6BITLEN << 6))
    elif len < (1 << 14):
        return struct.pack('>BB', ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6), len & 0xFF)
    else:
        return struct.pack('>BL', (RDB_32BITLEN << 6), len)


def get_rdb_version():
    """
    :return little endian encoded 2-byte RDB version
    """
    return struct.pack('<BB', RDB_VERSION & 0xff, (RDB_VERSION >> 8) & 0xff)
