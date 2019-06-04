"""
This module is based on the following Redis files:

* https://github.com/antirez/redis/blob/3.2.6/src/rdb.h
* https://github.com/antirez/redis/blob/3.2.6/src/rdb.c
* https://github.com/antirez/redis/blob/3.2.6/src/cluster.c

"""

import struct

RDB_TYPE_STRING = 0

RDB_6BITLEN = 0
RDB_14BITLEN = 1
RDB_32BITLEN = 2

RDB_VERSION = 7


def object_type(type_name):
    # FIXME: only works with strings at the moment
    return struct.pack('<b', RDB_TYPE_STRING)


def object_value(keyspace, key, key_type):
    # FIXME: only works with strings at the moment
    key_value = keyspace.get(key)
    return save_len(len(key_value)) + key_value


def save_len(len):
    """
    :return big endian encoded value

    Original: https://github.com/antirez/redis/blob/3.2.6/src/rdb.c

    """
    if len < (1 << 6):
        return struct.pack('>B', (len & 0xFF) | (RDB_6BITLEN << 6))
    elif len < (1 << 14):
        return struct.pack('>BB', ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6), len & 0xFF)
    else:
        return struct.pack('>BL', (RDB_32BITLEN << 6), len)


def get_rdb_version():
    return struct.pack('<BB', RDB_VERSION & 0xff, (RDB_VERSION >> 8) & 0xff)
