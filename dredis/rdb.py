"""
This module is based on the following Redis files:

* https://github.com/antirez/redis/blob/3.2.6/src/rdb.h
* https://github.com/antirez/redis/blob/3.2.6/src/rdb.c
* https://github.com/antirez/redis/blob/3.2.6/src/cluster.c

"""
import os
import struct

import dredis
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

RDB_ENCVAL = 3
RDB_ENC_INT8 = 0
RDB_ENC_INT16 = 1
RDB_ENC_INT32 = 2

RDB_OPCODE_AUX = 250
RDB_OPCODE_RESIZEDB = 251
RDB_OPCODE_EXPIRETIME_MS = 252
RDB_OPCODE_EXPIRETIME = 253
RDB_OPCODE_SELECTDB = 254
RDB_OPCODE_EOF = 255

RDB_VERSION = 7

BAD_DATA_FORMAT_ERR = ValueError("Bad data format")


def get_rdb_version():
    """
    :return: little endian encoded 2-byte RDB version
    """
    return struct.pack('<BB', RDB_VERSION & 0xff, (RDB_VERSION >> 8) & 0xff)


def load_object(keyspace, key, payload):
    object_loader = ObjectLoader(keyspace, payload)
    object_loader.load(key)


def generate_payload(keyspace, key):
    key_type = keyspace.type(key)
    if key_type == 'none':
        return None
    else:
        object_dumper = ObjectDumper(keyspace)
        payload = (
                object_dumper.dump_type(key_type) +
                object_dumper.dump(key, key_type) +
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


def load_rdb(keyspace, rdb_content):
    object_loader = ObjectLoader(keyspace, rdb_content)
    object_loader.load_rdb()


def dump_rdb(keyspace, filename):
    object_dumper = ObjectDumper(keyspace)
    rdb_content = object_dumper.dump_rdb()
    tmp_filename = 'temp-dump-pid-%d.rdb' % os.getpid()
    with open(tmp_filename, 'wb') as f:
        f.write(rdb_content)
    os.rename(tmp_filename, filename)


# NOTE: The classes ObjectLoader and ObjectDumper are symmetrical.
# any changes to one of their public methods should affect the other class's equivalent method.
# All of the `ObjectLoader.load*` and `ObjectDumper.dump*` methods are coupled!


class ObjectLoader(object):

    RDB_HEADER_LENGTH = 2
    CRC64_CHECKSUM_LENGTH = 8
    OBJECT_FOOTER_LENGTH = RDB_HEADER_LENGTH + CRC64_CHECKSUM_LENGTH
    REDIS_VERSION_HEADER_LENGTH = 9  # "REDIS0007" for example

    def __init__(self, keyspace, payload):
        self.keyspace = keyspace
        self.payload = payload
        self.index = 0

    def load_rdb(self):
        # if redis_version[:5] != "REDIS"
        # check version
        db = 0
        self.index = self.REDIS_VERSION_HEADER_LENGTH
        while True:
            obj_type = self.load_type()
            if obj_type == RDB_OPCODE_AUX:
                # ignoring aux field=value
                self._load_string()
                self._load_string()
                continue
            elif obj_type == RDB_OPCODE_RESIZEDB:
                # ignoring db_size & expires_size
                self.load_len()
                self.load_len()
                continue
            elif obj_type in (
                RDB_OPCODE_EXPIRETIME,
                RDB_OPCODE_EXPIRETIME_MS,
            ):
                raise Exception("bad")
            elif obj_type == RDB_OPCODE_EOF:
                break
            elif obj_type == RDB_OPCODE_SELECTDB:
                # FIXME: at the moment only add keys to the default db
                self.load_len()
                continue
            key = self._load_string()
            self._load(key, obj_type)

    def load(self, key):
        if len(self.payload) < self.OBJECT_FOOTER_LENGTH:
            raise BAD_DATA_FORMAT_ERR
        obj_type = self.load_type()
        self._load(key, obj_type)

    def _load(self, key, obj_type):
        if obj_type == RDB_TYPE_STRING:
            self.load_string(key)
        elif obj_type == RDB_TYPE_SET:
            self.load_set(key)
        elif obj_type == RDB_TYPE_ZSET:
            self.load_zset(key)
        elif obj_type == RDB_TYPE_HASH:
            self.load_hash(key)
        else:
            raise BAD_DATA_FORMAT_ERR

    def load_string(self, key):
        obj = self._load_string()
        self.keyspace.set(key, obj)

    def load_set(self, key):
        length = self.load_len()
        for _ in xrange(length):
            elem = self._load_string()
            self.keyspace.sadd(key, elem)

    def load_zset(self, key):
        length = self.load_len()
        for _ in xrange(length):
            value = self._load_string()
            score = self.load_double()
            self.keyspace.zadd(key, score, value)

    def load_hash(self, key):
        length = self.load_len()
        for _ in xrange(length):
            field = self._load_string()
            value = self._load_string()
            self.keyspace.hset(key, field, value)

    def load_double(self):
        length = struct.unpack('>B', self.payload[self.index])[0]
        self.index += 1
        if length == 255:
            result = float('-inf')
        elif length == 254:
            result = float('+inf')
        elif length == 253:
            result = float('nan')
        else:
            result = float(self.payload[self.index:self.index + length])
            self.index += length
        return result

    def load_len(self):
        """
        :return: (int, str). the length of the string and the data after the string

        Based on rdbLoadLen() in rdb.c
        """

        len_type = (self._get_byte(0) & 0xC0) >> 6
        if len_type == RDB_6BITLEN:
            length = self._get_byte(0) & 0x3F
            self.index += 1
        elif len_type == RDB_14BITLEN:
            length = ((self._get_byte(0) & 0x3F) << 8) | self._get_byte(1)
            self.index += 2
        elif len_type == RDB_32BITLEN:
            length = self._get_long(1, 5)
            self.index += 5
        else:
            raise BAD_DATA_FORMAT_ERR
        return length

    def load_type(self):
        result = struct.unpack('<B', self.payload[self.index])[0]
        self.index += 1
        return result

    def _load_string(self):
        length, is_encoded = self._load_string_len()
        if is_encoded:
            obj = self._load_encoded_string(length)
        else:
            obj = self.payload[self.index:self.index + length]
            self.index += length
        return obj

    def _get_byte(self, i):
        return struct.unpack('>B', self.payload[self.index + i])[0]

    def _get_long(self, start, end):
        return struct.unpack('>L', self.payload[self.index + start:self.index + end])[0]

    def _load_string_len(self):
        len_type = (self._get_byte(0) & 0xC0) >> 6
        if len_type == RDB_ENCVAL:
            enctype = self._get_byte(0) & 0x3F
            self.index += 1
            return enctype, True
        else:
            return self.load_len(), False

    def _load_encoded_string(self, enctype):
        if enctype == RDB_ENC_INT8:
            length = self._get_byte(0)
            self.index += 1
        elif enctype == RDB_ENC_INT16:
            length = self._get_byte(0) | (self._get_byte(1) << 8)
            self.index += 2
        elif enctype == RDB_ENC_INT32:
            length = self._get_byte(0) | (self._get_byte(1) << 8) | (self._get_byte(2) << 16) | (self._get_byte(3) << 24)
            self.index += 4
        # TODO: no support for RDB_ENC_LZF at the moment
        else:
            raise ValueError("Unknown RDB string encoding type %d" % enctype)
        return length


class ObjectDumper(object):

    def __init__(self, keyspace):
        self.keyspace = keyspace

    def dump_rdb(self):
        aux_fields = chr(RDB_OPCODE_AUX).join([
            'REDIS%04d' % RDB_VERSION,
            '%cdredis-ver%c%s' % (len("dredis-ver"), len(dredis.__version__), dredis.__version__)
        ])
        result = bytearray()
        result.extend(aux_fields)
        # TODO: add support to multiple dbs
        db = 0
        result.append(RDB_OPCODE_SELECTDB)
        result += self.dump_length(db)
        # FIXME: `sorted` may not be worth it. keeping it to ensure consistency of RDB files
        for key in sorted(self.keyspace.keys('*')):
            obj_type = self.keyspace.type(key)
            result.extend(self.dump_type(obj_type))
            result.extend(self._dump_string(key))
            result.extend(self.dump(key, obj_type))
        result.append(RDB_OPCODE_EOF)
        result.extend(crc64.checksum(result))
        return result

    def dump(self, key, key_type):
        if key_type == 'string':
            return self.dump_string(key)
        if key_type == 'set':
            return self.dump_set(key)
        if key_type == 'hash':
            return self.dump_hash(key)
        if key_type == 'zset':
            return self.dump_zset(key)
        raise ValueError("Can't convert %r" % key_type)

    def dump_string(self, key):
        string = self.keyspace.get(key)
        return self._dump_string(string)

    def dump_set(self, key):
        members = self.keyspace.smembers(key)
        length = len(members)
        result = self.dump_length(length)
        for member in members:
            result += self._dump_string(member)
        return result

    def dump_hash(self, key):
        keys_and_values = self.keyspace.hgetall(key)
        length = len(keys_and_values) / 2
        result = self.dump_length(length)
        while keys_and_values:
            hash_key = keys_and_values.pop(0)
            hash_value = keys_and_values.pop(0)
            result += self._dump_string(hash_key)
            result += self._dump_string(hash_value)
        return result

    def dump_zset(self, key):
        values_and_scores = self.keyspace.zrange(key, 0, -1, with_scores=True)
        length = len(values_and_scores) / 2
        result = self.dump_length(length)
        while values_and_scores:
            value = values_and_scores.pop(0)
            score = values_and_scores.pop(0)
            result += self._dump_string(value)
            result += self.dump_double(score)
        return result

    def dump_length(self, len):
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

    def dump_double(self, number):
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

    def dump_type(self, key_type):
        """
        :return: little endian encoded type
        """
        return struct.pack('<B', RDB_TYPES[key_type])

    def _dump_string(self, string):
        try:
            int(string)
        except ValueError:
            return self.dump_length(len(string)) + string
        else:
            return self._dump_encoded_string(int(string))

    def _dump_encoded_string(self, value):
        if (value >= -(1 << 7)) and (value <= (1 << 7) - 1):
            return struct.pack('<Bb', (RDB_ENCVAL << 6) | RDB_ENC_INT8, value)
        elif (value >= -(1 << 15)) and (value <= (1 << 15) - 1):
            return struct.pack('<Bh', (RDB_ENCVAL << 6) | RDB_ENC_INT16, value)
        elif (value >= -(1 << 31)) and (value <= (1 << 31) - 1):
            return struct.pack('<Bi', (RDB_ENCVAL << 6) | RDB_ENC_INT32, value)
        else:
            raise ValueError("can't encode %r as integer" % value)
