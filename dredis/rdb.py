"""
This module is based on the following Redis files:

* https://github.com/antirez/redis/blob/3.2.6/src/rdb.h
* https://github.com/antirez/redis/blob/3.2.6/src/rdb.c
* https://github.com/antirez/redis/blob/3.2.6/src/cluster.c

"""
import logging
import os
import struct

import lzf

import dredis
from dredis import crc64


logger = logging.getLogger(__name__)

RDB_TYPE_STRING = 0
RDB_TYPE_SET = 2
RDB_TYPE_ZSET = 3
RDB_TYPE_HASH = 4

RDB_TYPE_ZSET_ZIPLIST = 12
# the following types are not supported yet:
# RDB_TYPE_HASH_ZIPMAP = 9
# RDB_TYPE_LIST_ZIPLIST = 10
# RDB_TYPE_SET_INTSET = 11
# RDB_TYPE_HASH_ZIPLIST = 13
# RDB_TYPE_LIST_QUICKLIST = 14

ZIP_END = 255
ZIP_BIGLEN = 254

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
RDB_ENC_LZF = 3

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
        """
        inspired heavily by rdb.c:rdbLoad()
        """
        if not self.payload.startswith("REDIS"):
            raise ValueError("Wrong signature trying to load DB from file")
        version = self.payload[len("REDIS"):self.REDIS_VERSION_HEADER_LENGTH]
        if not version.isdigit() or int(version) > RDB_VERSION:
            raise ValueError("Can't handle RDB format version %s" % version)

        self.index = self.REDIS_VERSION_HEADER_LENGTH
        while True:
            obj_type = self.load_type()
            if obj_type == RDB_OPCODE_EXPIRETIME:
                self.index += 4  # rdbLoadTime() reads 4 bytes
                obj_type = self.load_type()
                logger.warning("Key expiration isn't supported, skipping expiration (RDB_OPCODE_EXPIRETIME)")
            elif obj_type == RDB_OPCODE_EXPIRETIME_MS:
                self.index += 8  # rdbLoadMillisecondTime() reads 8 bytes
                obj_type = self.load_type()
                logger.warning("Key expiration isn't supported, skipping expiration (RDB_OPCODE_EXPIRETIME_MS)")
            elif obj_type == RDB_OPCODE_EOF:
                break
            elif obj_type == RDB_OPCODE_SELECTDB:
                # FIXME: at the moment only add keys to the default db
                self.load_len()
                continue
            elif obj_type == RDB_OPCODE_AUX:
                # ignoring aux field=value
                self._load_string()
                self._load_string()
                continue
            elif obj_type == RDB_OPCODE_RESIZEDB:
                # ignoring db_size & expires_size
                self.load_len()
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
        elif obj_type == RDB_TYPE_ZSET_ZIPLIST:
            self.load_zset_ziplist(key)
        else:
            logger.error("Can't load %r (obj_type=%r)" % (key, obj_type))
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

    def load_zset_ziplist(self, key):
        """
        From ziplist.c:
            * ZIPLIST OVERALL LAYOUT:
            * The general layout of the ziplist is as follows:
            * <zlbytes><zltail><zllen><entry><entry><zlend>
        """
        ziplist = self._load_string()
        zindex = 0
        zlbytes = struct.unpack('I', ziplist[zindex:zindex+4])[0]  # noqa
        zindex += 4
        zltail = struct.unpack('I', ziplist[zindex:zindex+4])[0]  # noqa
        zindex += 4
        zllen = struct.unpack('H', ziplist[zindex:zindex + 2])[0]
        zindex += 2

        for _ in xrange(zllen // 2):
            member, zindex = self._read_ziplist_entry(ziplist, zindex)
            score, zindex = self._read_ziplist_entry(ziplist, zindex)
            self.keyspace.zadd(key, score, member)

        zlend = struct.unpack('B', ziplist[zindex])[0]
        if zlend != ZIP_END:
            raise ValueError("Invalid ziplist end %r (key = %r)" % (zlend, key))

    def _read_ziplist_entry(self, f, zindex):
        z = [zindex]

        def read_unsigned_char(f):
            result = struct.unpack('B', f[z[0]])[0]
            z[0] += 1
            return result

        def read_signed_char(f):
            result = struct.unpack('b', f[z[0]])[0]
            z[0] += 1
            return result

        def read_unsigned_int(f):
            result = struct.unpack('I', f[z[0]:z[0]+4])[0]
            z[0] += 4
            return result

        def read_unsigned_int_be(f):
            result = struct.unpack('>I', f[z[0]:z[0]+4])[0]
            z[0] += 4
            return result

        def read_signed_int(f):
            result = struct.unpack('i', f[z[0]:z[0]+4])[0]
            z[0] += 4
            return result

        def read_signed_short(f):
            result = struct.unpack('h', f[z[0]:z[0]+2])[0]
            z[0] += 2
            return result

        def read_signed_long(f):
            result = struct.unpack('l', f[z[0]:z[0]+8])[0]
            z[0] += 8
            return result

        def read_24bit_signed_number(f):
            s = b'0' + f[z[0]:z[0]+3]
            z[0] += 3
            num = struct.unpack('i', s)[0]
            return num >> 8

        length = 0
        value = None
        prev_length = read_unsigned_char(f)
        if prev_length == ZIP_BIGLEN:
            prev_length = read_unsigned_int(f)
        entry_header = read_unsigned_char(f)
        if (entry_header >> 6) == 0:
            length = entry_header & 0x3F
            value = f[z[0]:z[0] + length]
            z[0] += length
        elif (entry_header >> 6) == 1:
            length = ((entry_header & 0x3F) << 8) | read_unsigned_char(f)
            value = f[z[0]:z[0] + length]
            z[0] += length
        elif (entry_header >> 6) == 2:
            length = read_unsigned_int_be(f)
            value = f[z[0]:z[0] + length]
            z[0] += length
        elif (entry_header >> 4) == 12:
            value = read_signed_short(f)
        elif (entry_header >> 4) == 13:
            value = read_signed_int(f)
        elif (entry_header >> 4) == 14:
            value = read_signed_long(f)
        elif (entry_header == 240):
            value = read_24bit_signed_number(f)
        elif (entry_header == 254):
            value = read_signed_char(f)
        elif (entry_header >= 241 and entry_header <= 253):
            value = entry_header - 241
        else:
            raise Exception('read_ziplist_entry', 'Invalid entry_header %d for key %s' % (entry_header, self._key))
        return value, z[0]

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

        len_type = (self._get_unsigned_byte(0) & 0xC0) >> 6
        if len_type == RDB_6BITLEN:
            length = self._get_unsigned_byte(0) & 0x3F
            self.index += 1
        elif len_type == RDB_14BITLEN:
            length = ((self._get_unsigned_byte(0) & 0x3F) << 8) | self._get_unsigned_byte(1)
            self.index += 2
        elif len_type == RDB_32BITLEN:
            self.index += 1
            length = self._get_unsigned_int_be()
            self.index += 4
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

    def _get_signed_byte(self):
        return struct.unpack('b', self.payload[self.index])[0]

    def _get_signed_int(self):
        return struct.unpack('i', self.payload[self.index:self.index + 4])[0]

    def _get_signed_short(self):
        return struct.unpack('h', self.payload[self.index:self.index + 2])[0]

    def _get_unsigned_byte(self, i):
        return struct.unpack('B', self.payload[self.index + i])[0]

    def _get_unsigned_int_be(self):
        return struct.unpack('>I', self.payload[self.index:self.index + 4])[0]

    def _load_string_len(self):
        len_type = (self._get_unsigned_byte(0) & 0xC0) >> 6
        if len_type == RDB_ENCVAL:
            enctype = self._get_unsigned_byte(0) & 0x3F
            self.index += 1
            return enctype, True
        else:
            return self.load_len(), False

    def _load_encoded_string(self, enctype):
        if enctype == RDB_ENC_INT8:
            length = self._get_signed_byte()
            self.index += 1
        elif enctype == RDB_ENC_INT16:
            length = self._get_signed_short()
            self.index += 2
        elif enctype == RDB_ENC_INT32:
            length = self._get_signed_int()
            self.index += 4
        elif enctype == RDB_ENC_LZF:
            compressed_len = self.load_len()
            out_max_len = self.load_len()
            data = self.payload[self.index:self.index + compressed_len]
            self.index += compressed_len
            decompressed_data = lzf.decompress(data, out_max_len)
            length = decompressed_data
        else:
            raise ValueError("Unknown RDB string encoding type %d" % enctype)
        return bytes(length)


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
