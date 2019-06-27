"""
This module is based on the following Redis files:

* https://github.com/antirez/redis/blob/3.2.6/src/rdb.h
* https://github.com/antirez/redis/blob/3.2.6/src/rdb.c
* https://github.com/antirez/redis/blob/3.2.6/src/cluster.c

"""
import logging
import os
import struct
from io import BytesIO

import lzf

import dredis
from dredis import crc64
from dredis.exceptions import DredisError

logger = logging.getLogger(__name__)

RDB_TYPE_STRING = 0
RDB_TYPE_SET = 2
RDB_TYPE_ZSET = 3
RDB_TYPE_HASH = 4

RDB_TYPE_SET_INTSET = 11
RDB_TYPE_ZSET_ZIPLIST = 12
RDB_TYPE_HASH_ZIPLIST = 13
# the following types are not supported yet:
# RDB_TYPE_HASH_ZIPMAP = 9
# RDB_TYPE_LIST_ZIPLIST = 10
# RDB_TYPE_LIST_QUICKLIST = 14

INTSET_ENC_INT16 = 2
INTSET_ENC_INT32 = 4
INTSET_ENC_INT64 = 8

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

BAD_DATA_FORMAT_ERR = DredisError("Bad data format")


def get_rdb_version():
    """
    :return: little endian encoded 2-byte RDB version
    """
    return struct.pack('<BB', RDB_VERSION & 0xff, (RDB_VERSION >> 8) & 0xff)


def load_object(keyspace, key, rdb_file):
    object_loader = ObjectLoader(keyspace, rdb_file)
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
    bad_payload = DredisError('DUMP payload version or checksum are wrong')
    if len(payload) < 10:
        raise bad_payload
    data, footer = payload[:-10], payload[-10:]
    rdb_version, crc = footer[:2], footer[2:]
    if rdb_version > get_rdb_version():
        raise bad_payload
    if crc64.checksum(data + rdb_version) != crc:
        raise bad_payload


def load_rdb(keyspace, rdb_file):
    object_loader = ObjectLoader(keyspace, rdb_file)
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
    REDIS_VERSION_LENGTH = 4
    REDIS_VERSION_HEADER_LENGTH = 9  # "REDIS0007" for example

    def __init__(self, keyspace, rdb_file):
        self.keyspace = keyspace
        self.file = rdb_file

    def load_rdb(self):
        """
        inspired heavily by rdb.c:rdbLoad()
        """
        header = self.file.read(self.REDIS_VERSION_HEADER_LENGTH)
        if not header.startswith("REDIS"):
            raise DredisError("Wrong signature trying to load DB from file")
        version = header[-self.REDIS_VERSION_LENGTH:]
        if not version.isdigit() or int(version) > RDB_VERSION:
            raise DredisError("Can't handle RDB format version %s" % version)

        while True:
            obj_type = self.load_type()
            if obj_type == RDB_OPCODE_EXPIRETIME:
                self.file.read(4)  # rdbLoadTime() reads 4 bytes
                obj_type = self.load_type()
                logger.warning("Key expiration isn't supported, skipping expiration (RDB_OPCODE_EXPIRETIME)")
            elif obj_type == RDB_OPCODE_EXPIRETIME_MS:
                self.file.read(8)  # rdbLoadMillisecondTime() reads 8 bytes
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
        obj_type = self.load_type()
        self._load(key, obj_type)

    def _load(self, key, obj_type):
        if obj_type == RDB_TYPE_STRING:
            self.load_string(key)
        elif obj_type == RDB_TYPE_SET:
            self.load_set(key)
        elif obj_type == RDB_TYPE_SET_INTSET:
            self.load_intset(key)
        elif obj_type == RDB_TYPE_ZSET:
            self.load_zset(key)
        elif obj_type == RDB_TYPE_HASH:
            self.load_hash(key)
        elif obj_type == RDB_TYPE_ZSET_ZIPLIST:
            self.load_zset_ziplist(key)
        elif obj_type == RDB_TYPE_HASH_ZIPLIST:
            self.load_hash_ziplist(key)
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

    def load_intset(self, key):
        """
        based on intset.h and https://github.com/sripathikrishnan/redis-rdb-tools/blob/543a73e84702e911ddcd31325ecfde77d7fd230b/rdbtools/parser.py#L665-L681
        """  # noqa
        intset = BytesIO(self._load_string())
        encoding = read_unsigned_int(intset)
        length = read_unsigned_int(intset)
        for _ in xrange(length):
            if encoding == INTSET_ENC_INT16:
                entry = read_signed_short(intset)
            elif encoding == INTSET_ENC_INT32:
                entry = read_signed_int(intset)
            elif encoding == INTSET_ENC_INT64:
                entry = read_signed_long(intset)
            else:
                raise DredisError('Invalid encoding %r for intset (key = %r)' % (encoding, key))
            self.keyspace.sadd(key, entry)

    def load_zset(self, key):
        length = self.load_len()
        for _ in xrange(length):
            value = self._load_string()
            score = self.load_double()
            self.keyspace.zadd(key, score, value)

    def _load_ziplist(self, key):
        """
        From ziplist.c:
            * ZIPLIST OVERALL LAYOUT:
            * The general layout of the ziplist is as follows:
            * <zlbytes><zltail><zllen><entry><entry><zlend>
        """
        ziplist = BytesIO(self._load_string())
        zlbytes = read_unsigned_int(ziplist)  # noqa
        zltail = read_unsigned_int(ziplist)  # noqa
        zllen = read_unsigned_short(ziplist)

        for _ in xrange(zllen):
            yield bytes(self._read_ziplist_entry(ziplist, key))

        zlend = read_unsigned_char(ziplist)
        if zlend != ZIP_END:
            raise DredisError("Invalid ziplist end %r (key = %r)" % (zlend, key))

    def load_zset_ziplist(self, key):
        ziplist = self._load_ziplist(key)
        while True:
            try:
                member = next(ziplist)
                score = next(ziplist)
            except StopIteration:
                break
            else:
                self.keyspace.zadd(key, score, member)

    def load_hash_ziplist(self, key):
        ziplist = self._load_ziplist(key)
        while True:
            try:
                field = next(ziplist)
                value = next(ziplist)
            except StopIteration:
                break
            else:
                self.keyspace.hset(key, field, value)

    def _read_ziplist_entry(self, f, key):
        """
        Copied and adapted from
        https://github.com/sripathikrishnan/redis-rdb-tools/blob/543a73e84702e911ddcd31325ecfde77d7fd230b/rdbtools/parser.py#L757-L787
        """  # noqa
        length = 0
        value = None
        prev_length = read_unsigned_char(f)
        if prev_length == ZIP_BIGLEN:
            prev_length = read_unsigned_int(f)
        entry_header = read_unsigned_char(f)
        if (entry_header >> 6) == 0:
            length = entry_header & 0x3F
            value = f.read(length)
        elif (entry_header >> 6) == 1:
            length = ((entry_header & 0x3F) << 8) | read_unsigned_char(f)
            value = f.read(length)
        elif (entry_header >> 6) == 2:
            length = read_unsigned_int_be(f)
            value = f.read(length)
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
            raise DredisError('Invalid ziplist entry_header %d for key %s' % (entry_header, key))
        return value

    def load_hash(self, key):
        length = self.load_len()
        for _ in xrange(length):
            field = self._load_string()
            value = self._load_string()
            self.keyspace.hset(key, field, value)

    def load_double(self):
        length = read_unsigned_char(self.file)
        if length == 255:
            result = float('-inf')
        elif length == 254:
            result = float('+inf')
        elif length == 253:
            result = float('nan')
        else:
            result = float(self.file.read(length))
        return result

    def load_len(self):
        """
        :return: (int, str). the length of the string and the data after the string

        Based on rdbLoadLen() in rdb.c
        """
        buff = [read_unsigned_char(self.file)]
        len_type = (buff[0] & 0xC0) >> 6
        if len_type == RDB_6BITLEN:
            length = buff[0] & 0x3F
        elif len_type == RDB_14BITLEN:
            buff.append(read_unsigned_char(self.file))
            length = ((buff[0] & 0x3F) << 8) | buff[1]
        elif len_type == RDB_32BITLEN:
            length = read_unsigned_int_be(self.file)
        else:
            raise BAD_DATA_FORMAT_ERR
        return length

    def load_type(self):
        result = read_unsigned_char(self.file)
        return result

    def _load_string(self):
        length, is_encoded = self._load_string_len()
        if is_encoded:
            obj = self._load_encoded_string(length)
        else:
            obj = self.file.read(length)
        return obj

    def _load_string_len(self):
        first_byte = read_unsigned_char(self.file)
        len_type = (first_byte & 0xC0) >> 6
        if len_type == RDB_ENCVAL:
            enctype = first_byte & 0x3F
            return enctype, True
        else:
            self.file.seek(-1, 1)  # go back one byte
            return self.load_len(), False

    def _load_encoded_string(self, enctype):
        if enctype == RDB_ENC_INT8:
            result = read_signed_char(self.file)
        elif enctype == RDB_ENC_INT16:
            result = read_signed_short(self.file)
        elif enctype == RDB_ENC_INT32:
            result = read_signed_int(self.file)
        elif enctype == RDB_ENC_LZF:
            compressed_len = self.load_len()
            out_max_len = self.load_len()
            data = self.file.read(compressed_len)
            result = lzf.decompress(data, out_max_len)
        else:
            raise DredisError("Unknown RDB string encoding type %d" % enctype)
        return bytes(result)


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
        raise DredisError("Can't convert %r" % key_type)

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
            raise DredisError("can't encode %r as integer" % value)


def read_unsigned_char(f):
    result = struct.unpack('B', f.read(1))[0]
    return result


def read_signed_char(f):
    result = struct.unpack('b', f.read(1))[0]
    return result


def read_unsigned_int(f):
    result = struct.unpack('I', f.read(4))[0]
    return result


def read_unsigned_int_be(f):
    result = struct.unpack('>I', f.read(4))[0]
    return result


def read_signed_int(f):
    result = struct.unpack('i', f.read(4))[0]
    return result


def read_signed_short(f):
    result = struct.unpack('h', f.read(2))[0]
    return result


def read_unsigned_short(f):
    return struct.unpack('H', f.read(2))[0]


def read_signed_long(f):
    result = struct.unpack('l', f.read(8))[0]
    return result


def read_24bit_signed_number(f):
    s = b'0' + f.read(3)
    num = struct.unpack('i', s)[0]
    return num >> 8
