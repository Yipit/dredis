import struct

import plyvel

LDB_DBS = {}
LDB_STRING_TYPE = 1
LDB_SET_TYPE = 2
LDB_SET_MEMBER_TYPE = 3
LDB_HASH_TYPE = 4
LDB_HASH_FIELD_TYPE = 5
LDB_ZSET_TYPE = 6
LDB_ZSET_VALUE_TYPE = 7
LDB_ZSET_SCORE_TYPE = 8
LDB_KEY_TYPES = [LDB_STRING_TYPE, LDB_SET_TYPE, LDB_HASH_TYPE, LDB_ZSET_TYPE]

# type_id | key_length
LDB_KEY_PREFIX_FORMAT = '>BI'
LDB_KEY_PREFIX_LENGTH = struct.calcsize(LDB_KEY_PREFIX_FORMAT)
LDB_ZSET_SCORE_FORMAT = '>d'

# ldb sorts elements lexicographically and negative numbers
# when converted to binary are "bigger" than positives.
# zero is the lowest byte combination.
LDB_MIN_ZSET_SCORE = 0


class LDBKeyCodec(object):

    def get_key(self, key, type_id):
        prefix = struct.pack(LDB_KEY_PREFIX_FORMAT, type_id, len(key))
        return prefix + bytes(key)

    def encode_string(self, key):
        return self.get_key(key, LDB_STRING_TYPE)

    def encode_set(self, key):
        return self.get_key(key, LDB_SET_TYPE)

    def encode_set_member(self, key, value):
        return self.get_key(key, LDB_SET_MEMBER_TYPE) + bytes(value)

    def encode_hash(self, key):
        return self.get_key(key, LDB_HASH_TYPE)

    def encode_hash_field(self, key, field):
        return self.get_key(key, LDB_HASH_FIELD_TYPE) + bytes(field)

    def encode_zset(self, key):
        return self.get_key(key, LDB_ZSET_TYPE)

    def encode_zset_value(self, key, value):
        return self.get_key(key, LDB_ZSET_VALUE_TYPE) + bytes(value)

    def encode_zset_score(self, key, value, score):
        return self.get_key(key, LDB_ZSET_SCORE_TYPE) + struct.pack(LDB_ZSET_SCORE_FORMAT, float(score)) + bytes(value)

    def decode_key(self, key):
        type_id, key_length = struct.unpack(LDB_KEY_PREFIX_FORMAT, key[:LDB_KEY_PREFIX_LENGTH])
        key_value = key[LDB_KEY_PREFIX_LENGTH:]
        return type_id, key_length, key_value

    def decode_zset_score(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        return struct.unpack(LDB_ZSET_SCORE_FORMAT, key_name[length:length + struct.calcsize(LDB_ZSET_SCORE_FORMAT)])[0]

    def decode_zset_value(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        return key_name[length + struct.calcsize(LDB_ZSET_SCORE_FORMAT):]

    def get_min_zset_score(self, key):
        return self.encode_zset_score(key, bytes(''), LDB_MIN_ZSET_SCORE)


KEY_CODEC = LDBKeyCodec()


def open_ldb(path):
    return plyvel.DB(bytes(path), create_if_missing=True)
