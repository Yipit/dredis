import struct

import plyvel

from dredis.path import Path

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


class LDBKeyCodec(object):

    # the key format using <key length + key> was inspired by the `blackwidow` project:
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/zsets_data_key_format.h#L44-L53
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/base_data_key_format.h#L37-L43
    #
    # LevelDB doesn't have column families like RocksDB, so the binary prefixes were created to distinguish object types

    def get_key(self, key, type_id):
        prefix = struct.pack(LDB_KEY_PREFIX_FORMAT, type_id, len(key))
        return prefix + bytes(key)

    def encode_string(self, key):
        return self.get_key(key, LDB_STRING_TYPE)

    def encode_set(self, key):
        return self.get_key(key, LDB_SET_TYPE)

    def encode_set_member(self, key, value):
        return self.get_key(key, LDB_SET_MEMBER_TYPE) + bytes(value)

    def get_min_set_member(self, key):
        return self.get_key(key, LDB_SET_MEMBER_TYPE)

    def encode_hash(self, key):
        return self.get_key(key, LDB_HASH_TYPE)

    def encode_hash_field(self, key, field):
        return self.get_key(key, LDB_HASH_FIELD_TYPE) + bytes(field)

    def get_min_hash_field(self, key):
        return self.get_key(key, LDB_HASH_FIELD_TYPE)

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
        return self.get_key(key, LDB_ZSET_SCORE_TYPE)

    def get_min_zset_value(self, key):
        return self.get_key(key, LDB_ZSET_VALUE_TYPE)


class LevelDB(object):

    def setup_dbs(self, root_dir):
        for db_id_ in range(16):
            db_id = str(db_id_)
            directory = Path(root_dir).join(db_id)
            self._assign_db(db_id, directory)

    def open_db(self, path):
        return plyvel.DB(bytes(path), create_if_missing=True)

    def get_db(self, db_id):
        return LDB_DBS[str(db_id)]['db']

    def delete_dbs(self):
        for db_id in LDB_DBS:
            self.delete_db(db_id)

    def delete_db(self, db_id):
        db_id = str(db_id)
        LDB_DBS[db_id]['db'].close()
        LDB_DBS[db_id]['directory'].reset()
        self._assign_db(db_id, LDB_DBS[db_id]['directory'])

    def _assign_db(self, db_id, directory):
        LDB_DBS[db_id] = {
            'db': self.open_db(directory),
            'directory': directory,
        }


KEY_CODEC = LDBKeyCodec()
LEVELDB = LevelDB()
