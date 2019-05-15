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
LDB_ZSET_SCORE_FORMAT_LENGTH = struct.calcsize(LDB_ZSET_SCORE_FORMAT)

LDB_KEY_PREFIX_STRUCT = struct.Struct(LDB_KEY_PREFIX_FORMAT)
LDB_ZSET_SCORE_STRUCT = struct.Struct(LDB_ZSET_SCORE_FORMAT)

class LDBKeyCodec(object):

    # the key format using <key length + key> was inspired by the `blackwidow` project:
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/zsets_data_key_format.h#L44-L53
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/base_data_key_format.h#L37-L43
    #
    # LevelDB doesn't have column families like RocksDB, so the binary prefixes were created to distinguish object types

    def get_key(self, key, type_id):
        prefix = LDB_KEY_PREFIX_STRUCT.pack(type_id, len(key))
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
        return self.get_key(key, LDB_ZSET_SCORE_TYPE) + LDB_ZSET_SCORE_STRUCT.pack(float(score)) + bytes(value)

    def decode_key(self, key):
        type_id, key_length = LDB_KEY_PREFIX_STRUCT.unpack(key[:LDB_KEY_PREFIX_LENGTH])
        key_value = key[LDB_KEY_PREFIX_LENGTH:]
        return type_id, key_length, key_value

    def decode_zset_score(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        return LDB_ZSET_SCORE_STRUCT.unpack(key_name[length:length + LDB_ZSET_SCORE_FORMAT_LENGTH])[0]

    def decode_zset_value(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        return key_name[length + LDB_ZSET_SCORE_FORMAT_LENGTH:]

    def get_min_zset_score(self, key):
        return self.get_key(key, LDB_ZSET_SCORE_TYPE)

    def get_min_zset_value(self, key):
        return self.get_key(key, LDB_ZSET_VALUE_TYPE)

import lmdb

class LMDBBatch(object):
    def __init__(self, env):
        self._env = env
        self._put = {}
        self._delete = set()

    def put(self, key, value):
        self._put[key] = value

    def delete(self, key):
        try:
            del self._put[key]
        except KeyError:
            pass
        self._delete.add(key)

    def write(self):
        with self._env.begin(write=True) as t:
            for k, v in self._put.items():
                t.put(k, v)
            for k in self._delete:
                t.delete(k)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.write()
            return True


B = 1
KB = 1024 * B
MB = KB * 1024
GB = MB * 1024

class LMDBWrapper(object):
    def __init__(self, path):
        self._env = lmdb.open(path, map_size=100*GB, map_async=True, writemap=True)

    def get(self, key, default=None):
        with self._env.begin() as t:
            return t.get(key, default)

    def put(self, key, value):
        with self._env.begin(write=True) as t:
            t.put(key, value)

    def delete(self, key):
        with self._env.begin(write=True) as t:
            t.delete(key)

    def write_batch(self):
        return LMDBBatch(self._env)

    def close(self):
        self._env.close()

    def iterator(self, prefix='', include_value=True):
        with self._env.begin() as t:
            c = t.cursor()
            if not c.set_range(prefix):
                return
            for k, v in c:
                if not k.startswith(prefix):
                    return
                if include_value:
                    yield k, v
                else:
                    yield k

    def __iter__(self):
        with self._env.begin() as t:
            c = t.cursor()
            for k, v in c:
                yield k, v



class LevelDB(object):

    def setup_dbs(self, root_dir):
        for db_id_ in range(16):
            db_id = str(db_id_)
            directory = Path(root_dir).join(db_id)
            self._assign_db(db_id, directory)

    def open_db(self, path):
        return LMDBWrapper(bytes(path))
        #return plyvel.DB(bytes(path), create_if_missing=True)

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
