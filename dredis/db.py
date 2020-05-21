import struct
import threading
import uuid

import lmdb
import plyvel

from dredis.path import Path
from dredis.utils import FLOAT_CODEC

NUMBER_OF_REDIS_DATABASES = 16
DEFAULT_REDIS_DB = '0'
UUID_LENGTH_IN_BYTES = 16  # len(uuid.uuid4().bytes) == 16


class KeyCodec(object):

    STRING_TYPE = 1
    SET_TYPE = 2
    SET_MEMBER_TYPE = 3
    HASH_TYPE = 4
    HASH_FIELD_TYPE = 5
    ZSET_TYPE = 6
    ZSET_VALUE_TYPE = 7
    ZSET_SCORE_TYPE = 8
    DELETED_KEY_TYPE = 127
    KEY_TYPES = [STRING_TYPE, SET_TYPE, HASH_TYPE, ZSET_TYPE]

    # type_id | key_length
    KEY_PREFIX_FORMAT = '>BI'
    KEY_PREFIX_STRUCT = struct.Struct(KEY_PREFIX_FORMAT)
    KEY_PREFIX_LENGTH = KEY_PREFIX_STRUCT.size

    ZSET_SCORE_STRUCT = FLOAT_CODEC.STRUCT
    ZSET_SCORE_FORMAT_LENGTH = ZSET_SCORE_STRUCT.size

    MIN_DELETED_VALUE = struct.pack('>B', DELETED_KEY_TYPE)

    # the key format using <key length + key> was inspired by the `blackwidow` project:
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/zsets_data_key_format.h#L44-L53
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/base_data_key_format.h#L37-L43
    #
    # LevelDB doesn't have column families like RocksDB, so the binary prefixes were created to distinguish object types

    def get_key(self, key, type_id):
        prefix = self.KEY_PREFIX_STRUCT.pack(type_id, len(key))
        return prefix + bytes(key)

    def encode_key_id_and_length(self, key, key_id, length):
        if key == key_id:
            # older schema before uuid
            return bytes(length)
        else:
            # newer schema with uuid
            return bytes(key_id) + bytes(length)

    def encode_string(self, key):
        return self.get_key(key, self.STRING_TYPE)

    def encode_set(self, key):
        return self.get_key(key, self.SET_TYPE)

    def encode_set_member(self, key, value):
        return self.get_key(key, self.SET_MEMBER_TYPE) + bytes(value)

    def get_min_set_member(self, key):
        return self.get_key(key, self.SET_MEMBER_TYPE)

    def encode_hash(self, key):
        return self.get_key(key, self.HASH_TYPE)

    def encode_hash_field(self, key, field):
        return self.get_key(key, self.HASH_FIELD_TYPE) + bytes(field)

    def get_min_hash_field(self, key):
        return self.get_key(key, self.HASH_FIELD_TYPE)

    def encode_zset(self, key):
        return self.get_key(key, self.ZSET_TYPE)

    def encode_zset_value(self, key, value):
        return self.get_key(key, self.ZSET_VALUE_TYPE) + bytes(value)

    def encode_zset_score(self, key, value, score):
        score = float(score)
        return self.get_key(key, self.ZSET_SCORE_TYPE) + FLOAT_CODEC.encode(score) + bytes(value)

    def encode_deleted_zset_score(self, key_id):
        return self.get_key(self.get_min_zset_score(key_id), self.DELETED_KEY_TYPE)

    def encode_deleted_zset_value(self, key_id):
        return self.get_key(self.get_min_zset_value(key_id), self.DELETED_KEY_TYPE)

    def encode_deleted_hash(self, key_id):
        return self.get_key(self.get_min_hash_field(key_id), self.DELETED_KEY_TYPE)

    def encode_deleted_set(self, key_id):
        return self.get_key(self.get_min_set_member(key_id), self.DELETED_KEY_TYPE)

    def decode_key(self, key):
        type_id, key_length = self.KEY_PREFIX_STRUCT.unpack(key[:self.KEY_PREFIX_LENGTH])
        key_value = key[self.KEY_PREFIX_LENGTH:]
        return type_id, key_length, key_value

    def decode_key_id_and_length(self, key, db_value):
        """
        In previous versions of dredis, all stored keys for zset, set, and hash were prefixed with the key name.
        In retrospect, that was a bad idea because delete operations had to be synchronous to ensure consistency.

        The new approach is to store a unique ID along with the key length, then every related stored key
        (zset score/value, set member, and hash field) will have the key ID as prefix instead of the key name.
        The new approach should allow asynchronous deletions and still guarantee strong consistency.

        Example (values meant to exemplify the idea, not the real bytes):
            zadd z 100 alice 200 bob

            Previously the following keys would be created
                6_z = 2
                7_z_alice = 100
                7_z_bob = 200
                8_z_100_alice = ''
                8_z_100_alice = ''

            New approach:
                6_z = uniqueID_2
                7_uniqueID_alice = 100
                7_uniqueID_bob = 200
                8_uniqueID_100_alice = ''
                8_uniqueID_100_bob = ''

            Then on deletions, the "pointer" key should be removed immediately and the related keys marked
            to be removed asynchronously. The next time a `zadd z` is executed, it will get a new unique ID
            that won't collide with the previous.

        To make migrations seamless and not break existing dredis installations, for previously created objects,
        we assume the unique ID is the key name.
        """
        if db_value is None:
            # newer schema with uuid
            key_id = uuid.uuid4().bytes
            length = '0'
        else:
            if len(db_value) < UUID_LENGTH_IN_BYTES:
                # older schema before uuid
                key_id = key
                length = db_value
            else:
                # new schema with uuid
                key_id = db_value[:UUID_LENGTH_IN_BYTES]
                length = db_value[UUID_LENGTH_IN_BYTES:]
        length = int(length)
        return key_id, length

    def decode_zset_score(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        score_bytes = key_name[length:length + self.ZSET_SCORE_FORMAT_LENGTH]
        return FLOAT_CODEC.decode(score_bytes)

    def decode_zset_value(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        return key_name[length + self.ZSET_SCORE_FORMAT_LENGTH:]

    def get_min_zset_score(self, key):
        return self.get_key(key, self.ZSET_SCORE_TYPE)

    def get_min_zset_value(self, key):
        return self.get_key(key, self.ZSET_VALUE_TYPE)


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
        with self._env.begin(write=True) as tnx:
            for k, v in self._put.items():
                tnx.put(k, v)
            for k in self._delete:
                tnx.delete(k)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.write()
            return True


class LMDBBackend(object):
    """
    Implement a subset of the interface of plyvel.DB
    """

    def __init__(self, path, **custom_options):
        default_options = {
            'map_size': 1 * 2 ** 30,  # 1GB
            'map_async': True,
            'writemap': True,
            'readahead': False,
            'metasync': False,
        }
        options = default_options.copy()
        options.update(custom_options)
        self._env = lmdb.open(path, **options)

    def get(self, key, default=None):
        with self._env.begin() as tnx:
            return tnx.get(key, default)

    def put(self, key, value):
        with self._env.begin(write=True) as tnx:
            tnx.put(key, value)

    def delete(self, key):
        with self._env.begin(write=True) as tnx:
            tnx.delete(key)

    def write_batch(self):
        return LMDBBatch(self._env)

    def close(self):
        self._env.close()

    def iterator(self, prefix=None, start=None, include_value=True):
        # LMDB doesn't have native prefix support, we must call `set_range()` to start it at the proper position,
        # otherwise it'd require extra iterations to filter by prefix.
        if start is None:
            start = prefix
        with self._env.begin() as t:
            c = t.cursor()
            if start is not None and not c.set_range(start):
                return
            for k, v in c:
                if prefix is not None and not k.startswith(prefix):
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


class MemoryBackend(object):
    """
    Implement a subset of the interface of plyvel.DB
    """

    def __init__(self, path, **custom_options):
        self._db = {}

    def get(self, key, default=None):
        return self._db.get(key, default)

    def put(self, key, value):
        self._db[key] = bytes(value)

    def delete(self, key):
        try:
            del self._db[key]
        except KeyError:
            return None

    def write_batch(self):
        return self

    def write(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            return True

    def iterator(self, prefix=None, start=None, include_value=True):
        for k, v in self:
            if start is not None and k < start:
                continue
            if prefix is not None and not k.startswith(prefix):
                continue
            if include_value:
                yield k, v
            else:
                yield k

    def close(self):
        pass

    def __iter__(self):
        for key in sorted(self._db):
            yield key, self._db[key]


def leveldb_backend(path, **custom_options):
    default_options = {
        'create_if_missing': True,
    }
    options = default_options.copy()
    options.update(custom_options)
    return plyvel.DB(path, **options)


DB_BACKENDS = {
    'leveldb': leveldb_backend,
    'lmdb': LMDBBackend,
    'memory': MemoryBackend,
}
DEFAULT_DB_BACKEND = 'leveldb'


class DBManager(object):

    def __init__(self):
        self._dbs = {}
        self._db_backend = DEFAULT_DB_BACKEND
        self._db_backend_options = {}
        self.thread_lock = threading.Lock()

    def setup_dbs(self, root_dir, backend, backend_options):
        self._db_backend = backend
        self._db_backend_options = backend_options
        for db_id_ in range(NUMBER_OF_REDIS_DATABASES):
            db_id = str(db_id_)
            directory = Path(root_dir).join(db_id)
            self._assign_db(db_id, directory)

    def open_db(self, path):
        db_factory = DB_BACKENDS[self._db_backend]
        options = self._db_backend_options
        return db_factory(bytes(path), **options)

    def get_db(self, db_id):
        return self._dbs[str(db_id)]['db']

    def delete_dbs(self):
        for db_id in self._dbs:
            self.delete_db(db_id)

    def delete_db(self, db_id):
        db_id = str(db_id)
        with self.thread_lock:
            self._dbs[db_id]['db'].close()
            self._dbs[db_id]['directory'].reset()
            self._assign_db(db_id, self._dbs[db_id]['directory'])

    def _assign_db(self, db_id, directory):
        self._dbs[db_id] = {
            'db': self.open_db(directory),
            'directory': directory,
        }


KEY_CODEC = KeyCodec()
DB_MANAGER = DBManager()
