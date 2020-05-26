import collections
import datetime
import fnmatch
import time
from io import BytesIO

from dredis import rdb, config
from dredis.db import DB_MANAGER, KEY_CODEC, DEFAULT_REDIS_DB
from dredis.exceptions import DredisError, BusyKeyError, NoKeyError
from dredis.lua import LuaRunner
from dredis.utils import to_float

RDB_FILENAME_FORMAT = 'dump_%Y-%m-%dT%H:%M:%S.rdb'


def to_float_string(f):
    # copied from the redis source:
    # https://github.com/antirez/redis/blob/c8391388c221b9255a7b6536c3f43438f36b8e2b/src/networking.c#L500-L524
    return "{:.17g}".format(float(f))


class Cursors(object):
    """
    Limit the size of cursors to limit memory consumption
    """

    def __init__(self, max_size):
        self._keys = collections.deque(maxlen=max_size)
        self._data = {}
        self._max_size = max_size
        # starting at something > 1 to to avoid collisions when the process gets restarted
        self._cursor_id = int(time.time() * 1000)

    def add(self, db, key, value):
        if len(self._data) >= self._max_size:
            key_to_remove = self._keys.popleft()
            del self._data[key_to_remove]
        self._cursor_id += 1
        cursor_key = self._get_cursor_key(db, key, self._cursor_id)
        self._data[cursor_key] = value
        self._keys.append(cursor_key)
        return self._cursor_id

    def get(self, db, key, cursor):
        cursor_key = self._get_cursor_key(db, key, cursor)
        return self._data[cursor_key]

    def _get_cursor_key(self, db, key, cursor_id):
        return (db, key, cursor_id)


CURSOR_MAX_SIZE = 8 * 1024
ZSET_CURSORS = Cursors(CURSOR_MAX_SIZE)
HASH_CURSORS = Cursors(CURSOR_MAX_SIZE)


class Keyspace(object):

    def __init__(self):
        self._lua_runner = LuaRunner(self)
        self._current_db = DEFAULT_REDIS_DB
        self._set_db(self._current_db)
        self.authenticated = False

    def _set_db(self, db):
        self._current_db = str(db)

    def flushall(self):
        DB_MANAGER.delete_dbs()

    def flushdb(self):
        DB_MANAGER.delete_db(self._current_db)

    def select(self, db):
        self._set_db(db)

    def save(self):
        filename = datetime.datetime.utcnow().strftime(RDB_FILENAME_FORMAT)
        rdb.dump_rdb(self, filename)

    def incrby(self, key, increment=1):
        number = self.get(key)
        if number is None:
            number = '0'
        result = int(number) + increment
        self.set(key, str(result))
        return result

    def get(self, key):
        return self._db.get(KEY_CODEC.encode_string(key))

    def set(self, key, value):
        self._db.put(KEY_CODEC.encode_string(key), value)

    def getrange(self, key, start, end):
        value = self.get(key)
        if value is None:
            return ''
        else:
            if end < 0:
                end = len(value) + end
            end += 1  # inclusive
            return value[start:end]

    def sadd(self, key, value):
        key_id, length = self._get_set_key_id_and_length(key)
        if self._db.get(KEY_CODEC.encode_set_member(key_id, value)) is None:
            with self._db.write_batch() as batch:
                batch.put(KEY_CODEC.encode_set(key), KEY_CODEC.encode_key_id_and_length(key, key_id, length + 1))
                batch.put(KEY_CODEC.encode_set_member(key_id, value), bytes(''))
            return 1
        else:
            return 0

    def smembers(self, key):
        result = set()
        key_id, length = self._get_set_key_id_and_length(key)
        if length > 0:
            for db_key, _ in self._get_db_iterator(KEY_CODEC.get_min_set_member(key_id)):
                _, length, member_key = KEY_CODEC.decode_key(db_key)
                member_value = member_key[length:]
                result.add(member_value)
        return result

    def sismember(self, key, value):
        key_id, _ = self._get_set_key_id_and_length(key)
        return self._db.get(KEY_CODEC.encode_set_member(key_id, value)) is not None

    def scard(self, key):
        _, length = self._get_set_key_id_and_length(key)
        return length

    def _get_set_key_id_and_length(self, key):
        db_value = self._db.get(KEY_CODEC.encode_set(key))
        return KEY_CODEC.decode_key_id_and_length(key, db_value)

    def delete(self, *keys):
        result = 0
        for key in keys:
            if self._db.get(KEY_CODEC.encode_string(key)) is not None:
                self._delete_db_string(key)
                result += 1
            elif self._db.get(KEY_CODEC.encode_set(key)) is not None:
                self._delete_db_set(key)
                result += 1
            elif self._db.get(KEY_CODEC.encode_hash(key)) is not None:
                self._delete_db_hash(key)
                result += 1
            elif self._db.get(KEY_CODEC.encode_zset(key)) is not None:
                self._delete_db_zset(key)
                result += 1
        return result

    def _delete_db_string(self, key):
        # there is one set of db keys for strings:
        # * string
        self._db.delete(KEY_CODEC.encode_string(key))

    def _delete_db_set(self, key):
        # there are two sets of db keys for sets:
        # * set
        # * set members
        #
        # currently the `set` key is immediately deleted and the other keys
        # will be collected by gc.KeyGarbageCollector()
        key_id, _ = self._get_set_key_id_and_length(key)
        with self._db.write_batch() as batch:
            batch.delete(KEY_CODEC.encode_set(key))
            batch.put(KEY_CODEC.encode_deleted_set(key_id), bytes(''))

    def _delete_db_hash(self, key):
        # there are two sets of db keys for hashes:
        # * hash
        # * hash fields
        #
        # currently the `hash` key is immediately deleted and the other keys
        # will be collected by gc.KeyGarbageCollector()
        key_id, _ = self._get_hash_key_id_and_length(key)
        with self._db.write_batch() as batch:
            batch.delete(KEY_CODEC.encode_hash(key))
            batch.put(KEY_CODEC.encode_deleted_hash(key_id), bytes(''))

    def _delete_db_zset(self, key):
        # there are three sets of db keys for zsets:
        # * zset
        # * zset scores
        # * zset values
        #
        # currently the `zset` key is immediately deleted and the other keys
        # will be collected by gc.KeyGarbageCollector()
        key_id, _ = self._get_zset_key_id_and_length(key)
        with self._db.write_batch() as batch:
            batch.delete(KEY_CODEC.encode_zset(key))
            batch.put(KEY_CODEC.encode_deleted_zset_score(key_id), bytes(''))
            batch.put(KEY_CODEC.encode_deleted_zset_value(key_id), bytes(''))

    def _get_db_iterator(self, key_prefix=None, start=None):
        for db_key, db_value in self._db.iterator(prefix=key_prefix, start=start):
            yield db_key, db_value

    def zadd(self, key, score, value, nx=False, xx=False):
        key_id, zset_length = self._get_zset_key_id_and_length(key)

        batch = self._db.write_batch()
        db_score = self._db.get(KEY_CODEC.encode_zset_value(key_id, value))
        if db_score is not None:
            if nx:
                return 0
            result = 0
            previous_score = db_score
            if float(previous_score) == float(score):
                return result
            else:
                batch.delete(KEY_CODEC.encode_zset_score(key_id, value, previous_score))
        else:
            if xx:
                return 0
            result = 1
            zset_length += 1
            batch.put(KEY_CODEC.encode_zset(key), KEY_CODEC.encode_key_id_and_length(key, key_id, zset_length))

        batch.put(KEY_CODEC.encode_zset_value(key_id, value), to_float_string(score))
        batch.put(KEY_CODEC.encode_zset_score(key_id, value, score), bytes(''))
        batch.write()

        return result

    def zrange(self, key, start, stop, with_scores):
        result = []

        key_id, zset_length = self._get_zset_key_id_and_length(key)
        if stop < 0:
            end = zset_length + stop
        else:
            end = stop

        if start < 0:
            begin = max(0, zset_length + start)
        else:
            begin = start
        for i, (db_key, _) in enumerate(self._get_db_iterator(KEY_CODEC.get_min_zset_score(key_id))):
            if i < begin:
                continue
            if i > end:
                break
            db_score = KEY_CODEC.decode_zset_score(db_key)
            db_value = KEY_CODEC.decode_zset_value(db_key)
            result.append(db_value)
            if with_scores:
                result.append(db_score)

        return result

    def zcard(self, key):
        _, zset_length = self._get_zset_key_id_and_length(key)
        return zset_length

    def zscore(self, key, member):
        key_id, length = self._get_zset_key_id_and_length(key)
        if length == 0:
            return None
        else:
            return self._db.get(KEY_CODEC.encode_zset_value(key_id, member))

    def zscan(self, key, cursor, match, count):
        def get_key_value_pair(db_key, db_value):
            field = KEY_CODEC.decode_zset_value(db_key)
            value = KEY_CODEC.decode_zset_score(db_key)
            return field, value

        key_id, _ = self._get_zset_key_id_and_length(key)
        get_min_field = KEY_CODEC.get_min_zset_score
        cursors = ZSET_CURSORS

        return self._scan(key_id, cursor, match, count, get_min_field, get_key_value_pair, cursors)

    def eval(self, script, keys, argv):
        return self._lua_runner.run(script, keys, argv)

    def zrem(self, key, *members):
        """
        see zadd() for information about score and value structures
        """
        result = 0
        key_id, zset_length = self._get_zset_key_id_and_length(key)

        # safe guard
        if zset_length == 0:
            return result

        batch = self._db.write_batch()
        for member in members:
            score = self._db.get(KEY_CODEC.encode_zset_value(key_id, member))
            if score is None:
                continue
            result += 1
            zset_length -= 1
            batch.delete(KEY_CODEC.encode_zset_value(key_id, member))
            batch.delete(KEY_CODEC.encode_zset_score(key_id, member, score))

        # empty zset should be removed from keyspace
        if zset_length == 0:
            self.delete(key)
        else:
            batch.put(KEY_CODEC.encode_zset(key), KEY_CODEC.encode_key_id_and_length(key, key_id, zset_length))
            batch.write()
        return result

    def _get_zset_key_id_and_length(self, key):
        db_value = self._db.get(KEY_CODEC.encode_zset(key))
        return KEY_CODEC.decode_key_id_and_length(key, db_value)

    def zrangebyscore(self, key, min_score, max_score, withscores=False, offset=0, count=float('+inf')):
        result = []
        num_elems_read = 0
        if withscores:
            num_elems_per_entry = 2
        else:
            num_elems_per_entry = 1

        key_id, _ = self._get_zset_key_id_and_length(key)

        score_range = ScoreRange(min_score, max_score)
        for db_key in self._db.iterator(prefix=KEY_CODEC.get_min_zset_score(key_id), include_value=False):
            if len(result) / num_elems_per_entry >= count:
                return result
            db_score = KEY_CODEC.decode_zset_score(db_key)
            if score_range.above_max(db_score):
                break
            if score_range.check(db_score):
                num_elems_read += 1
                if len(result) / num_elems_per_entry >= count:
                    return result
                if num_elems_read > offset:
                    db_value = KEY_CODEC.decode_zset_value(db_key)
                    result.append(db_value)
                    if withscores:
                        result.append(db_score)
        return result

    def zcount(self, key, min_score, max_score):
        # TODO: optimize for performance. it's probably possible to create a new entry only for scores
        # like:
        #     <prefix>myzset<score> = number of elements with that score
        #
        #     ZADD myzset 10 a
        #     <prefix>_myzset_10 = 1  ; one element with score 10
        #
        #     ZADD myzset 10 b
        #     <prefix>_myzset_10 = 2  ; two elements with score 10
        key_id, _ = self._get_zset_key_id_and_length(key)
        score_range = ScoreRange(min_score, max_score)
        count = 0
        for db_key, _ in self._get_db_iterator(KEY_CODEC.get_min_zset_score(key_id)):
            db_score = KEY_CODEC.decode_zset_score(db_key)
            if score_range.check(db_score):
                count += 1
            if score_range.above_max(db_score):
                break
        return count

    def zrank(self, key, member):
        key_id, _ = self._get_zset_key_id_and_length(key)

        score = self._db.get(KEY_CODEC.encode_zset_value(key_id, member))
        if score is None:
            return None

        rank = 0
        for db_key, _ in self._get_db_iterator(KEY_CODEC.get_min_zset_score(key_id)):
            db_score = KEY_CODEC.decode_zset_score(db_key)
            db_value = KEY_CODEC.decode_zset_value(db_key)
            if db_score < float(score):
                rank += 1
            elif db_score == float(score) and db_value < member:
                rank += 1
            else:
                break
        return rank

    def zunionstore(self, destination, keys, weights):
        union = collections.defaultdict(list)
        for (key, weight) in zip(keys, weights):
            elem_with_scores = self.zrange(key, 0, -1, with_scores=True)
            while elem_with_scores:
                member = elem_with_scores.pop(0)
                score = elem_with_scores.pop(0)
                union[member].append(float(score) * weight)
        aggregate_fn = sum  # FIXME: redis also supports MIN and MAX

        result = 0
        for member, scores in union.items():
            score = aggregate_fn(scores)
            result += self.zadd(destination, str(score), member)
        return result

    def type(self, key):
        if self._db.get(KEY_CODEC.encode_string(key)) is not None:
            return 'string'
        if self._db.get(KEY_CODEC.encode_set(key)) is not None:
            return 'set'
        if self._db.get(KEY_CODEC.encode_hash(key)) is not None:
            return 'hash'
        if self._db.get(KEY_CODEC.encode_zset(key)) is not None:
            return 'zset'
        return 'none'

    def keys(self, pattern):
        db_keys = set()
        for key_type in KEY_CODEC.KEY_TYPES:
            for key in self._db.iterator(prefix=chr(key_type), include_value=False):
                _, _, key_value = KEY_CODEC.decode_key(key)
                if pattern is None or fnmatch.fnmatch(key_value, pattern):
                    db_keys.add(key_value)
        return db_keys

    def dbsize(self):
        return len(self.keys(pattern=None))

    def exists(self, *keys):
        result = 0
        for key in keys:
            if self.type(key) != 'none':
                result += 1
        return result

    def hset(self, key, field, value):
        result = 0
        key_id, hash_length = self._get_hash_key_id_and_length(key)
        if self._db.get(KEY_CODEC.encode_hash_field(key_id, field)) is None:
            result = 1
        with self._db.write_batch() as batch:
            batch.put(KEY_CODEC.encode_hash(key), KEY_CODEC.encode_key_id_and_length(key, key_id, hash_length + result))
            batch.put(KEY_CODEC.encode_hash_field(key_id, field), value)
        return result

    def _get_hash_key_id_and_length(self, key):
        db_value = self._db.get(KEY_CODEC.encode_hash(key))
        return KEY_CODEC.decode_key_id_and_length(key, db_value)

    def hsetnx(self, key, field, value):
        key_id, hash_length = self._get_hash_key_id_and_length(key)
        # only set if not set before
        if self._db.get(KEY_CODEC.encode_hash_field(key_id, field)) is None:
            with self._db.write_batch() as batch:
                batch.put(KEY_CODEC.encode_hash(key), KEY_CODEC.encode_key_id_and_length(key, key_id, hash_length + 1))
                batch.put(KEY_CODEC.encode_hash_field(key_id, field), value)
            return 1
        else:
            return 0

    def hdel(self, key, *fields):
        result = 0
        key_id, hash_length = self._get_hash_key_id_and_length(key)

        # safe guard
        if hash_length == 0:
            return result

        batch = self._db.write_batch()
        for field in fields:
            if self._db.get(KEY_CODEC.encode_hash_field(key_id, field)) is not None:
                result += 1
                hash_length -= 1
                batch.delete(KEY_CODEC.encode_hash_field(key_id, field))

        if hash_length == 0:
            # remove empty hashes from keyspace
            self.delete(key)
        else:
            batch.put(KEY_CODEC.encode_hash(key), KEY_CODEC.encode_key_id_and_length(key, key_id, hash_length))
            batch.write()
        return result

    def hget(self, key, field):
        key_id, _ = self._get_hash_key_id_and_length(key)
        return self._db.get(KEY_CODEC.encode_hash_field(key_id, field))

    def hkeys(self, key):
        result = []
        key_id, hash_length = self._get_hash_key_id_and_length(key)
        if hash_length > 0:
            for db_key, _ in self._get_db_iterator(KEY_CODEC.get_min_hash_field(key_id)):
                _, length, field_key = KEY_CODEC.decode_key(db_key)
                field = field_key[length:]
                result.append(field)

        return result

    def hvals(self, key):
        result = []
        key_id, hash_length = self._get_hash_key_id_and_length(key)
        if hash_length > 0:
            for _, db_value in self._get_db_iterator(KEY_CODEC.get_min_hash_field(key_id)):
                result.append(db_value)
        return result

    def hlen(self, key):
        key_id, hash_length = self._get_hash_key_id_and_length(key)
        return hash_length

    def hincrby(self, key, field, increment):
        before = self.hget(key, field) or '0'
        new_value = int(before) + int(increment)
        self.hset(key, field, str(new_value))
        return new_value

    def hgetall(self, key):
        result = []
        key_id, hash_length = self._get_hash_key_id_and_length(key)
        if hash_length > 0:
            for db_key, db_value in self._get_db_iterator(KEY_CODEC.get_min_hash_field(key_id)):
                _, length, field_key = KEY_CODEC.decode_key(db_key)
                field = field_key[length:]
                result.append(field)
                result.append(db_value)
        return result

    def hscan(self, key, cursor, match, count):
        def get_key_value_pair(db_key, db_value):
            _, length, field_key = KEY_CODEC.decode_key(db_key)
            field = field_key[length:]
            return field, db_value

        key_id, _ = self._get_hash_key_id_and_length(key)
        get_min_field = KEY_CODEC.get_min_hash_field
        cursors = HASH_CURSORS

        return self._scan(key_id, cursor, match, count, get_min_field, get_key_value_pair, cursors)

    def _scan(self, key, cursor, match, count, get_min_field, get_key_value_pair, cursors):
        elements = []
        new_cursor = 0
        min_db_key = get_min_field(key)

        if cursor == 0:
            db_key_from_cursor = min_db_key
        else:
            try:
                db_key_from_cursor = cursors.get(self._current_db, key, cursor)
            except KeyError:
                return [new_cursor, elements]

        for i, (db_key, db_value) in enumerate(self._get_db_iterator(start=db_key_from_cursor)):
            if not db_key.startswith(min_db_key):
                break
            # store the next element at the cursor
            if i == count:
                new_cursor = cursors.add(self._current_db, key, db_key)
                break
            field, value = get_key_value_pair(db_key, db_value)
            if match is None or fnmatch.fnmatch(field, match):
                elements.append(field)
                elements.append(value)
        return [new_cursor, elements]

    @property
    def _db(self):
        return DB_MANAGER.get_db(self._current_db)

    def dump(self, key):
        return rdb.generate_payload(self, key)

    def restore(self, key, ttl, payload, replace):
        # TODO: there's no TTL support at the moment
        object_type = self.type(key)
        if object_type != 'none':
            if replace:
                self.delete(key)
            else:
                raise BusyKeyError()
        rdb.verify_payload(payload)
        rdb.load_object(self, key, BytesIO(payload))

    def rename(self, old_name, new_name):
        if self.exists(old_name):
            if old_name == new_name:
                return
            # replace the key that holds the key ID and don't touch the rest
            key_type = self.type(old_name)
            if key_type == 'zset':
                old_db_key = KEY_CODEC.encode_zset(old_name)
                new_db_key = KEY_CODEC.encode_zset(new_name)
            elif key_type == 'hash':
                old_db_key = KEY_CODEC.encode_hash(old_name)
                new_db_key = KEY_CODEC.encode_hash(new_name)
            elif key_type == 'set':
                old_db_key = KEY_CODEC.encode_set(old_name)
                new_db_key = KEY_CODEC.encode_set(new_name)
            elif key_type == 'string':
                old_db_key = KEY_CODEC.encode_string(old_name)
                new_db_key = KEY_CODEC.encode_string(new_name)
            else:
                raise DredisError("invalid key type")
            self._replace_db_key(new_db_key, old_db_key)
        else:
            raise NoKeyError()

    def _replace_db_key(self, new_db_key, old_db_key):
        db_value = self._db.get(old_db_key)
        with self._db.write_batch() as batch:
            batch.delete(old_db_key)
            batch.put(new_db_key, db_value)

    def auth(self, password):
        if config.get('requirepass') == config.EMPTY:
            raise DredisError("client sent AUTH, but no password is set")
        if password != config.get('requirepass'):
            self.authenticated = False
            raise DredisError("invalid password")
        else:
            self.authenticated = True


class ScoreRange(object):

    def __init__(self, min_value, max_value):
        self._min_value = min_value
        self._max_value = max_value

    def check(self, value):
        if self._min_value.startswith('('):
            if to_float(self._min_value[1:]) >= value:
                return False
        elif to_float(self._min_value) > value:
            return False

        if self._max_value.startswith('('):
            if to_float(self._max_value[1:]) <= value:
                return False
        elif to_float(self._max_value) < value:
            return False

        return True

    def above_max(self, value):
        max_value = self._max_value[1:] if self._max_value.startswith('(') else self._max_value
        return value > to_float(max_value)
