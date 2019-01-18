import collections
import fnmatch

from dredis.ldb import LDB_DBS, LDB_KEY_TYPES, LDB_MIN_ZSET_SCORE, KEY_CODEC, get_ldb
from dredis.lua import LuaRunner
from dredis.path import Path
from dredis.utils import to_float

DEFAULT_REDIS_DB = '0'
NUMBER_OF_REDIS_DATABASES = 15


class DiskKeyspace(object):

    def __init__(self, root_dir):
        self._lua_runner = LuaRunner(self)
        self._root_directory = Path(root_dir)
        self._setup_dbs()
        self._current_db = DEFAULT_REDIS_DB
        self._set_db(self._current_db)

    def _set_db(self, db):
        db = str(db)
        self._current_db = db
        self.directory = self._root_directory.join(db)

    def _key_path(self, key):
        return self.directory.join(key)

    def _setup_dbs(self):
        for db_id_ in range(NUMBER_OF_REDIS_DATABASES):
            db_id = str(db_id_)
            if db_id not in LDB_DBS:
                directory = self._root_directory.join(db_id)
                LDB_DBS[db_id] = get_ldb(directory)

    def flushall(self):
        for db_id_ in range(NUMBER_OF_REDIS_DATABASES):
            db_id = str(db_id_)
            directory = self._root_directory.join(db_id)
            LDB_DBS[db_id].close()
            directory.reset()
            LDB_DBS[db_id] = get_ldb(directory)

    def flushdb(self):
        self._ldb.close()
        self.directory.reset()
        self._ldb = get_ldb(self.directory)

    def select(self, db):
        self._set_db(db)

    def incrby(self, key, increment=1):
        number = self.get(key)
        if number is None:
            number = '0'
        result = int(number) + increment
        self.set(key, str(result))
        return result

    def get(self, key):
        return self._ldb.get(KEY_CODEC.encode_string(key))

    def set(self, key, value):
        self._ldb.put(KEY_CODEC.encode_string(key), value)

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
        if self._ldb.get(KEY_CODEC.encode_set_member(key, value)) is None:
            length = int(self._ldb.get(KEY_CODEC.encode_set(key)) or b'0')
            self._ldb.put(KEY_CODEC.encode_set(key), bytes(length + 1))
            self._ldb.put(KEY_CODEC.encode_set_member(key, value), bytes(''))
            return 1
        else:
            return 0

    def smembers(self, key):
        result = set()
        if self._ldb.get(KEY_CODEC.encode_set(key)):
            # the empty string marks the beginning of the members
            member_start = KEY_CODEC.encode_set_member(key, bytes(''))
            for db_key, db_value in self._ldb.iterator(start=member_start, include_start=False):
                _, length, member_key = KEY_CODEC.decode_key(db_key)
                member_value = member_key[length:]
                result.add(member_value)
        return result

    def sismember(self, key, value):
        return self._ldb.get(KEY_CODEC.encode_set_member(key, value)) is not None

    def scard(self, key):
        length = self._ldb.get(KEY_CODEC.encode_set(key))
        if length is None:
            return 0
        else:
            return int(length)

    def delete(self, *keys):
        result = 0
        for key in keys:
            if self._ldb.get(KEY_CODEC.encode_string(key)) is not None:
                self._ldb.delete(KEY_CODEC.encode_string(key))
                result += 1
            elif self._ldb.get(KEY_CODEC.encode_set(key)) is not None:
                self._ldb.delete(KEY_CODEC.encode_set(key))
                for member in self.smembers(key):
                    self._ldb.delete(KEY_CODEC.encode_set_member(key, member))
                result += 1
            elif self._ldb.get(KEY_CODEC.encode_hash(key)) is not None:
                self._ldb.delete(KEY_CODEC.encode_hash(key))
                for field in self.hkeys(key):
                    self._ldb.delete(KEY_CODEC.encode_hash_field(key, field))
                result += 1
            elif self._ldb.get(KEY_CODEC.encode_zset(key)) is not None:
                self._ldb.delete(KEY_CODEC.encode_zset(key))
                min_key = KEY_CODEC.encode_zset_score(key, bytes(''), LDB_MIN_ZSET_SCORE)
                for db_key, _ in self._ldb.iterator(start=min_key, include_start=True):
                    self._ldb.delete(db_key)
                result += 1

        return result

    def zadd(self, key, score, value):
        """
        This is an example of how sorted sets are stored on leveldb

        zadd myzset 10 "hello"
        zadd myzset 10 "world"
        zadd myzset 11 "hello"

        zset_6_myzset = 2

        zset_6_myzset_7_hello = 10
        zset_6_myzset_8_10_hello = ''

        zset_6_myzset_7_value_world = 10
        zset_6_myzset_8_world = ''
        """
        zset_length = int(self._ldb.get(KEY_CODEC.encode_zset(key), '0'))

        db_score = self._ldb.get(KEY_CODEC.encode_zset_value(key, value))
        if db_score is not None:
            result = 0
            previous_score = db_score
            if float(previous_score) == float(score):
                return result
            else:
                self._ldb.delete(KEY_CODEC.encode_zset_score(key, value, previous_score))
        else:
            result = 1
            zset_length += 1

        self._ldb.put(KEY_CODEC.encode_zset(key), bytes(zset_length))
        self._ldb.put(KEY_CODEC.encode_zset_value(key, value), bytes(score))
        self._ldb.put(KEY_CODEC.encode_zset_score(key, value, score), bytes(''))

        return result

    def zrange(self, key, start, stop, with_scores):
        result = []

        zset_length = int(self._ldb.get(KEY_CODEC.encode_zset(key), '0'))
        if stop < 0:
            end = zset_length + stop
        else:
            end = stop

        if start < 0:
            begin = max(0, zset_length + start)
        else:
            begin = start
        min_key = KEY_CODEC.encode_zset_score(key, bytes(''), LDB_MIN_ZSET_SCORE)
        for i, (db_key, _) in enumerate(self._ldb.iterator(start=min_key, include_start=True)):
            if i < begin:
                continue
            if i > end:
                break
            db_score = KEY_CODEC.decode_zset_score(db_key)
            db_value = KEY_CODEC.decode_zset_value(db_key)
            result.append(db_value)
            if with_scores:
                result.append(str(db_score))

        return result

    def zcard(self, key):
        return int(self._ldb.get(KEY_CODEC.encode_zset(key), '0'))

    def zscore(self, key, member):
        return self._ldb.get(KEY_CODEC.encode_zset_value(key, member))

    def eval(self, script, keys, argv):
        return self._lua_runner.run(script, keys, argv)

    def zrem(self, key, *members):
        """
        see zadd() for information about score and value structures
        """
        result = 0
        zset_length = int(self._ldb.get(KEY_CODEC.encode_zset(key), '0'))
        for member in members:
            score = self._ldb.get(KEY_CODEC.encode_zset_value(key, member))
            if score is None:
                continue
            result += 1
            zset_length -= 1
            self._ldb.delete(KEY_CODEC.encode_zset_value(key, member))
            self._ldb.delete(KEY_CODEC.encode_zset_score(key, member, score))

        # empty zset should be removed from keyspace
        if zset_length == 0:
            self.delete(key)
        else:
            self._ldb.put(KEY_CODEC.encode_zset(key), bytes(zset_length))
        return result

    def zrangebyscore(self, key, min_score, max_score, withscores=False, offset=0, count=float('+inf')):
        result = []
        num_elems_read = 0
        if withscores:
            num_elems_per_entry = 2
        else:
            num_elems_per_entry = 1

        score_range = ScoreRange(min_score, max_score)
        min_key = KEY_CODEC.encode_zset_score(key, bytes(''), LDB_MIN_ZSET_SCORE)
        for db_key, _ in self._ldb.iterator(start=min_key, include_start=True):
            db_score = KEY_CODEC.decode_zset_score(db_key)
            db_value = KEY_CODEC.decode_zset_value(db_key)
            if score_range.above_max(db_score):
                break
            if score_range.check(db_score):
                num_elems_read += 1
                if len(result) / num_elems_per_entry >= count:
                    return result
                if num_elems_read > offset:
                    result.append(db_value)
                    if withscores:
                        result.append(str(db_score))
        return result

    def zcount(self, key, min_score, max_score):
        # TODO: optimize for performance. it's probably possible to create a new entry only for scores
        # like:
        #     zadd myzset 10 a
        #     zset_6_myzset_10 = 1
        #     zadd myzset 10 b
        #     zset_6_myzset_10 = 2

        score_range = ScoreRange(min_score, max_score)
        min_key = KEY_CODEC.encode_zset_score(key, bytes(''), LDB_MIN_ZSET_SCORE)
        count = 0
        for db_key, _ in self._ldb.iterator(start=min_key, include_start=True):
            db_score = KEY_CODEC.decode_zset_score(db_key)
            if score_range.check(db_score):
                count += 1
            if score_range.above_max(db_score):
                break
        return count

    def zrank(self, key, member):
        score = self._ldb.get(KEY_CODEC.encode_zset_value(key, member))
        if score is None:
            return None

        min_key = KEY_CODEC.encode_zset_score(key, bytes(''), LDB_MIN_ZSET_SCORE)
        rank = 0
        for db_key, _ in self._ldb.iterator(start=min_key, include_start=True):
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
        if self._ldb.get(KEY_CODEC.encode_string(key)):
            return 'string'
        if self._ldb.get(KEY_CODEC.encode_set(key)):
            return 'set'
        if self._ldb.get(KEY_CODEC.encode_hash(key)):
            return 'hash'
        if self._ldb.get(KEY_CODEC.encode_zset(key)):
            return 'zset'
        return 'none'

    def keys(self, pattern):
        level_db_keys = set()
        for key, _ in self._ldb:
            key_type, _, key_value = KEY_CODEC.decode_key(key)
            if key_type not in LDB_KEY_TYPES:
                continue
            if pattern is None or fnmatch.fnmatch(key_value, pattern):
                level_db_keys.add(key_value)
        return level_db_keys

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
        if self._ldb.get(KEY_CODEC.encode_hash_field(key, field)) is None:
            result = 1
        hash_length = int(self._ldb.get(KEY_CODEC.encode_hash(key), '0'))
        self._ldb.put(KEY_CODEC.encode_hash(key), bytes(hash_length + 1))
        self._ldb.put(KEY_CODEC.encode_hash_field(key, field), value)
        return result

    def hsetnx(self, key, field, value):
        # only set if not set before
        if self._ldb.get(KEY_CODEC.encode_hash_field(key, field)) is None:
            hash_length = int(self._ldb.get(KEY_CODEC.encode_hash(key), '0'))
            self._ldb.put(KEY_CODEC.encode_hash(key), bytes(hash_length + 1))
            self._ldb.put(KEY_CODEC.encode_hash_field(key, field), value)
            return 1
        else:
            return 0

    def hdel(self, key, *fields):
        result = 0
        hash_length = int(self._ldb.get(KEY_CODEC.encode_hash(key), '0'))

        for field in fields:
            if self._ldb.get(KEY_CODEC.encode_hash_field(key, field)) is not None:
                result += 1
                hash_length -= 1
                self._ldb.delete(KEY_CODEC.encode_hash_field(key, field))

        if hash_length == 0:
            # remove empty hashes from keyspace
            self.delete(key)
        else:
            self._ldb.put(KEY_CODEC.encode_hash(key), bytes(hash_length))
        return result

    def hget(self, key, field):
        return self._ldb.get(KEY_CODEC.encode_hash_field(key, field))

    def hkeys(self, key):
        result = []
        if self._ldb.get(KEY_CODEC.encode_hash(key)) is not None:
            # the empty string marks the beginning of the fields
            field_start = KEY_CODEC.encode_hash_field(key, bytes(''))
            for db_key, db_value in self._ldb.iterator(start=field_start, include_start=False):
                _, length, field_key = KEY_CODEC.decode_key(db_key)
                field = field_key[length:]
                result.append(field)

        return result

    def hvals(self, key):
        result = []
        if self._ldb.get(KEY_CODEC.encode_hash(key)) is not None:
            # the empty string marks the beginning of the fields
            field_start = KEY_CODEC.encode_hash_field(key, bytes(''))
            for db_key, db_value in self._ldb.iterator(start=field_start, include_start=False):
                result.append(db_value)
        return result

    def hlen(self, key):
        result = self._ldb.get(KEY_CODEC.encode_hash(key))
        if result is None:
            return 0
        else:
            return int(result)

    def hincrby(self, key, field, increment):
        before = self.hget(key, field) or '0'
        new_value = int(before) + int(increment)
        self.hset(key, field, str(new_value))
        return new_value

    def hgetall(self, key):
        keys = self.hkeys(key)
        values = self.hvals(key)
        result = []
        for (k, v) in zip(keys, values):
            result.append(k)
            result.append(v)
        return result

    def _get_ldb(self):
        return LDB_DBS[self._current_db]

    def _set_ldb(self, value):
        LDB_DBS[self._current_db] = value

    _ldb = property(_get_ldb, _set_ldb)


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
