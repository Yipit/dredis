import collections
import fnmatch
import hashlib
import re
import struct

import plyvel

from dredis.lua import LuaRunner
from dredis.path import Path
from dredis.utils import to_float

DEFAULT_REDIS_DB = '0'
NUMBER_OF_REDIS_DATABASES = 15
DECIMAL_REGEX = re.compile(r'(\d+)\.0+$')


LDB_DBS = {}

LDB_STRING_TYPE = 1

# type_id | key_length
LDB_KEY_PREFIX_FORMAT = '>BI'
LDB_KEY_PREFIX_LENGTH = struct.calcsize(LDB_KEY_PREFIX_FORMAT)


def get_ldb_key(key, type_id):
    prefix = struct.pack(LDB_KEY_PREFIX_FORMAT, type_id, len(key))
    return prefix + bytes(key)


def encode_ldb_key_string(key):
    return get_ldb_key(key, LDB_STRING_TYPE)


def decode_ldb_key(key):
    return key[LDB_KEY_PREFIX_LENGTH:]


class DiskKeyspace(object):

    def __init__(self, root_dir):
        self._lua_runner = LuaRunner(self)
        self._root_directory = Path(root_dir)
        self._current_db = DEFAULT_REDIS_DB
        self._set_db(self._current_db)

    def _set_db(self, db):
        db = str(db)
        self._current_db = db
        self.directory = self._root_directory.join(db)
        if db not in LDB_DBS:
            LDB_DBS[db] = plyvel.DB(bytes(self.directory + "-leveldb"), create_if_missing=True)

    def _key_path(self, key):
        return self.directory.join(key)

    def setup_directories(self):
        for db_id in range(NUMBER_OF_REDIS_DATABASES):
            self._root_directory.join(str(db_id)).makedirs(ignore_if_exists=True)

    def flushall(self):
        for db_id_ in range(NUMBER_OF_REDIS_DATABASES):
            db_id = str(db_id_)
            directory = self._root_directory.join(db_id)
            if db_id in LDB_DBS:
                ldb_directory = bytes(directory + "-leveldb")
                LDB_DBS[db_id].close()
                Path(ldb_directory).reset()
                LDB_DBS[db_id] = plyvel.DB(ldb_directory, create_if_missing=True)
            directory.reset()

    def flushdb(self):
        ldb_directory = bytes(self.directory + "-leveldb")
        LDB_DBS[self._current_db].close()
        Path(ldb_directory).reset()
        LDB_DBS[self._current_db] = plyvel.DB(ldb_directory, create_if_missing=True)
        self.directory.reset()

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
        return LDB_DBS[self._current_db].get(encode_ldb_key_string(key))

    def set(self, key, value):
        LDB_DBS[self._current_db].put(encode_ldb_key_string(key), value)

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
        key_path = self._key_path(key)
        values_path = key_path.join('values')
        if not self.exists(key):
            values_path.makedirs()
            self.write_type(key, 'set')
        fname = self._get_filename_hash(value)
        value_path = values_path.join(fname)
        if value_path.exists():
            return 0
        else:
            value_path.write(value)
            return 1

    def smembers(self, key):
        result = set()
        if self.exists(key):
            key_path = self._key_path(key)
            values_path = key_path.join('values')
            for fname in values_path.listdir():
                content = values_path.join(fname).read()
                result.add(content)
        return result

    def sismember(self, key, value):
        key_path = self._key_path(key)
        values_path = key_path.join('values')
        fname = self._get_filename_hash(value)
        value_path = values_path.join(fname)
        return value_path.exists()

    def scard(self, key):
        return len(self.smembers(key))

    def delete(self, *keys):
        result = 0
        for key in keys:
            key_path = self._key_path(key)
            if key_path.exists():
                key_path.delete()

            if self.get(key):
                LDB_DBS[self._current_db].delete(encode_ldb_key_string(key))
                result += 1
        return result

    def zadd(self, key, score, value):
        """
        score structure (binary)
        ------
        /path/to/scores/10 -> 8 bytes + (4 bytes + content)*
        example:
        0x0002   0x05 hello     0x05 world
         count   size data      size data
          2       5   "hello"   5    "world"

        value structure
        ------
        /path/values/hash("x1") -> "10"
        """

        # if `score` has 0 as the decimal point, trim it: 10.00 -> 10
        match = DECIMAL_REGEX.match(score)
        if match:
            score = match.group(1)

        key_path = self._key_path(key)
        scores_path = key_path.join('scores')
        values_path = key_path.join('values')
        if not key_path.exists():
            scores_path.makedirs()
            values_path.makedirs()
            self.write_type(key, 'zset')

        score_path = scores_path.join(score)
        value_path = values_path.join(self._get_filename_hash(value))
        if value_path.exists():
            result = 0
            previous_score = value_path.read()
            if previous_score == score:
                return result
            else:
                previous_score_path = scores_path.join(previous_score)
                previous_score_path.remove_line(value)
        else:
            result = 1

        value_path.write(score)
        score_path.append(value)
        return result

    def write_type(self, key, name):
        key_path = self._key_path(key)
        type_path = key_path.join('type')
        type_path.write(name)

    def zrange(self, key, start, stop, with_scores):
        key_path = self._key_path(key)
        lines = []
        if with_scores:
            n_elems = 2
        else:
            n_elems = 1
        scores_path = key_path.join('scores')
        if scores_path.exists():
            scores = sorted(scores_path.listdir(), key=float)
            for score in scores:
                sublist = sorted(scores_path.join(score).readlines())
                for line in sublist:
                    lines.append(line)
                    if with_scores:
                        lines.append(score)
        if stop < 0:
            end = len(lines) + stop * n_elems + n_elems
        else:
            end = (stop + 1) * n_elems

        if start < 0:
            begin = max(0, len(lines) + start * n_elems)
        else:
            begin = start * n_elems

        return lines[begin:end]

    def zcard(self, key):
        key_path = self._key_path(key)
        values_path = key_path.join('values')
        if values_path.exists():
            return len(values_path.listdir())
        else:
            return 0

    def zscore(self, key, member):
        key_path = self._key_path(key)
        value_path = key_path.join('values').join(self._get_filename_hash(member))
        if value_path.exists():
            return value_path.read().strip()
        else:
            return None

    def eval(self, script, keys, argv):
        return self._lua_runner.run(script, keys, argv)

    def zrem(self, key, *members):
        """
        see zadd() for information about score and value structures
        """
        key_path = self._key_path(key)
        scores_path = key_path.join('scores')
        values_path = key_path.join('values')
        result = 0
        for member in members:
            value_path = values_path.join(self._get_filename_hash(member))
            if not value_path.exists():
                continue
            result += 1
            score = value_path.read().strip()
            score_path = scores_path.join(score)
            score_path.remove_line(member)
            value_path.delete()
        # empty zset should be removed from keyspace
        if scores_path.empty_directory():
            self.delete(key)
        return result

    def zrangebyscore(self, key, min_score, max_score, withscores=False, offset=0, count=float('+inf')):
        result = []
        num_elems_read = 0
        if withscores:
            num_elems_per_entry = 2
        else:
            num_elems_per_entry = 1
        key_path = self._key_path(key)
        scores_path = key_path.join('scores')
        if scores_path.exists():
            scores = sorted(scores_path.listdir(), key=float)
            score_range = ScoreRange(min_score, max_score)
            scores = [score for score in scores if score_range.check(float(score))]
            for score in scores:
                lines = sorted(scores_path.join(score).readlines())
                for line in lines:
                    num_elems_read += 1
                    if len(result) / num_elems_per_entry >= count:
                        return result
                    if num_elems_read > offset:
                        result.append(line)
                        if withscores:
                            result.append(str(score))
        return result

    def zcount(self, key, min_score, max_score):
        key_path = self._key_path(key)
        scores_path = key_path.join('scores')
        count = 0
        if scores_path.exists():
            scores = sorted(scores_path.listdir(), key=float)
            score_range = ScoreRange(min_score, max_score)
            scores = [score for score in scores if score_range.check(float(score))]
            for score in scores:
                count += scores_path.join(score).read_zset_header()
        return count

    def zrank(self, key, member):
        key_path = self._key_path(key)
        value_path = key_path.join('values').join(self._get_filename_hash(member))
        if value_path.exists():
            scores_path = key_path.join('scores')
            scores = sorted(scores_path.listdir(), key=float)
            member_score = value_path.read().strip()
            rank = 0
            for score in scores:
                score_path = scores_path.join(str(score))
                lines = score_path.readlines()  # FIXME: move this to be inside the `if` block
                if score == member_score:
                    for line in lines:
                        if line == member:
                            return rank
                        else:
                            rank += 1
                else:
                    rank += len(lines)

            return rank
        else:
            return None

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
        key_path = self._key_path(key)
        if key_path.exists():
            type_path = key_path.join('type')
            return type_path.read()
        elif self.get(key):
            # leveldb only supports strings at the moment
            return 'string'
        else:
            return 'none'

    def keys(self, pattern):
        level_db_keys = []
        for key, _ in LDB_DBS[self._current_db]:
            key_value = decode_ldb_key(key)
            if pattern is None or fnmatch.fnmatch(key_value, pattern):
                level_db_keys.append(key_value)
        return self.directory.listdir(pattern) + level_db_keys

    def dbsize(self):
        return len(self.keys(pattern=None))

    def exists(self, *keys):
        result = 0
        for key in keys:
            key_path = self._key_path(key)
            if key_path.exists() or self.get(key):
                result += 1
        return result

    def hset(self, key, field, value):
        key_path = self._key_path(key)
        fields_path = key_path.join('fields')
        if not self.exists(key):
            fields_path.makedirs()
        field_path = fields_path.join(field)
        if field_path.exists():
            result = 0
        else:
            result = 1
        field_path.write(value)
        self.write_type(key, 'hash')
        return result

    def hsetnx(self, key, field, value):
        key_path = self._key_path(key)
        fields_path = key_path.join('fields')
        if not self.exists(key):
            fields_path.makedirs()
        field_path = fields_path.join(field)
        result = 0
        # only set if not set before
        if not field_path.exists():
            result = 1
            field_path.write(value)
        self.write_type(key, 'hash')
        return result

    def hdel(self, key, *fields):
        result = 0
        key_path = self._key_path(key)
        fields_path = key_path.join('fields')
        for field in fields:
            field_path = fields_path.join(field)
            if field_path.exists():
                field_path.delete()
                result += 1
        # remove empty hashes from keyspace
        if fields_path.empty_directory():
            self.delete(key)
        return result

    def hget(self, key, field):
        key_path = self._key_path(key)
        fields_path = key_path.join('fields')
        field_path = fields_path.join(field)
        if field_path.exists():
            result = field_path.read()
        else:
            result = None
        return result

    def hkeys(self, key):
        key_path = self._key_path(key)
        fields_path = key_path.join('fields')
        if fields_path.exists():
            result = fields_path.listdir()
        else:
            result = []
        return result

    def hvals(self, key):
        result = []
        key_path = self._key_path(key)
        fields_path = key_path.join('fields')
        if fields_path.exists():
            for field in fields_path.listdir():
                field_path = fields_path.join(field)
                value = field_path.read()
                result.append(value)
        return result

    def hlen(self, key):
        key_path = self._key_path(key)
        fields_path = key_path.join('fields')
        result = 0
        if fields_path.exists():
            result = len(fields_path.listdir())
        return result

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

    def _get_filename_hash(self, value):
        return hashlib.md5(value).hexdigest()


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
