import collections
import fnmatch
import hashlib
import re

from dredis.lua import LuaRunner
from dredis.path import Path

DECIMAL_REGEX = re.compile('(\d+)\.0+$')


class DiskKeyspace(object):

    def __init__(self, root_dir):
        self._root_directory = Path(root_dir)
        default_db = '0'
        self._set_db_directory(default_db)

    def _set_db_directory(self, db):
        self.directory = self._root_directory.join(db)

    def _key_path(self, key):
        return self.directory.join(key)

    def flushall(self):
        for db_id in range(15):
            self._root_directory.join(str(db_id)).reset()

    def flushdb(self):
        self.directory.reset()

    def select(self, db):
        self._set_db_directory(db)

    def incrby(self, key, increment=1):
        key_path = self._key_path(key)
        value_path = key_path.join('value')
        number = 0
        if self.exists(key):
            content = value_path.read()
            number = int(content)
        else:
            key_path.makedirs()
            self.write_type(key, 'string')
        result = number + increment
        value_path.write(str(result))
        return result

    def get(self, key):
        key_path = self._key_path(key)
        value_path = key_path.join('value')
        if self.exists(key):
            return value_path.read()
        else:
            return None

    def set(self, key, value):
        key_path = self._key_path(key)
        if not self.exists(key):
            key_path.makedirs()
            self.write_type(key, 'string')
        key_path.join('value').write(value)

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
        fname = hashlib.md5(value).hexdigest()
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
        fname = hashlib.md5(value).hexdigest()
        value_path = values_path.join(fname)
        return value_path.exists()

    def scard(self, key):
        return len(self.smembers(key))

    def delete(self, *keys):
        result = 0
        for key in keys:
            if self.exists(key):
                key_path = self._key_path(key)
                key_path.delete()
                result += 1
        return result

    def zadd(self, key, score, value):
        """
        # alternative
        /path/x/10
        /path/y/20 -> 10
        /path/z/30
        --------
        # alternative
        /path/10/x
        /path/20/y ->
        /path/30/z
        /path/1/y <-
        ------
        # alternative
        /path/10.txt -> x
        /path/20.txt -> y
        /path/30.txt -> z
        ------
        # current
        /path/scores/10 -> "x1\nx2"
        /path/scores/20 -> "y"
        /path/scores/30 -> "z"
        /path/values/hash(x) -> 1
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
        value_path = values_path.join(hashlib.md5(value).hexdigest())
        if value_path.exists():
            previous_score = value_path.read()
            if previous_score == score:
                return 0
            else:
                previous_score_path = scores_path.join(previous_score)
                previous_score_path.remove_line(value)

        value_path.write(score)
        score_path.append(value)
        return 1

    def write_type(self, key, name):
        key_path = self._key_path(key)
        type_path = key_path.join('type')
        type_path.write(name)

    def zrange(self, key, start, stop, with_scores):
        key_path = self._key_path(key)
        lines = []
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
            stop = -stop
        elif stop > len(lines):
            stop = -1
        end = len(lines) - stop + 1
        return lines[start:end]

    def zcard(self, key):
        key_path = self._key_path(key)
        values_path = key_path.join('values')
        if values_path.exists():
            return len(values_path.listdir())
        else:
            return 0

    def zscore(self, key, member):
        key_path = self._key_path(key)
        value_path = key_path.join('values').join(hashlib.md5(member).hexdigest())
        if value_path.exists():
            return value_path.read().strip()
        else:
            return None

    def eval(self, script, keys, argv):
        runtime = LuaRunner(self)
        return runtime.run(script, keys, argv)

    def zrem(self, key, *members):
        """
        /path/scores/10 -> "x1\nx2"
        /path/scores/20 -> "y"
        /path/scores/30 -> "z"
        /path/values/hash(x) -> 1
        """
        key_path = self._key_path(key)
        scores_path = key_path.join('scores')
        values_path = key_path.join('values')
        result = 0
        for member in members:
            value_path = values_path.join(hashlib.md5(member).hexdigest())
            if not value_path.exists():
                continue
            result += 1
            score = value_path.read().strip()
            score_path = scores_path.join(score)
            score_path.remove_line(member)
            value_path.delete()
        # empty zset should be removed from keyspace
        if values_path.empty_directory():
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
            scores = [score for score in scores if self._range_check(min_score, max_score, float(score))]
            for score in scores:
                lines = sorted(scores_path.join(score).readlines())
                for line in lines:
                    num_elems_read += 1
                    if len(result) / num_elems_per_entry >= count:
                        return result
                    if offset <= num_elems_read:
                        result.append(line)
                        if withscores:
                            result.append(str(score))
        return result

    def zcount(self, key, min_score, max_score):
        return len(self.zrangebyscore(key, min_score, max_score))

    def zrank(self, key, member):
        key_path = self._key_path(key)
        value_path = key_path.join('values').join(hashlib.md5(member).hexdigest())
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
        else:
            return 'none'

    def keys(self, pattern):
        all_keys = self.directory.listdir()
        return list(fnmatch.filter(all_keys, pattern))

    def exists(self, *keys):
        result = 0
        for key in keys:
            key_path = self._key_path(key)
            if key_path.exists():
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

    def _range_check(self, min_score, max_score, score):
        if min_score.startswith('('):
            if float(min_score[1:]) >= score:
                return False
        elif float(min_score) > score:
            return False

        if max_score.startswith('('):
            if float(max_score[1:]) <= score:
                return False
        elif float(max_score) < score:
            return False

        return True
