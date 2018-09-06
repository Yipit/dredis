import collections
import fnmatch
import hashlib
import os.path
import re
import tempfile

from dredis.lua import LuaRunner
from dredis.path import Path

DECIMAL_REGEX = re.compile('(\d+)\.0+$')


class DiskKeyspace(object):

    def __init__(self, root_dir):
        self._root_directory = root_dir
        default_db = '0'
        self._set_db_directory(default_db)

    def _set_db_directory(self, db):
        self.directory = Path(self._root_directory).join(db)

    def _key_path(self, key):
        return Path(self.directory).join(key)

    def flushall(self):
        for db_id in range(15):
            Path(os.path.join(self._root_directory, str(db_id))).reset()

    def flushdb(self):
        Path(self.directory).reset()

    def select(self, db):
        self._set_db_directory(db)

    def incrby(self, key, increment=1):
        key_path = self._key_path(key)
        value_path = os.path.join(key_path, 'value')
        number = 0
        if self.exists(key):
            content = Path(value_path).read()
            number = int(content)
        else:
            os.makedirs(key_path)
            self.write_type(key, 'string')
        result = number + increment
        Path(value_path).write(str(result))
        return result

    def get(self, key):
        key_path = self._key_path(key)
        value_path = os.path.join(key_path, 'value')
        if self.exists(key):
            return Path(value_path).read()
        else:
            return None

    def set(self, key, value):
        key_path = self._key_path(key)
        if not self.exists(key):
            os.makedirs(key_path)
            self.write_type(key, 'string')
        Path(os.path.join(key_path, 'value')).write(value)

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
        values_path = os.path.join(key_path, 'values')
        if not self.exists(key):
            os.makedirs(values_path)
            self.write_type(key, 'set')
        fname = hashlib.md5(value).hexdigest()
        value_path = os.path.join(values_path, fname)
        if os.path.exists(value_path):
            return 0
        else:
            Path(value_path).write(value)
            return 1

    def smembers(self, key):
        result = set()
        if self.exists(key):
            key_path = self._key_path(key)
            values_path = os.path.join(key_path, 'values')
            for fname in os.listdir(values_path):
                content = Path(os.path.join(values_path, fname)).read()
                result.add(content)
        return result

    def sismember(self, key, value):
        key_path = self._key_path(key)
        values_path = os.path.join(key_path, 'values')
        fname = hashlib.md5(value).hexdigest()
        value_path = os.path.join(values_path, fname)
        return os.path.exists(value_path)

    def scard(self, key):
        return len(self.smembers(key))

    def delete(self, *keys):
        result = 0
        for key in keys:
            if self.exists(key):
                key_path = self._key_path(key)
                Path(key_path).delete()
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
        scores_path = os.path.join(key_path, 'scores')
        values_path = os.path.join(key_path, 'values')
        if not os.path.exists(key_path):
            os.makedirs(scores_path)
            os.makedirs(values_path)
            self.write_type(key, 'zset')

        score_path = os.path.join(scores_path, score)
        value_path = os.path.join(values_path, hashlib.md5(value).hexdigest())
        if os.path.exists(value_path):
            previous_score = Path(value_path).read()
            if previous_score == score:
                return 0
            else:
                previous_score_path = os.path.join(scores_path, previous_score)
                self._remove_line_from_file(previous_score_path, skip_line=value)

        Path(value_path).write(score)
        Path(score_path).append(value)
        return 1

    def write_type(self, key, name):
        key_path = self._key_path(key)
        type_path = os.path.join(key_path, 'type')
        Path(type_path).write(name)

    def _remove_line_from_file(self, score_path, skip_line):
        tempfd, tempfname = tempfile.mkstemp()
        with open(tempfname, 'w') as tfile:
            with open(score_path) as f:
                for line in f.readlines():
                    if line.strip() != skip_line:
                        tfile.write(line)
        os.close(tempfd)
        os.rename(tempfname, score_path)

    def zrange(self, key, start, stop, with_scores):
        key_path = self._key_path(key)
        lines = []
        scores_path = os.path.join(key_path, 'scores')
        if os.path.exists(scores_path):
            scores = sorted(os.listdir(scores_path), key=float)
            for score in scores:
                sublist = sorted(Path(os.path.join(scores_path, score)).readlines())
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
        values_path = os.path.join(key_path, 'values')
        if os.path.exists(values_path):
            return len(os.listdir(values_path))
        else:
            return 0

    def zscore(self, key, member):
        key_path = self._key_path(key)
        value_path = os.path.join(key_path, 'values', hashlib.md5(member).hexdigest())
        if os.path.exists(value_path):
            return Path(value_path).read().strip()
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
        scores_path = os.path.join(key_path, 'scores')
        values_path = os.path.join(key_path, 'values')
        result = 0
        for member in members:
            value_path = os.path.join(values_path, hashlib.md5(member).hexdigest())
            if not os.path.exists(value_path):
                continue
            result += 1
            score = Path(value_path).read().strip()
            score_path = os.path.join(scores_path, score)
            self._remove_line_from_file(score_path, member)
            Path(value_path).delete()
        # empty zset should be removed from keyspace
        if self._empty_directory(values_path):
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
        scores_path = os.path.join(key_path, 'scores')
        if os.path.exists(scores_path):
            scores = sorted(os.listdir(scores_path), key=float)
            scores = [score for score in scores if self._range_check(min_score, max_score, float(score))]
            for score in scores:
                lines = sorted(Path(os.path.join(scores_path, score)).readlines())
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
        value_path = os.path.join(key_path, 'values', hashlib.md5(member).hexdigest())
        if os.path.exists(value_path):
            scores_path = os.path.join(key_path, 'scores')
            scores = sorted(os.listdir(scores_path), key=float)
            member_score = Path(value_path).read().strip()
            rank = 0
            for score in scores:
                score_path = os.path.join(scores_path, str(score))
                lines = Path(score_path).readlines()  # FIXME: move this to be inside the `if` block
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
        if os.path.exists(key_path):
            type_path = os.path.join(key_path, 'type')
            return Path(type_path).read()
        else:
            return 'none'

    def keys(self, pattern):
        all_keys = os.listdir(self.directory)
        return list(fnmatch.filter(all_keys, pattern))

    def exists(self, *keys):
        result = 0
        for key in keys:
            key_path = self._key_path(key)
            if os.path.exists(key_path):
                result += 1
        return result

    def hset(self, key, field, value):
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        if not self.exists(key):
            os.makedirs(fields_path)
        field_path = os.path.join(fields_path, field)
        if os.path.exists(field_path):
            result = 0
        else:
            result = 1
        Path(field_path).write(value)
        self.write_type(key, 'hash')
        return result

    def hsetnx(self, key, field, value):
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        if not self.exists(key):
            os.makedirs(fields_path)
        field_path = os.path.join(fields_path, field)
        result = 0
        # only set if not set before
        if not os.path.exists(field_path):
            result = 1
            Path(field_path).write(value)
        self.write_type(key, 'hash')
        return result

    def hdel(self, key, *fields):
        result = 0
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        for field in fields:
            field_path = os.path.join(fields_path, field)
            if os.path.exists(field_path):
                Path(field_path).delete()
                result += 1
        # remove empty hashes from keyspace
        if self._empty_directory(fields_path):
            self.delete(key)
        return result

    def hget(self, key, field):
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        field_path = os.path.join(fields_path, field)
        if os.path.exists(field_path):
            result = Path(field_path).read()
        else:
            result = None
        return result

    def hkeys(self, key):
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        if os.path.exists(fields_path):
            result = os.listdir(fields_path)
        else:
            result = []
        return result

    def hvals(self, key):
        result = []
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        if os.path.exists(fields_path):
            for field in os.listdir(fields_path):
                field_path = os.path.join(fields_path, field)
                value = Path(field_path).read()
                result.append(value)
        return result

    def hlen(self, key):
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        result = 0
        if os.path.exists(fields_path):
            result = len(os.listdir(fields_path))
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

    def _empty_directory(self, path):
        return os.path.exists(path) and not os.listdir(path)

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
