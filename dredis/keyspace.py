import collections
import fnmatch
import hashlib
import json
import os.path
import re
import shutil
import tempfile

from lupa import LuaRuntime

from dredis.commands import run_command, SimpleString


DECIMAL_REGEX = re.compile('(\d+)\.0+$')


class RedisScriptError(Exception):
    """Indicate error from calls to redis.call()"""


class DiskKeyspace(object):

    def __init__(self, root_dir):
        self._root_directory = root_dir
        default_db = '0'
        self._set_db_directory(default_db)

    def _set_db_directory(self, db):
        self.directory = os.path.join(self._root_directory, db)

    def _key_path(self, key):
        return os.path.join(self.directory, key)

    def flushall(self):
        try:
            shutil.rmtree(self._root_directory)
        except:
            pass

        for db_id in range(15):
            try:
                os.makedirs(os.path.join(self._root_directory, str(db_id)))
            except:
                pass

    def flushdb(self):
        try:
            shutil.rmtree(self.directory)
        except:
            pass
        try:
            os.makedirs(self.directory)
        except:
            pass

    def select(self, db):
        self._set_db_directory(db)

    def incrby(self, key, increment=1):
        key_path = self._key_path(key)
        value_path = os.path.join(key_path, 'value')
        number = 0
        if self.exists(key):
            with open(value_path, 'r') as f:
                content = f.read()
            number = int(content)
        else:
            os.makedirs(key_path)
            self.write_type(key, 'string')
        result = number + increment
        with open(value_path, 'w') as f:
            f.write(str(result))
        return result

    def get(self, key):
        key_path = self._key_path(key)
        value_path = os.path.join(key_path, 'value')
        if self.exists(key):
            with open(value_path, 'r') as f:
                return f.read()
        else:
            return None

    def set(self, key, value):
        key_path = self._key_path(key)
        if not self.exists(key):
            os.makedirs(key_path)
            self.write_type(key, 'string')
        value_path = os.path.join(key_path, 'value')
        with open(value_path, 'w') as f:
            f.write(value)

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
            with open(value_path, 'w') as f:
                f.write(value)
            return 1

    def smembers(self, key):
        result = set()
        if self.exists(key):
            key_path = self._key_path(key)
            values_path = os.path.join(key_path, 'values')
            for fname in os.listdir(values_path):
                with open(os.path.join(values_path, fname)) as f:
                    result.add(f.read())
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
                if os.path.isfile(key_path):
                    os.remove(self._key_path(key))
                else:
                    shutil.rmtree(key_path)
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
            with open(value_path, 'r') as fvalue:
                previous_score = fvalue.read()
            if previous_score == score:
                return 0
            else:
                previous_score_path = os.path.join(scores_path, previous_score)
                self._remove_line_from_file(previous_score_path, skip_line=value)

        with open(value_path, 'w') as f:
            f.write(score)
        with open(score_path, 'a') as f:
            f.write(value + '\n')
        return 1

    def write_type(self, key, name):
        key_path = self._key_path(key)
        type_path = os.path.join(key_path, 'type')
        with open(type_path, 'w') as f:
            f.write(name)

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
                with open(os.path.join(scores_path, score)) as f:
                    sublist = sorted(line.strip() for line in f.readlines())
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
            with open(value_path, 'r') as f:
                return f.read().strip()
        else:
            return None

    def eval(self, script, keys, argv):
        lua = LuaRuntime(unpack_returned_tuples=True)
        lua.execute('KEYS = {%s}' % ', '.join(map(json.dumps, keys)))
        lua.execute('ARGV = {%s}' % ', '.join(map(json.dumps, argv)))
        redis_obj = RedisLua(self, lua)
        redis_lua = lua.eval('function(redis) {} end'.format(script))
        result = redis_lua(redis_obj)
        return self._convert_lua_types_to_redis_types(result, type(lua.table()))

    def _convert_lua_types_to_redis_types(self, result, table_type):
        def convert(value):
            """
            str -> str
            true -> 1
            false -> None
            number -> int
            table -> {
                if 'err' key is present, raise an error
                else if 'ok' key is present, return its value
                else convert to a list using the previous rules
            }

            Reference:
            https://github.com/antirez/redis/blob/5b4bec9d336655889641b134791dfdd2adc864cf/src/scripting.c#L273-L340

            """
            if isinstance(value, table_type):
                if 'err' in value:
                    raise ValueError('ERR Error running script: {}'.format(value['err']))
                elif 'ok' in value:
                    return value['ok']
                else:
                    return map(convert, value.values())
            elif isinstance(value, (tuple, list, set)):
                return map(convert, value)
            elif value is True:
                return 1
            elif value is False:
                return None
            else:
                # assuming string at this point
                return value
        return convert(result)

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
            with open(value_path, 'r') as f:
                score = f.read().strip()
            score_path = os.path.join(scores_path, score)
            os.remove(value_path)
            self._remove_line_from_file(score_path, member)
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
            scores = [score for score in scores if min_score <= float(score) <= max_score]
            for score in scores:
                with open(os.path.join(scores_path, score)) as f:
                    lines = sorted(line.strip() for line in f.readlines())
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
            with open(value_path, 'r') as f:
                member_score = f.read().strip()
            rank = 0
            for score in scores:
                score_path = os.path.join(scores_path, str(score))
                with open(score_path, 'r') as f:
                    lines = f.readlines()
                if score == member_score:
                    for line in lines:
                        if line.strip() == member:
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
            with open(type_path, 'r') as f:
                return f.read()
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
            # self.write_type()
        field_path = os.path.join(fields_path, field)
        if os.path.exists(field_path):
            result = 0
        else:
            result = 1
        with open(field_path, 'w') as f:
            f.write(value)
        self.write_type(key, 'hash')
        return result

    def hsetnx(self, key, field, value):
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        if not self.exists(key):
            os.makedirs(fields_path)
            # self.write_type()
        field_path = os.path.join(fields_path, field)
        result = 0
        # only set if not set before
        if not os.path.exists(field_path):
            result = 1
            with open(field_path, 'w') as f:
                f.write(value)
        return result

    def hdel(self, key, *fields):
        result = 0
        key_path = self._key_path(key)
        fields_path = os.path.join(key_path, 'fields')
        for field in fields:
            field_path = os.path.join(fields_path, field)
            if os.path.exists(field_path):
                os.remove(field_path)
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
            with open(field_path, 'r') as f:
                result = f.read()
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
                with open(field_path, 'r') as f:
                    value = f.read()
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


class RedisLua(object):

    def __init__(self, keyspace, lua_runtime):
        self._keyspace = keyspace
        self._lua_runtime = lua_runtime

    def call(self, cmd, *args):
        try:
            result = run_command(self._keyspace, cmd, args)
        except KeyError:
            raise RedisScriptError('@user_script: Unknown Redis command called from Lua script')
        except Exception as exc:
            raise RedisScriptError(str(exc))
        else:
            return self._convert_redis_types_to_lua_types(result)

    def pcall(self, cmd, *args):
        try:
            return self.call(cmd, *args)
        except Exception as exc:
            table = self._lua_runtime.table()
            table['err'] = str(exc)
            return table

    def _convert_redis_types_to_lua_types(self, result):
        """
        Redis reply should be converted to the equivalent lua type
        The official implementation converts:
          * $-1 and *-1 to `false`
          * errors  to `{err=ERRORMSG}`
          * simple strings to `{ok=STRING}`
          * integers to numbers
          * arrays to lua tables following the previous conversions

        The implementation can be found at:
        https://github.com/antirez/redis/blob/5b4bec9d336655889641b134791dfdd2adc864cf/src/scripting.c#L106-L201
        """
        if isinstance(result, (tuple, list, set)):
            table = self._lua_runtime.table()
            for i, elem in enumerate(result, start=1):
                table[i] = self._convert_redis_types_to_lua_types(elem)
            return table
        elif result is None:
            return False
        elif result is True:
            return 1
        elif isinstance(result, SimpleString):
            table = self._lua_runtime.table()
            table["ok"] = result
            return table
        else:
            return result
