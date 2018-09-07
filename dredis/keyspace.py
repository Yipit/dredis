import collections
import hashlib
import re

import sys

from dredis.lua import LuaRunner
from dredis.path import Path

import sqlite3

db_conn = sqlite3.connect('experiment.db')

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
        Path('experiment.db').write('')
        global db_conn
        db_conn = sqlite3.connect('experiment.db')

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
        c = db_conn.cursor()
        try:
            c.execute('''
                select value from {table}
            '''.format(table=key))
        except sqlite3.OperationalError:
            return

        result = c.fetchone()
        c.close()
        return result[0]

    def set(self, key, value):
        key_path = self._key_path(key)
        if not self.exists(key):
            key_path.makedirs()
            self.write_type(key, 'string')
        key_path.join('value').write(value)
        c = db_conn.cursor()
        c.execute('create table if not exists {table} (value blob)'.format(table=key))
        c.execute("insert into {table} values('{value}')".format(table=key, value=value))
        db_conn.commit()
        c.close()


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
        c = db_conn.cursor()
        result = 0
        for key in keys:
            try:
                c.execute('drop table {table}'.format(table=key))
            except sqlite3.OperationalError:
                pass
            else:
                result += 1
            db_conn.commit()
        return result

    def zadd(self, key, score, value):
        c = db_conn.cursor()
        c.execute('''
        create table if not exists {table} (score real, value text primary key)
        '''.format(table=key))
        result = c.execute('''
            insert into {table}(score, value) values({score}, '{value}')
                on conflict(value) do UPDATE SET score={score} where score != {score}
            '''.format(
                table=key,
                score=float(score),
                value=value))
        db_conn.commit()
        c.close()
        return result.rowcount

    def write_type(self, key, name):
        key_path = self._key_path(key)
        type_path = key_path.join('type')
        type_path.write(name)

    def zrange(self, key, start, stop, with_scores):
        c = db_conn.cursor()
        try:
            if with_scores:
                c.execute('''select value, score from {table} ORDER BY (score)'''.format(table=key))
            else:
                c.execute('''select value from {table} ORDER BY (score)'''.format(table=key))
        except sqlite3.OperationalError:
            return []

        rows = c.fetchall()
        if stop < 0:
            stop = -stop
        elif stop > len(rows):
            stop = -1
        end = len(rows) - stop + 1
        result = []
        for cols in rows[start:end]:
            for col in cols:
                result.append(str(col))
        return result

    def zcard(self, key):
        c = db_conn.cursor()
        try:
            c.execute('''select count(*) from {table}'''.format(table=key))
        except sqlite3.OperationalError:
            return 0
        else:
            result = c.fetchone()
            return result[0]

    def zscore(self, key, member):
        c = db_conn.cursor()
        try:
            c.execute('''select score from {table} where value = "{value}"'''.format(
                table=key,
                value=member,
            ))
        except sqlite3.OperationalError:
            return None
        else:
            result = c.fetchone()
            if result:
                if int(result[0]) == result[0]:
                    return str(int(result[0]))
                else:
                    return str(result[0])
            else:
                return None

    def zrem(self, key, *members):
        c = db_conn.cursor()
        try:
            result = c.execute('''delete from {table} where value in ({members})'''.format(
                table=key,
                members=','.join(map(repr, members))
            ))
        except sqlite3.OperationalError:
            return 0
        else:
            db_conn.commit()
            return result.rowcount

    def zrangebyscore(self, key, min_score, max_score, withscores=False, offset=0, count=float('+inf')):
        range_clause = ScoreRange(min_score, max_score).sql('score')
        c = db_conn.cursor()
        try:
            if withscores:
                c.execute('''select value, score from {table} WHERE {range_clause} ORDER BY (score)'''.format(
                    table=key,
                    range_clause=range_clause,
                ))
            else:
                c.execute('''select value from {table} WHERE {range_clause} ORDER BY (score)'''.format(
                    table=key,
                    range_clause=range_clause,
                ))
        except sqlite3.OperationalError:
            return []

        result = []
        for row in c.fetchall():
            for col in row:
                result.append(str(col))

        if withscores:
            offset = offset * 2
            count = count * 2

        if count == float('+inf'):
            count = len(result)
        return result[offset:][:count]

    def zcount(self, key, min_score, max_score):
        return len(self.zrangebyscore(key, min_score, max_score))

    def zrank(self, key, member):
        c = db_conn.cursor()
        try:
            c.execute('''select count(t.score) from {table} as t inner JOIN (
                select score, value from {table} where value = '{value}') as r ON (t.score <= r.score) where t.score < r.score OR t.value <= r.value ORDER BY (t.score)
            '''.format(
                table=key,
                value=member,
            ))  # will need to subtract 1 from the count to have the real rank
        except sqlite3.OperationalError:
            return None

        rank = c.fetchone()[0]
        if rank:
            # the SQL count returns 0 if not found, 1 or greater when the item is found, thus -1 in the next line
            return rank - 1
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

    def eval(self, script, keys, argv):
        runtime = LuaRunner(self)
        return runtime.run(script, keys, argv)

    def type(self, key):
        key_path = self._key_path(key)
        if key_path.exists():
            type_path = key_path.join('type')
            return type_path.read()
        else:
            return 'none'

    def keys(self, pattern):
        return self.directory.listdir(pattern)

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


class ScoreRange(object):

    def __init__(self, min_value, max_value):
        if '(' in min_value:
            self._min_exclusive = True
            self._min_value = min_value[1:]
        else:
            self._min_exclusive = False
            self._min_value = min_value
        if '(' in max_value:
            self._max_exclusive = True
            self._max_value = max_value[1:]
        else:
            self._max_exclusive = False
            self._max_value = max_value
        self._convert_infinity_to_number()

    def _convert_infinity_to_number(self):
        if self._min_value == '-inf':
            self._min_value = -sys.maxint
        elif self._min_value == '+inf':
            self._min_value = sys.maxint

        if self._max_value == '+inf':
            self._max_value = sys.maxint
        elif self._max_value == '-inf':
            self._max_value = -sys.maxint

    def check(self, value):
        if self._min_value.startswith('('):
            if float(self._min_value[1:]) >= value:
                return False
        elif float(self._min_value) > value:
            return False

        if self._max_value.startswith('('):
            if float(self._max_value[1:]) <= value:
                return False
        elif float(self._max_value) < value:
            return False

        return True

    def sql(self, variable):
        min_clause = '<' if self._min_exclusive else '<='
        max_clause = '<' if self._max_exclusive else '<='
        result = '({min_value} {min_clause} {variable} AND {variable} {max_clause} {max_value})'.format(
            min_value=self._min_value,
            min_clause=min_clause,
            max_value=self._max_value,
            max_clause=max_clause,
            variable=variable,
        )
        return result