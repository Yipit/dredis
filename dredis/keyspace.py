import collections
import fnmatch
import hashlib
import re

import sys

from dredis.lua import LuaRunner
from dredis.path import Path

import sqlite3


DECIMAL_REGEX = re.compile('(\d+)\.0+$')


class DiskKeyspace(object):

    _REDIS_TYPES = ['string', 'hash', 'set', 'zset']

    def __init__(self, root_dir):
        self._root_directory = Path(root_dir)
        default_db = '0'
        self._set_db_conn(default_db)

    def _set_db_conn(self, db):
        self._db_conn = sqlite3.connect('experiment-{}.db'.format(db))

    def flushall(self):
        for db_id in range(15):
            db_conn = sqlite3.connect('experiment-{}.db'.format(db_id))
            db_conn.execute('''PRAGMA writable_schema = 1''')
            db_conn.execute('''delete from sqlite_master where type in ('table', 'index', 'trigger')''')
            db_conn.execute('''PRAGMA writable_schema = 0''')
            db_conn.commit()

    def flushdb(self):
        self._db_conn.execute('''PRAGMA writable_schema = 1''')
        self._db_conn.execute('''delete from sqlite_master where type in ('table', 'index', 'trigger')''')
        self._db_conn.execute('''PRAGMA writable_schema = 0''')
        self._db_conn.commit()

    def select(self, db):
        self._set_db_conn(db)

    def incrby(self, key, increment=1):
        c = self._db_conn.cursor()
        stored_value = self.get(key)
        if stored_value:
            c.execute("""
                UPDATE string_{table} SET value = coalesce((select cast(value as decimal) + {increment} from string_{table}), {increment})
            """.format(
                table=key,
                increment=increment,
            ))
        else:
            self._create_string_table(key, c)
            c.execute("""
                INSERT INTO string_{table} VALUES ({increment})
            """.format(
                table=key,
                increment=increment,
            ))
        self._db_conn.commit()
        c.execute('select value from string_{table}'.format(table=key))
        result = c.fetchone()[0]
        c.close()

        return int(result)

    def get(self, key):
        c = self._db_conn.cursor()
        try:
            c.execute('''
                select value from string_{table}
            '''.format(table=key))
        except sqlite3.OperationalError:
            return

        result = c.fetchone()
        c.close()
        return result[0]

    def set(self, key, value):
        c = self._db_conn.cursor()
        self._create_string_table(key, c)
        c.execute("insert into string_{table} values('{value}') on conflict(value) do UPDATE SET value='{value}'".format(table=key, value=value))
        self._db_conn.commit()
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
        c = self._db_conn.cursor()
        self._create_set_table(c, key)
        c.execute("insert or ignore into set_{table} values('{value}')".format(table=key, value=value))
        self._db_conn.commit()
        result = c.rowcount
        c.close()
        return result

    def smembers(self, key):
        c = self._db_conn.cursor()
        try:
            c.execute('''select value from set_{table}'''.format(table=key))
        except sqlite3.OperationalError:
            return set()
        rows = c.fetchall()
        return set(row[0] for row in rows)

    def sismember(self, key, value):
        c = self._db_conn.cursor()
        try:
            c.execute('''select count(*) from set_{table} where value = "{value}" LIMIT 1'''.format(table=key, value=value))
        except sqlite3.OperationalError:
            return False
        result = c.fetchone()
        return result[0] > 0

    def scard(self, key):
        return len(self.smembers(key))

    def delete(self, *keys):
        c = self._db_conn.cursor()
        result = 0
        for key in keys:
            for redis_type in self._REDIS_TYPES:
                try:
                    c.execute('drop table {type}_{table}'.format(type=redis_type, table=key))
                except sqlite3.OperationalError:
                    pass
                else:
                    result += 1
            self._db_conn.commit()
        return result

    def zadd(self, key, score, value):
        c = self._db_conn.cursor()
        self._create_zset_table(key, c)
        result = c.execute('''
            insert into zset_{table}(score, value) values({score}, '{value}')
                on conflict(value) do UPDATE SET score={score} where score != {score}
            '''.format(
                table=key,
                score=float(score),
                value=value))
        self._db_conn.commit()
        c.close()
        return result.rowcount

    def write_type(self, key, name):
        key_path = self._key_path(key)
        type_path = key_path.join('type')
        type_path.write(name)

    def zrange(self, key, start, stop, with_scores):
        c = self._db_conn.cursor()
        try:
            if with_scores:
                c.execute('''select value, score from zset_{table} ORDER BY (score)'''.format(table=key))
            else:
                c.execute('''select value from zset_{table} ORDER BY (score)'''.format(table=key))
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
        c = self._db_conn.cursor()
        try:
            c.execute('''select count(*) from zset_{table}'''.format(table=key))
        except sqlite3.OperationalError:
            return 0
        else:
            result = c.fetchone()
            return result[0]

    def zscore(self, key, member):
        c = self._db_conn.cursor()
        try:
            c.execute('''select score from zset_{table} where value = "{value}"'''.format(
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
        c = self._db_conn.cursor()
        try:
            result = c.execute('''delete from zset_{table} where value in ({members})'''.format(
                table=key,
                members=','.join(map(repr, members))
            ))
        except sqlite3.OperationalError:
            return 0
        else:
            self._db_conn.commit()
            return result.rowcount

    def zrangebyscore(self, key, min_score, max_score, withscores=False, offset=0, count=float('+inf')):
        range_clause = ScoreRange(min_score, max_score).sql('score')
        c = self._db_conn.cursor()
        try:
            if withscores:
                c.execute('''select value, score from zset_{table} WHERE {range_clause} ORDER BY (score)'''.format(
                    table=key,
                    range_clause=range_clause,
                ))
            else:
                c.execute('''select value from zset_{table} WHERE {range_clause} ORDER BY (score)'''.format(
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
        c = self._db_conn.cursor()
        try:
            c.execute('''select count(t.score) from zset_{table} as t inner JOIN (
                select score, value from zset_{table} where value = '{value}') as r ON (t.score <= r.score) where t.score < r.score OR t.value <= r.value ORDER BY (t.score)
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
        c = self._db_conn.cursor()
        c.execute('select name from sqlite_master WHERE type = "table" AND name LIKE "%_{table}"'.format(
            table=key
        ))
        result = c.fetchone()
        if result:
            return result[0].split('_', 1)[0]
        else:
            return 'none'

    def keys(self, pattern):
        c = self._db_conn.cursor()
        c.execute('select name from sqlite_master WHERE type = "table"')
        tables = [row[0] for row in c.fetchall()]
        names = []
        for table in tables:
            c.execute('select count(*) from {table}'.format(table=table))
            if c.fetchone() != (0,):
                names.append(table.split('_', 1)[1])
        return fnmatch.filter(names, pattern)

    def exists(self, *keys):
        c = self._db_conn.cursor()
        possible_key_names = []
        for redis_type in self._REDIS_TYPES:
            for key in keys:
                possible_key_names.append('{}_{}'.format(redis_type, key))
        c.execute('select name from sqlite_master where type = "table" AND name in ({names})'.format(
            names=', '.join(map(repr, possible_key_names))))
        return len(c.fetchall())

    def hset(self, key, field, value):
        c = self._db_conn.cursor()
        self._create_hash_table(key, c)
        c.execute('select value from hash_{table} where field = "{field}"'.format(table=key, field=field))
        existing_value = c.fetchone()
        if existing_value == (value,):
            return 0
        else:
            c.execute("insert OR replace into hash_{table} values('{field}', '{value}')".format(table=key, field=field, value=value))
            self._db_conn.commit()
            result = c.rowcount
            c.close()
            return result

    def hsetnx(self, key, field, value):
        c = self._db_conn.cursor()
        self._create_hash_table(key, c)
        c.execute("insert OR IGNORE into hash_{table} values('{field}', '{value}')".format(table=key, field=field, value=value))
        self._db_conn.commit()
        result = c.rowcount
        c.close()
        return result

    def hdel(self, key, *fields):
        c = self._db_conn.cursor()
        try:
            c.execute("delete from hash_{table} where field IN ({fields})".format(table=key, fields=','.join(map(repr, fields))))
            self._db_conn.commit()
            result = c.rowcount
            c.close()
            return result
        except sqlite3.OperationalError:
            return 0

    def hget(self, key, field):
        c = self._db_conn.cursor()
        try:
            c.execute("select value from hash_{table} where field = '{field}' LIMIT 1".format(
                table=key, field=field))
            result = c.fetchone()
            if result:
                return result[0]
            else:
                return None
        except sqlite3.OperationalError:
            return None

    def hkeys(self, key):
        c = self._db_conn.cursor()
        try:
            c.execute("select field from hash_{table}".format(table=key))
            rows = c.fetchall()
            return [row[0] for row in rows]
        except sqlite3.OperationalError:
            return []

    def hvals(self, key):
        c = self._db_conn.cursor()
        try:
            c.execute("select value from hash_{table}".format(table=key))
            rows = c.fetchall()
            return [row[0] for row in rows]
        except sqlite3.OperationalError:
            return []

    def hlen(self, key):
        return len(self.hkeys(key))

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

    def _create_hash_table(self, key, cursor):
        cursor.execute(
            'create table if not exists hash_{table} (field TEXT PRIMARY KEY, value TEXT, CONSTRAINT fieldvalue_uniq UNIQUE (field, value))'.format(
                table=key))

    def _create_zset_table(self, key, c):
        c.execute('''create table if not exists zset_{table} (score real, value text primary key)'''.format(table=key))

    def _create_string_table(self, key, c):
        c.execute('create table if not exists string_{table} (value text primary key)'.format(table=key))

    def _create_set_table(self, c, key):
        c.execute('create table if not exists set_{table} (value TEXT primary key)'.format(table=key))



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