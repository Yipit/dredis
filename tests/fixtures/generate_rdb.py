"""
Generate a Redis dump (RDB) with misc data types and values.
The goal is to have a diverse RDB that can help with regression tests.

Run this with a compatible version of Redis (the file was generated with 3.2.6).
Run `redis-server` from the same directory as this file so you can get the `dump.rdb` file here.
"""
import random
import string

import redis


LARGE = 1000
SMALL = 50

MAX_INTEGER = 2 ** 16
MAX_SCORE = 2 ** 16
MAX_STRING_LENGTH = 1000


def random_string(max_length):
    return ''.join(random.choice(string.printable) for _ in range(int(random.random() * max_length)))


def random_sign():
    return random.choice([-1, 1])


def random_score():
    return random_sign() * random.random() * MAX_SCORE


def random_integer():
    return random_sign() * int(random.random() * MAX_INTEGER)


def random_value(max_length=MAX_STRING_LENGTH):
    return random.choice([
        # strings should happen more often
        random_string(max_length),
        random_string(max_length),
        random_string(max_length),

        random_integer(),
    ])


def add_strings(r):
    print("Adding strings...")
    for _ in xrange(SMALL):
        key = 'string_%s' % random_string(max_length=20)
        value = random_value()
        r.set(key, value)


def add_sets(r):
    print("Adding sets...")
    for _ in xrange(SMALL):
        r.sadd('set_small', random_value())
    for _ in xrange(LARGE):
        r.sadd('set_large', random_value())
    for _ in xrange(SMALL):
        r.sadd('intset', random_integer())


def add_zsets(r):
    print("Adding zsets...")
    for _ in xrange(SMALL):
        r.zadd('zset_small', random_score(), random_value())
    for _ in xrange(LARGE):
        r.zadd('zset_large', random_score(), random_value())


def add_hashes(r):
    print("Adding hashes...")
    for _ in xrange(SMALL):
        r.hset('hash_small', random_string(max_length=30), random_value(max_length=30))
    for _ in xrange(LARGE):
        r.hset('hash_large', random_string(max_length=30), random_value())


class Dumper(object):

    def __init__(self, dump_file, redis_conn):
        self._current_func_name = ''
        self._redis_conn = redis_conn
        self._dump_file = dump_file
        self._dump_file.write('def run(r):')

    def __getattr__(self, func_name):
        if self._current_func_name != func_name:
            self._dump_file.write('\n')
        self._current_func_name = func_name
        return self

    def __call__(self, key, *args):
        getattr(self._redis_conn, self._current_func_name)(key, *args)
        self._dump_file.write('    r.%s(%r, %s)\n' % (self._current_func_name, key, ','.join(map(repr, args))))

    def save(self):
        self._dump_file.close()
        self._redis_conn.save()


def main():
    redis_conn = redis.StrictRedis()
    redis_conn.flushall()

    dump_file = open('reproduce_dump.py', 'w')
    dumper = Dumper(dump_file, redis_conn)
    add_strings(dumper)
    add_sets(dumper)
    add_zsets(dumper)
    add_hashes(dumper)

    print("Saving..")
    dumper.save()
    print("Done.")


if __name__ == '__main__':
    main()
