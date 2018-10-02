"""
The following results should serve as reference
------

Results from 2018-09-26 on @htlbra's Macbook (LARGE_NUMBER == 1000):

$ make performance-server & make test-performance | grep time
ZADD time = 0.47492s
ZADD time = 0.71635s
ZCARD time = 0.00120s
ZRANK time = 0.00430s
ZCOUNT time = 0.00449s
ZRANGE time = 0.01667s


$ redis-server --port 6376 & make test-performance | grep time
ZADD time = 0.07227s
ZADD time = 0.07522s
ZCARD time = 0.00008s
ZRANK time = 0.00010s
ZCOUNT time = 0.00010s
ZRANGE time = 0.00379s

"""

import time

from tests.helpers import fresh_redis


PROFILE_PORT = 6376
LARGE_NUMBER = 1000


def test_zadd_same_score_all_elements():
    r = fresh_redis(port=PROFILE_PORT)
    before_zadd = time.time()
    for score in range(LARGE_NUMBER):
        assert r.zadd('myzset', 0, 'value{}'.format(score)) == 1
    after_zadd = time.time()
    print '\nZADD time = {:.5f}s'.format(after_zadd - before_zadd)


def test_zadd_rescore_same_element():
    r = fresh_redis(port=PROFILE_PORT)
    before_zadd = time.time()
    for score in range(LARGE_NUMBER):
        r.zadd('myzset', score, 'value')
    after_zadd = time.time()
    print '\nZADD time = {:.5f}s'.format(after_zadd - before_zadd)


def test_zcard():
    r = fresh_redis(port=PROFILE_PORT)
    for score in range(LARGE_NUMBER):
        assert r.zadd('myzset', 0, 'value{}'.format(score)) == 1
    before_zcard = time.time()
    assert r.zcard('myzset') == LARGE_NUMBER
    after_zcard = time.time()
    print '\nZCARD time = {:.5f}s'.format(after_zcard - before_zcard)


def test_zrank():
    r = fresh_redis(port=PROFILE_PORT)
    for score in range(LARGE_NUMBER):
        assert r.zadd('myzset', 0, 'value{}'.format(score)) == 1
    before_zrank = time.time()
    assert r.zrank('myzset', 'value{}'.format(LARGE_NUMBER - 1)) == LARGE_NUMBER - 1
    after_zrank = time.time()

    print '\nZRANK time = {:.5f}s'.format(after_zrank - before_zrank)


def test_zcount():
    r = fresh_redis(port=PROFILE_PORT)
    for score in range(LARGE_NUMBER):
        assert r.zadd('myzset', 0, 'value{}'.format(score)) == 1
    before_zcount = time.time()
    assert r.zcount('myzset', '-inf', '+inf') == LARGE_NUMBER
    after_zcount = time.time()

    print '\nZCOUNT time = {:.5f}s'.format(after_zcount - before_zcount)


def test_zrange():
    r = fresh_redis(port=PROFILE_PORT)
    for score in range(LARGE_NUMBER):
        assert r.zadd('myzset', 0, 'value{}'.format(score)) == 1
    before_zrange = time.time()
    assert len(r.zrange('myzset', 0, LARGE_NUMBER)) == LARGE_NUMBER
    after_zrange = time.time()

    print '\nZRANGE time = {:.5f}s'.format(after_zrange - before_zrange)


def test_zrem():
    r = fresh_redis(port=PROFILE_PORT)
    elems = ['value{}'.format(i) for i in range(LARGE_NUMBER)]

    for elem in elems:
        assert r.zadd('myzset', 0, elem) == 1

    before_zrem = time.time()
    for elem in elems:
        assert r.zrem('myzset', elem) == 1
    after_zrem = time.time()

    print '\nZREM time = {:.5f}s'.format(after_zrem - before_zrem)
