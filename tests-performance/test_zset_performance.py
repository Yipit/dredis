import time

from tests.helpers import fresh_redis


PROFILE_PORT = 6376


def test_very_large_zset():
    r = fresh_redis(port=PROFILE_PORT)
    large_number = int(2 * 1e3)
    before_zadd = time.time()
    for score in range(large_number):
        assert r.zadd('myzset', 0, 'value{}'.format(score)) == 1
    after_zadd = time.time()
    before_zcard = time.time()
    assert r.zcard('myzset') == large_number
    after_zcard = time.time()

    print '\nZADD time = {}s'.format(after_zadd - before_zadd)
    print 'ZCARD time = {}s'.format(after_zcard - before_zcard)
