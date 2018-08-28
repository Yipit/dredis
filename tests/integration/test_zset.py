import time

from tests.helpers import fresh_redis


def test_zset_zadd_and_zcard():
    r = fresh_redis()
    assert r.zcard('myzset') == 0

    assert r.zadd('myzset', 0, 'myvalue1') == 1
    assert r.zcard('myzset') == 1

    assert r.zadd('myzset', 1, 'myvalue2') == 1
    assert r.zcard('myzset') == 2

    assert r.zadd('myzset', 0, 'myvalue1') == 0  # not changed


def test_zset_zrange_with_positive_integers():
    r = fresh_redis()
    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    assert r.zrange('myzset', 0, 1) == ['myvalue1', 'myvalue2']


def test_zset_zrange_with_negative_numbers():
    r = fresh_redis()
    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    assert r.zrange('myzset', 0, -1) == ['myvalue1', 'myvalue2']
    assert r.zrange('myzset', 0, -2) == ['myvalue1']
    assert r.zrange('myzset', 0, -3) == []


def test_zset_with_rescoring():
    r = fresh_redis()
    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    r.zadd('myzset', 0, 'myvalue2')  # now the score 0 has "myvalue1" & "myvalue2"
    assert r.zcard('myzset') == 2
    assert r.zrange('myzset', 0, -1) == ['myvalue1', 'myvalue2']


def test_very_large_zset():
    r = fresh_redis()
    large_number = int(1e3)
    before_zadd = time.time()
    for score in range(large_number):
        r.zadd('myzset', 0, 'value{}'.format(score))
        # r.zadd('myzset', score, 'value{}'.format(score))
    after_zadd = time.time()
    before_zcard = time.time()
    # assert r.zcard('myzset') == large_number
    r.zcard('myzset')
    after_zcard = time.time()

    print '\nZADD time = {}s'.format(after_zadd - before_zadd)
    print 'ZCARD time = {}s'.format(after_zcard - before_zcard)


def test_zrem():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')

    assert r.zrem('myzset', 'myvalue1') == 1
    assert r.zrem('myzset', 'notfound') == 0

    assert r.zrange('myzset', 0, -1) == ['myvalue2']


def test_zscore():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')

    assert r.zscore('myzset', 'myvalue1') == 0
    assert r.zscore('myzset', 'myvalue2') == 1
    assert r.zscore('myzset', 'notfound') is None


def test_zrangebyscore():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue0')
    r.zadd('myzset', 100, 'myvalue1')
    r.zadd('myzset', 200, 'myvalue2')
    r.zadd('myzset', 300, 'myvalue3')

    assert r.zrangebyscore('myzset', 100, 200) == ['myvalue1', 'myvalue2']


def test_zrangebyscore_with_scores():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue0')
    r.zadd('myzset', 100, 'myvalue1')
    r.zadd('myzset', 200, 'myvalue2')
    r.zadd('myzset', 300, 'myvalue3')

    assert r.zrangebyscore('myzset', 100, 200, withscores=True) == [('myvalue1', 100), ('myvalue2', 200)]


def test_zrangebyscore_with_limit():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue0')
    r.zadd('myzset', 100, 'myvalue1')
    r.zadd('myzset', 200, 'myvalue2')
    r.zadd('myzset', 300, 'myvalue3')

    assert r.zrangebyscore('myzset', 0, 400, start=2, num=2, withscores=True) == [('myvalue1', 100), ('myvalue2', 200)]


def test_zrank():
    r = fresh_redis()

    r.zadd('myzset', 0, 'zero')
    r.zadd('myzset', 100, 'one')
    r.zadd('myzset', 200, 'two')
    r.zadd('myzset', 300, 'three')

    assert r.zrank('myzset', 'zero') == 0
    assert r.zrank('myzset', 'one') == 1
    assert r.zrank('myzset', 'two') == 2
    assert r.zrank('myzset', 'three') == 3
    assert r.zrank('myzset', 'notfound') is None


def test_zsets_should_support_floats_as_score_and_ranges():
    r = fresh_redis()

    r.zadd('myzset', 0.2, 'zero')
    r.zadd('myzset', 0.7, 'one')
    r.zadd('myzset', 1.3, 'two')
    r.zadd('myzset', 2.5, 'three')

    assert r.zrangebyscore('myzset', 0, 0.7, withscores=True) == [('zero', 0.2), ('one', 0.7)]
    assert r.zrangebyscore('myzset', '-inf', '+inf', withscores=True) == [
        ('zero', 0.2),
        ('one', 0.7),
        ('two', 1.3),
        ('three', 2.5),
    ]
    assert r.zrank('myzset', 'zero') == 0
