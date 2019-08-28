import random

import pytest
import redis

from tests.helpers import fresh_redis


def test_zset_zadd_and_zcard():
    r = fresh_redis()
    assert r.zcard('myzset') == 0

    assert r.zadd('myzset', 0, 'myvalue1') == 1
    assert r.zcard('myzset') == 1

    assert r.zadd('myzset', 1, 'myvalue2') == 1
    assert r.zcard('myzset') == 2

    assert r.zadd('myzset', 0, 'myvalue1') == 0  # not changed


def test_zadd_with_multiple_parameters():
    r = fresh_redis()
    assert r.zadd('myzset', 0, 'myvalue1', 1, 'myvalue2', 2, 'myvalue3') == 3
    assert r.zcard('myzset') == 3


def test_zset_zrange_with_positive_indexes():
    r = fresh_redis()
    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    assert r.zrange('myzset', 0, 1) == ['myvalue1', 'myvalue2']
    assert r.zrange('myzset', 0, 100) == ['myvalue1', 'myvalue2']


def test_zset_zrange_with_negative_indexes():
    r = fresh_redis()
    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    assert r.zrange('myzset', 0, -1) == ['myvalue1', 'myvalue2']
    assert r.zrange('myzset', 0, -2) == ['myvalue1']
    assert r.zrange('myzset', 0, -3) == []

    assert r.zrange('myzset', -2, 1) == ['myvalue1', 'myvalue2']


def test_redis_official_zset_tests_for_zrange():
    # adapted from TCL to Python. original source:
    # https://github.com/antirez/redis/blob/cb51bb4320d2240001e8fc4a522d59fb28259703/tests/unit/type/zset.tcl#L191-L219

    r = fresh_redis()
    r.flushall()
    r.zadd('ztmp', 1, 'a')
    r.zadd('ztmp', 2, 'b')
    r.zadd('ztmp', 3, 'c')
    r.zadd('ztmp', 4, 'd')

    assert r.zrange('ztmp', 0, -1) == ['a', 'b', 'c', 'd']
    assert r.zrange('ztmp', 0, -2) == ['a', 'b', 'c']
    assert r.zrange('ztmp', 1, -1) == ['b', 'c', 'd']
    assert r.zrange('ztmp', 1, -2) == ['b', 'c']
    assert r.zrange('ztmp', -2, -1) == ['c', 'd']
    assert r.zrange('ztmp', -2, -2) == ['c']

    # out of range start index
    assert r.zrange('ztmp', -5, 2) == ['a', 'b', 'c']
    assert r.zrange('ztmp', -5, 1) == ['a', 'b']
    assert r.zrange('ztmp', 5, -1) == []
    assert r.zrange('ztmp', 5, -2) == []

    # out of range end index
    assert r.zrange('ztmp', 0, 5) == ['a', 'b', 'c', 'd']
    assert r.zrange('ztmp', 1, 5) == ['b', 'c', 'd']
    assert r.zrange('ztmp', 0, -5) == []
    assert r.zrange('ztmp', 1, -5) == []

    # withscores
    assert r.zrange('ztmp', 0, -1, withscores=True) == [('a', 1), ('b', 2), ('c', 3), ('d', 4)]


def test_zset_zrange_with_scores():
    r = fresh_redis()
    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    assert r.zrange('myzset', 0, 1, withscores=True) == [('myvalue1', 0), ('myvalue2', 1)]


def test_zset_with_rescoring():
    r = fresh_redis()
    assert r.zadd('myzset', 0, 'myvalue1') == 1
    assert r.zadd('myzset', 1, 'myvalue2') == 1
    assert r.zadd('myzset', 0, 'myvalue2') == 0  # now the score 0 has "myvalue1" & "myvalue2"
    assert r.zcard('myzset') == 2
    assert r.zrange('myzset', 0, -1, withscores=True) == [('myvalue1', 0), ('myvalue2', 0)]


def test_zrem():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue0')
    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    r.zadd('myzset', 0, 'myvalue3')

    assert r.zrem('myzset', 'myvalue1') == 1
    assert r.zrem('myzset', 'notfound') == 0

    assert r.zrange('myzset', 0, -1) == ['myvalue0', 'myvalue3', 'myvalue2']
    assert r.zcard('myzset') == 3


def test_zscore():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue1')
    r.zadd('myzset', 1, 'myvalue2')
    r.zadd('myzset', 2.0, 'decimal1')
    r.zadd('myzset', 2.3, 'decimal2')

    assert r.zscore('myzset', 'myvalue1') == 0
    assert r.zscore('myzset', 'myvalue2') == 1
    assert r.zscore('myzset', 'decimal1') == 2.0
    assert r.zscore('myzset', 'decimal2') == 2.3

    assert r.zscore('myzset', 'notfound') is None
    # the `redis-py` library converts `zscore` results to float automatically,
    # and the following tests want to confirm the raw response from Redis
    assert r.eval("return redis.call('zscore', 'myzset', 'decimal1')", 0) == '2'


def test_zrangebyscore():
    r = fresh_redis()

    r.zadd('myzset', 0, 'myvalue0')
    r.zadd('myzset', 100, 'myvalue1')
    r.zadd('myzset', 200, 'myvalue2')
    r.zadd('myzset', 300, 'myvalue3')

    assert r.zrangebyscore('myzset', 100, 200) == ['myvalue1', 'myvalue2']
    assert r.zrangebyscore('myzset', '(100', 200) == ['myvalue2']
    assert r.zrangebyscore('myzset', '(100', '(300') == ['myvalue2']


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

    assert r.zrangebyscore('myzset', 0, 400, start=2, num=2, withscores=True) == [('myvalue2', 200), ('myvalue3', 300)]


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
    assert r.zrange('myzset', 0, 10) == ['zero', 'one', 'two', 'three']

    # redis accepts empty string and converts it to 0
    assert r.zrangebyscore('myzset', '', 1, withscores=True) == [('zero', 0.2), ('one', 0.7)]


def test_zset_rescore_when_zero_is_decimal_point():
    r = fresh_redis()

    assert r.zadd('myzset', 1.0, 'a') == 1
    assert r.zadd('myzset', 1, 'a') == 0  # nothing changed


def test_zcount():
    r = fresh_redis()

    r.zadd('myzset', 1, 'a')
    r.zadd('myzset', 2, 'b')

    assert r.zcount('myzset', 0, 10) == 2


def test_zuinionstore():
    # doesn't support AGGREGATE at the moment
    '''ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]'''
    r = fresh_redis()

    r.zadd('myzset1', 0, 'myvalue1')
    r.zadd('myzset1', 1, 'common')

    r.zadd('myzset2', 2, 'myvalue2')
    r.zadd('myzset2', 3, 'common')

    r.zadd('myzset3', 4, 'myvalue3')
    r.zadd('myzset3', 5, 'common')

    factor1 = 1
    factor2 = 2
    factor3 = 3

    assert r.zunionstore('result1', {'myzset1': factor1, 'myzset2': factor2, 'myzset3': factor3}) == 4
    assert r.zrange('result1', 0, -1, withscores=True) == [
        ('myvalue1', 0 * factor1),
        ('myvalue2', 2 * factor2),
        ('myvalue3', 4 * factor3),
        ('common', 1 * factor1 + 3 * factor2 + 5 * factor3),
    ]

    assert r.zunionstore('result2', ['myzset1', 'myzset2', 'myzset3']) == 4
    assert r.zrange('result2', 0, -1, withscores=True) == [
        ('myvalue1', 0),
        ('myvalue2', 2),
        ('myvalue3', 4),
        ('common', 9),
    ]


def test_empty_zset_after_zrem_should_be_removed_from_keyspace():
    r = fresh_redis()

    r.zadd('myzset1', 0, 'myvalue1')
    r.zrem('myzset1', 'myvalue1')

    assert r.keys() == []


def test_zadd_should_only_accept_pairs():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError) as exc:
        r.execute_command('ZADD', 'mykey', 0, 'myvalue1', 1)
    assert str(exc.value) == "syntax error"


def test_zrange_should_only_accept_withscores_as_extra_argument():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError) as exc:
        r.execute_command('ZRANGE', 'mykey', 0, 1, 'bleh')
    assert str(exc.value) == "syntax error"
    assert r.execute_command('ZRANGE', 'mykey', 0, 1, 'WITHSCORES') == []


def test_zrangebyscore_should_validate_withscores_and_limit_extra_arguments():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError) as exc1:
        r.execute_command('ZRANGEBYSCORE', 'mykey', 0, 1, 'bleh', 'WITHSCORES', 'LIMIT', 0)  # missing count
    assert str(exc1.value) == "syntax error"

    with pytest.raises(redis.ResponseError) as exc2:
        r.execute_command('ZRANGEBYSCORE', 'mykey', 0, 1, 'bleh', 'extraword')  # unknown parameter
    assert str(exc2.value) == "syntax error"


def test_zrangebyscore_should_validate_limit_values_as_integers():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError) as exc1:
        r.execute_command('ZRANGEBYSCORE', 'mykey', 0, 1, 'bleh',  'LIMIT', 0, 's')
    assert str(exc1.value) == "syntax error"

    with pytest.raises(redis.ResponseError) as exc2:
        r.execute_command('ZRANGEBYSCORE', 'mykey', 0, 1, 'bleh',  'LIMIT', 's', 1)
    assert str(exc2.value) == "syntax error"


def test_zrangebyscore_with_limit_from_official_redis_tests():
    # adapted from:
    # https://github.com/antirez/redis/blob/cb51bb4320d2240001e8fc4a522d59fb28259703/tests/unit/type/zset.tcl#L361-L371
    r = fresh_redis()

    r.zadd('zset', float('-inf'), 'a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', float('+inf'), 'g')

    assert r.zrangebyscore('zset', 0, 10, start=0, num=2) == ['b', 'c']
    assert r.zrangebyscore('zset', 0, 10, start=2, num=3) == ['d', 'e', 'f']
    assert r.zrangebyscore('zset', 0, 10, start=2, num=10) == ['d', 'e', 'f']
    assert r.zrangebyscore('zset', 0, 10, start=20, num=10) == []


def test_invalid_floats():
    r = fresh_redis()
    r.zadd('myzset', 0, 'test')

    with pytest.raises(redis.ResponseError) as exc1:
        r.zrangebyscore('myzset', 'invalid', 0)
    assert str(exc1.value) == 'min or max is not a float'

    with pytest.raises(redis.ResponseError) as exc2:
        r.zrangebyscore('myzset', 0, 'invalid')
    assert str(exc2.value) == 'min or max is not a float'

    with pytest.raises(redis.ResponseError) as exc3:
        r.zrangebyscore('myzset', 0, 'NaN')
    assert str(exc3.value) == 'min or max is not a float'

    with pytest.raises(redis.ResponseError) as exc4:
        r.zrangebyscore('myzset', 'NaN', 0)
    assert str(exc4.value) == 'min or max is not a float'


def test_zadd_with_newlines():
    r = fresh_redis()
    r.zadd('myzset', 0, 'my\ntest\nstring')
    r.zadd('myzset', 0, 'my\nsecond\nstring')

    assert r.zcard('myzset') == 2
    assert r.zrange('myzset', 0, 1) == ['my\nsecond\nstring', 'my\ntest\nstring']


def test_deleting_a_zset_should_not_impact_other_zsets():
    # this is a regression test
    r = fresh_redis()
    r.zadd('myzset1', 0, 'test1')
    r.zadd('myzset2', 0, 'test2')

    r.delete('myzset1')

    assert r.keys('*') == ['myzset2']
    assert r.zrange('myzset2', 0, 10) == ['test2']


def test_order_of_zrange_with_negative_scores():
    r = fresh_redis()

    pairs = [
        ('test1', -2.5),
        ('test2', -1.1),
        ('test3', 0.0),
        ('test4', 1.2),
        ('test5', 3.5),
    ]
    for member, score in pairs:
        r.zadd('myzset', score, member)
    assert r.zrange('myzset', 0, -1, withscores=True) == pairs


def test_zscan_with_all_elements_returned():
    r = fresh_redis()

    pairs = [
        ('test1', 1),
        ('test2', 2),
        ('test3', 3),
        ('test4', 4),
        ('test5', 5),
    ]
    random.shuffle(pairs)
    for member, score in pairs:
        r.zadd('myzset', score, member)

    cursor, elements = r.zscan('myzset', 0)
    assert cursor == 0
    assert sorted(elements) == sorted(pairs)


def test_zscan_with_a_subset_of_elements_returned():
    r = fresh_redis()

    # adding 200 elements to prevent real Redis from using a compact data structure
    # and returning all elements regardless of `COUNT`
    pairs = [('test{}'.format(i), i) for i in range(200)]

    random.shuffle(pairs)
    for member, score in pairs:
        r.zadd('myzset', score, member)

    cursor1, elems1 = r.zscan('myzset', 0, count=len(pairs) + 100)
    assert cursor1 == 0
    assert sorted(elems1) == sorted(pairs)

    cursor2, elems2 = r.zscan('myzset', 0, count=2)
    assert cursor2 != 0
    # Redis doesn't guarantee the order of the returned elements
    for e in elems2:
        assert e in pairs

    found_elems = []
    cursor3 = 0
    while True:
        cursor3, elems3 = r.zscan('myzset', cursor3, count=1)
        found_elems.extend(elems3)
        if cursor3 == 0:
            break
    assert sorted(pairs) == sorted(found_elems)


def test_zscan_with_a_subset_of_matching_elements_returned():
    r = fresh_redis()

    pairs = [
        ('a-test1', 1),
        ('b-test2', 2),
        ('a-test3', 3),
        ('a-test4', 4),
        ('b-test5', 5),
    ]
    # adding 200 elements to prevent real Redis from using a compact data structure
    # and returning all elements regardless of `COUNT`
    pairs.extend([('c-test{}'.format(i), i) for i in range(6, 200)])
    random.shuffle(pairs)
    for member, score in pairs:
        r.zadd('myzset', score, member)

    # MATCH is applied just before the elements are returned to the client, which means that you may need
    # multiple iterations to find a matching subset of elements
    cursor1 = 0
    matching_elems_found = []
    while True:
        cursor1, elems1 = r.zscan('myzset', cursor1, match='a-*', count=2)
        assert len(elems1) <= 2
        matching_elems_found.extend(elems1)
        if cursor1 == 0:
            break
    assert sorted(matching_elems_found) == sorted([
        ('a-test1', 1),
        ('a-test3', 3),
        ('a-test4', 4),
    ])


def test_zscan_invalid_cursor():
    r = fresh_redis()
    with pytest.raises(redis.ResponseError) as exc:
        r.zscan('myzset', 'a1')
    assert str(exc.value) == "invalid cursor"


def test_zscan_invalid_count():
    r = fresh_redis()
    r.zadd('myzset', 0, 'test')
    with pytest.raises(redis.ResponseError) as exc:
        r.zscan('myzset', 0, count='a')
    assert str(exc.value) == "value is not an integer or out of range"


def test_zscan_with_a_cursor_that_doesnt_exist():
    r = fresh_redis()

    assert r.zscan('myzset', 123) == (0, [])
