from tests.helpers import fresh_redis


def test_types():
    r = fresh_redis()

    r.set('mystr', 'test')
    r.incr('myint')
    r.sadd('myset', 'test')
    r.zadd('myzset', 0, 'test')

    assert r.type('mystr') == 'string'
    assert r.type('myint') == 'string'
    assert r.type('myset') == 'set'
    assert r.type('myzset') == 'zset'


def test_keys():
    r = fresh_redis()

    r.set('mystr', 'test')
    r.incr('myint')
    r.sadd('myset', 'test')
    r.zadd('myzset', 0, 'test')

    assert r.keys('myi*') == ['myint']

    # order isn't guaranteed
    all_keys = r.keys('*')
    assert len(all_keys) == 4
    assert sorted(all_keys) == sorted(['mystr', 'myint', 'myset', 'myzset'])
    assert sorted(r.keys('my*set')) == sorted(['myset', 'myzset'])
    assert r.keys('my?et') == ['myset']


def test_exists():
    r = fresh_redis()

    r.set('mystr', 'test')
    assert r.exists('mystr') == 1
    assert r.exists('notfound') == 0

    # redis-py doesn't support multiple args to `r.exists()`
    assert r.execute_command('EXISTS', 'mystr', 'notfound') == 1
