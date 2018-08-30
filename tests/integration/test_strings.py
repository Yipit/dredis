from tests.helpers import fresh_redis


def test_set_string():
    r = fresh_redis()

    assert r.set('foo', 'bar') is True
    assert r.set('foo', 'bar') is True


def test_get_string():
    r = fresh_redis()

    assert r.get('foo') is None
    r.set('foo', 'bar')
    assert r.get('foo') == 'bar'


def test_set_and_get_bytes():
    r = fresh_redis()

    assert r.set('foo', b'\x05\x02\x03') is True
    assert r.get('foo') == b'\x05\x02\x03'


def test_set_and_get_integer():
    r = fresh_redis()

    r.set('foo', 1)
    assert r.get('foo') == '1'


def test_incr_integers():
    r = fresh_redis()

    assert r.incr('a') == 1
    assert r.incr('a') == 2

    r.set('b', 10)
    assert r.incr('b') == 11
    assert r.incr('b') == 12


def test_delete():
    r = fresh_redis()

    r.set('test1', 'value1')
    r.set('test2', 'value2')

    assert r.delete('test1', 'test2', 'notfound') == 2
    assert r.get('test1') is None
    assert r.get('test2') is None
