from tests.helpers import fresh_redis


def test_sadd():
    r = fresh_redis()
    assert r.sadd('myset', 'myvalue1') == 1
    assert r.sadd('myset', 'myvalue1') == 0
    assert r.sadd('myset', 'myvalue2') == 1


def test_sismember():
    r = fresh_redis()
    r.sadd('myset', 'myvalue1')
    r.sadd('myset', 'myvalue2')

    assert r.sismember('myset', 'myvalue1') is True
    assert r.sismember('myset', 'myvalue2') is True
    assert r.sismember('myset', 'myvalue3') is False


def test_smembers():
    r = fresh_redis()
    r.sadd('myset', 'myvalue1')
    r.sadd('myset', 'myvalue2')

    assert r.smembers('myset') == {'myvalue1', 'myvalue2'}


def test_scard():
    r = fresh_redis()
    r.sadd('myset', 'myvalue1')
    r.sadd('myset', 'myvalue2')

    assert r.scard('myset') == 2
