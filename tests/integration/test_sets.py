import redis
from tests.helpers import HOST, PORT


def test_sadd():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()
    assert r.sadd('myset', 'myvalue1') == 1
    assert r.sadd('myset', 'myvalue1') == 0
    assert r.sadd('myset', 'myvalue2') == 1


def test_sismember():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()
    r.sadd('myset', 'myvalue1')
    r.sadd('myset', 'myvalue2')

    assert r.sismember('myset', 'myvalue1') is True
    assert r.sismember('myset', 'myvalue2') is True
    assert r.sismember('myset', 'myvalue3') is False


def test_smembers():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()
    r.sadd('myset', 'myvalue1')
    r.sadd('myset', 'myvalue2')

    assert r.smembers('myset') == {'myvalue1', 'myvalue2'}


def test_scard():
    r = redis.StrictRedis(host=HOST, port=PORT)
    r.flushall()
    r.sadd('myset', 'myvalue1')
    r.sadd('myset', 'myvalue2')

    assert r.scard('myset') == 2
