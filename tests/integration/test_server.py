import glob
import os.path

import pytest
import redis

from tests.helpers import fresh_redis


def test_flushall():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.flushall() is True

    assert r0.keys('*') == []
    assert r1.keys('*') == []


def test_flush_db():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.flushdb() is True

    assert r0.keys('*') == []
    assert r1.keys('*') == ['test2']


def test_ping():
    r = fresh_redis()

    assert r.execute_command('ping') == 'PONG'
    assert r.execute_command('ping', 'msg') == 'msg'


def test_dbsize():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    assert r0.dbsize() == 0
    assert r1.dbsize() == 0

    r0.set('test', 'value')
    assert r0.dbsize() == 1
    assert r1.dbsize() == 0


def test_save_creates_an_rdb_file():
    r = fresh_redis()
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # 2 directory levels up
    rdb_files_before = set(glob.glob(os.path.join(root_dir, 'dump*.rdb')))

    r.set('test', 'value')
    assert r.save()
    assert len(set(glob.glob(os.path.join(root_dir, 'dump*.rdb'))) - rdb_files_before) == 1


def test_config_help():
    r = fresh_redis()
    result = r.execute_command('CONFIG', 'HELP')

    # assert strings individually because redis has more help lines than dredis
    assert "GET <pattern> -- Return parameters matching the glob-like <pattern> and their values." in result
    assert "SET <parameter> <value> -- Set parameter to value." in result


def test_config_get_with_unknown_config():
    r = fresh_redis()

    assert r.config_get('foo') == {}


def test_config_get_with_wrong_number_of_arguments():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError) as exc:
        r.execute_command('CONFIG', 'GET', 'foo', 'bar', 'baz')

    assert str(exc.value) == "Unknown subcommand or wrong number of arguments for 'GET'. Try CONFIG HELP."


def test_config_set_with_unknown_config():
    r = fresh_redis()

    with pytest.raises(redis.ResponseError) as exc:
        r.config_set('foo', 'bar')

    assert str(exc.value) == "Unsupported CONFIG parameter: foo"


@pytest.mark.skipif(os.getenv('REALREDIS') == '1', reason="these options only exist in dredis")
def test_config_get():
    r = fresh_redis()

    assert sorted(r.config_get('*').keys()) == sorted(['debug', 'readonly', 'requirepass'])
    assert r.config_get('*deb*').keys() == ['debug']


@pytest.mark.skipif(os.getenv('REALREDIS') == '1', reason="these options only exist in dredis")
def test_config_set():
    r = fresh_redis()
    original_value = r.config_get('debug')['debug']

    try:
        assert r.config_set('debug', 'false')
        assert r.config_get('debug') == {'debug': 'false'}

        assert r.config_set('debug', 'true')
        assert r.config_get('debug') == {'debug': 'true'}
    finally:
        # undo it to not affect other tests
        assert r.config_set('debug', original_value)
