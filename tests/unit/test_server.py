from dredis import rdb
from dredis.keyspace import Keyspace
from dredis.server import transmit, transform
import mock
import os.path

from tests.fixtures import reproduce_dump

FIXTURE_DUMP = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'fixtures',
    'dump.rdb',
)


def test_transmit_integer():
    mock_function = mock.Mock()
    transmit(mock_function, 1)
    mock_function.assert_called_with(':1\r\n')


def test_transform_integer():
    assert transform(1) == ':1\r\n'


def test_transform_bulk_string():
    assert transform("test") == '$4\r\ntest\r\n'


def test_transform_nil():
    assert transform(None) == '$-1\r\n'


def test_transform_simple_array():
    assert transform(['1', '2']) == '*2\r\n$1\r\n1\r\n$1\r\n2\r\n'


def test_transform_mixed_array():
    assert transform(['1', 2, None]) == '*3\r\n$1\r\n1\r\n:2\r\n$-1\r\n'


def test_transform_nested_array():
    assert transform(['1', 3, ['2']]) == '*3\r\n$1\r\n1\r\n:3\r\n*1\r\n$1\r\n2\r\n'


def test_transform_error():
    assert transform(Exception('test')) == '-ERR test\r\n'


def test_rdb_load(keyspace):
    rdb.load_rdb(keyspace, open(FIXTURE_DUMP, 'rb').read())

    new_keyspace = Keyspace()
    new_keyspace.select(1)
    reproduce_dump.run(new_keyspace)

    assert new_keyspace.keys('*') == keyspace.keys('*')

    for key in new_keyspace.keys('string_*'):
        assert new_keyspace.get(key) == keyspace.get(key)

    for key in new_keyspace.keys('set_*'):
        assert new_keyspace.smembers(key) == keyspace.smembers(key)

    for key in new_keyspace.keys('zset_*'):
        assert new_keyspace.zrange(key, 0, -1, with_scores=True) == keyspace.zrange(key, 0, -1, with_scores=True)

    for key in new_keyspace.keys('hash_*'):
        assert new_keyspace.hgetall(key) == keyspace.hgetall(key)
