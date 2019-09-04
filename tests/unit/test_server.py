from dredis import config
from dredis.server import transmit, transform
import mock


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
    assert transform(Exception('test')) == '-INTERNALERROR test\r\n'


def test_config():
    original_value = config.get('debug')
    try:
        # change config
        config.set('debug', 'false')
        assert config.get('debug') == 'false'

        config.set('debug', 'true')
        assert config.get('debug') == 'true'
    finally:
        # undo it to not affect other tests
        config.set('debug', original_value)
