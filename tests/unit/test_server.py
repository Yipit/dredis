from dredis.server import transmit
import mock


def test_transmit_integer():
    mock_function = mock.Mock()
    transmit(mock_function, 1)
    mock_function.assert_called_with(':1\r\n')


def test_transmit_bulk_string():
    mock_function = mock.Mock()
    transmit(mock_function, "test")
    mock_function.assert_called_with('$4\r\ntest\r\n')


def test_transmit_nil():
    mock_function = mock.Mock()
    transmit(mock_function, None)
    mock_function.assert_called_with('$-1\r\n')


def test_transmit_simple_array():
    mock_function = mock.Mock()
    transmit(mock_function, ['1', '2'])
    mock_function.assert_called_with('*2\r\n$1\r\n1\r\n$1\r\n2\r\n')


def test_transmit_mixed_array():
    mock_function = mock.Mock()
    transmit(mock_function, ['1', 2, None])
    mock_function.assert_called_with('*3\r\n$1\r\n1\r\n:2\r\n$-1\r\n')


def test_transmit_nested_array():
    mock_function = mock.Mock()
    transmit(mock_function, ['1', 3, ['2']])
    mock_function.assert_called_with('*3\r\n$1\r\n1\r\n:3\r\n*1\r\n$1\r\n2\r\n')
