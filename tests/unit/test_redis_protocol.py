import pytest

from dredis.parser import Parser


def test_parse_simple_string():
    def read(n):
        return "+PING\r\n"

    p = Parser(read)
    assert list(p.get_instructions()) == [['PING']]


def test_simple_array():
    def read(n):
        return "*1\r\n$4\r\nPING\r\n"

    p = Parser(read)
    assert list(p.get_instructions()) == [['PING']]


def test_bulk_string_inside_array():
    def read(n):
        return "\
*5\r\n\
$4\r\n\
EVAL\r\n\
$69\r\n\
redis.call('set', KEYS[1], KEYS[2])\n\
return redis.call('get', KEYS[1])\r\n\
$1\r\n\
2\r\n\
$7\r\n\
testkey\r\n\
$9\r\n\
testvalue\r\n"

    p = Parser(read)
    assert list(p.get_instructions()) == [[
        'EVAL',
        '''redis.call('set', KEYS[1], KEYS[2])\nreturn redis.call('get', KEYS[1])''',
        '2',
        'testkey',
        'testvalue'
    ]]


def test_multiple_arrays():
    def read(n):
        return "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"

    p = Parser(read)
    assert list(p.get_instructions()) == [['PING'], ['PING']]


@pytest.mark.parametrize("line", ("*", "*1", "*1\r\n$4\r\n"))
def test_parser_should_ignore_half_sent_commands(line):
    def read(bufsize):
        return line

    p = Parser(read)
    assert list(p.get_instructions()) == []


def test_parser_should_work_with_chunks_sent_separately():
    responses = ["*1"]

    def read(bufsize):
        return responses.pop(0)

    p = Parser(read)

    with pytest.raises(StopIteration):
        next(p.get_instructions())

    responses.append("\r\n$4\r")
    with pytest.raises(StopIteration):
        next(p.get_instructions())

    responses.append("\nPIN")
    with pytest.raises(StopIteration):
        next(p.get_instructions())

    responses.append("G\r\n")
    assert next(p.get_instructions()) == ['PING']
