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


def test_parser_should_request_more_data_if_needed():
    responses = [
        "*1\r\n$4\r\n",
        "PING\r\n"
    ]

    def read(bufsize):
        return responses.pop(0)

    p = Parser(read)
    assert list(p.get_instructions()) == [['PING']]
