from server3 import parse_instructions


def test_parse_simple_string():
    assert parse_instructions("+PING\r\n") == ['PING']


def test_simple_array():
    assert parse_instructions("*1\r\n$4\r\nPING\r\n") == ['PING']


def test_bulk_string_inside_array():
    assert parse_instructions("\
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
testvalue\r\n") == [
        'EVAL',
        '''redis.call('set', KEYS[1], KEYS[2])\nreturn redis.call('get', KEYS[1])''',
        '2',
        'testkey',
        'testvalue'
    ]
