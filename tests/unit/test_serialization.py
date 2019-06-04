from dredis.keyspace import Keyspace


def test_serialize_not_found_key():
    k = Keyspace()
    assert k.dump('notfound') is None


def test_serialize_string():
    # dredis serializes strings verbatim,
    # it doesn't encode int strings differently than raw strings,
    # and doesn't use LZF compresssion

    str1 = 'test'
    str2 = 'a' * (1 << 6)
    str3 = 'a' * (1 << 14)

    k = Keyspace()
    k.set('str1', str1)
    k.set('str2', str2)
    k.set('str3', str3)

    assert k.dump('str1') == b'\x00\x04' + str1 + b'\x07\x00~\xa2zSd;e_'
    assert k.dump('str2') == b'\x00@@' + str2 + b'\x07\x00\xd2>\xaf>\x83X\xde\xe5'
    assert k.dump('str3') == b'\x00\x80\x00\x00@\x00' + str3 + b'\x07\x00\xe9\x9e\x16)r\x8c\xac\x87'
