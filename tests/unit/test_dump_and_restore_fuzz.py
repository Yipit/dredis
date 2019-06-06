import random
import string

from dredis.keyspace import to_float_string


def _get_random_string():
    return ''.join(random.choice(string.printable) for _ in range(int(random.random() * MAX_LENGTH)))


def random_sign():
    return [-1, 1][int(random.random() * 2)]


MAX_LENGTH = 1000
DATA_SIZE = 500
FUZZY_STRINGS = [_get_random_string() for _ in range(DATA_SIZE)]
FUZZY_FLOATS = [random.random() * random_sign() for _ in range(DATA_SIZE)]
FUZZY_INTS = [int(random.random()) * random_sign() for _ in range(DATA_SIZE)]


# NOTE: do not use pytest.mark.parametrize because it will create too many test cases
# and make the output confusing.
def test_dump_and_restore_fuzzy_strings(keyspace):
    key1 = 'test1'
    key2 = 'test2'
    for fuzzy_value in FUZZY_STRINGS:
        keyspace.set(key1, fuzzy_value)
        keyspace.restore(key2, 0, keyspace.dump(key1), replace=True)
        assert keyspace.get(key1) == fuzzy_value
        assert keyspace.get(key2) == keyspace.get(key1)


def test_dump_and_restore_fuzzy_sets(keyspace):
    key1 = 'test1'
    key2 = 'test2'
    for fuzzy_value in FUZZY_STRINGS:
        keyspace.sadd(key1, fuzzy_value)
    keyspace.restore(key2, 0, keyspace.dump(key1), replace=True)
    assert keyspace.smembers(key1) == set(FUZZY_STRINGS)
    assert keyspace.smembers(key2) == keyspace.smembers(key1)


def test_dump_and_restore_fuzzy_sorted_sets(keyspace):
    key1 = 'test1'
    key2 = 'test2'
    fuzzy_sorted_set = {}
    for score, value in zip(FUZZY_FLOATS, FUZZY_STRINGS):
        keyspace.zadd(key1, score=score, value=value)
        fuzzy_sorted_set[value] = to_float_string(score)

    keyspace.restore(key2, 0, keyspace.dump(key1), replace=True)

    zrange = keyspace.zrange(key2, 0, -1, with_scores=True)
    assert dict(zip(zrange[::2], zrange[1::2])) == fuzzy_sorted_set
