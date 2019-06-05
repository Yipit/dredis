import random
import string


def _get_random_value():
    return ''.join(random.choice(string.printable) for _ in range(int(random.random() * MAX_LENGTH)))


MAX_LENGTH = 1000
DATA_SIZE = 500
FUZZY_DATA = [_get_random_value() for i in range(DATA_SIZE)]


# NOTE: do not use pytest.mark.parametrize because it will create too many test cases
# and make the output confusing.
def test_dump_and_restore_fuzzy_strings(keyspace):
    key1 = 'test1'
    key2 = 'test2'
    for fuzzy_value in FUZZY_DATA:
        keyspace.set(key1, fuzzy_value)
        keyspace.restore(key2, 0, keyspace.dump(key1), replace=True)
        assert keyspace.get(key1) == fuzzy_value
        assert keyspace.get(key2) == keyspace.get(key1)


def test_dump_and_restore_fuzzy_sets(keyspace):
    key1 = 'test1'
    key2 = 'test2'
    for fuzzy_value in FUZZY_DATA:
        keyspace.sadd(key1, fuzzy_value)
    keyspace.restore(key2, 0, keyspace.dump(key1), replace=True)
    assert keyspace.smembers(key1) == set(FUZZY_DATA)
    assert keyspace.smembers(key2) == keyspace.smembers(key1)
