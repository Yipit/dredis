import random
import string

import pytest


def _get_random_value():
    return ''.join(random.choice(string.printable) for _ in range(int(random.random() * MAX_LENGTH)))


MAX_LENGTH = 1000
DATA_SIZE = 500
FUZZY_DATA = [_get_random_value() for i in range(DATA_SIZE)]


@pytest.mark.parametrize('fuzzy_value', FUZZY_DATA)
def test_dump_and_restore_fuzzy_strings(keyspace, fuzzy_value):
    key1 = 'test1'
    key2 = 'test2'
    keyspace.set(key1, fuzzy_value)
    keyspace.restore(key2, 0, keyspace.dump(key1), replace=False)
    assert keyspace.get(key1) == fuzzy_value
    assert keyspace.get(key2) == keyspace.get(key1)
