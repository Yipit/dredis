import pytest

from dredis import crc64, rdb


def test_should_raise_an_error_with_invalid_payload_size(keyspace):
    data = 'testvalue'
    with pytest.raises(ValueError) as exc:
        keyspace.restore('test', ttl=0, payload=data, replace=False)
    assert 'DUMP payload version or checksum are wrong' in str(exc)


def test_should_raise_an_error_with_incompatible_rdb_version(keyspace):
    data = '\x00'
    rdb_version = '\x08\x00'
    checksum = 'a' * 8
    with pytest.raises(ValueError) as exc:
        keyspace.restore('test', ttl=0, payload=data + rdb_version + checksum, replace=False)
    assert 'DUMP payload version or checksum are wrong' in str(exc)


def test_should_throw_an_error_with_invalid_checksum(keyspace):
    keyspace.set('test1', 'testvalue')
    payload = keyspace.dump('test1')[:-3] + 'bad'
    with pytest.raises(ValueError) as exc:
        keyspace.restore('test2', ttl=0, payload=payload, replace=False)
    assert 'DUMP payload version or checksum are wrong' in str(exc)


def test_should_raise_an_error_when_key_exists_and_replace_is_false(keyspace):
    keyspace.set('test', 'testvalue')
    with pytest.raises(KeyError) as exc:
        keyspace.restore('test', ttl=0, payload='testvalue1', replace=False)
    assert 'BUSYKEY Target key name already exists' in str(exc)


def test_should_raise_an_error_with_bad_object_type(keyspace):
    keyspace.set('test', 'testvalue')
    partial_payload = 'X' + keyspace.dump('test')[:-10] + rdb.get_rdb_version()
    payload = partial_payload + crc64.checksum(partial_payload)

    with pytest.raises(ValueError) as exc:
        keyspace.restore('test', ttl=0, payload=payload, replace=True)
    assert 'Bad data format' in str(exc)


def test_should_raise_an_error_with_bad_string_object(keyspace):
    keyspace.set('test', 'testvalue')
    partial_payload = '\x00\xff' + keyspace.dump('test')[:-10] + rdb.get_rdb_version()
    payload = partial_payload + crc64.checksum(partial_payload)

    with pytest.raises(ValueError) as exc:
        keyspace.restore('test', ttl=0, payload=payload, replace=True)
    assert 'Bad data format' in str(exc)


def test_should_be_able_to_restore_strings(keyspace):
    keyspace.set('test1', 'testvalue')
    payload = keyspace.dump('test1')

    keyspace.restore('test2', 0, payload, replace=False)

    assert keyspace.get('test2') == 'testvalue'


def test_should_be_able_to_restore_sets(keyspace):
    keyspace.sadd('set1', 'member1')
    keyspace.sadd('set1', 'member2')
    keyspace.sadd('set1', 'member3')
    payload = keyspace.dump('set1')

    keyspace.restore('set2', 0, payload, replace=False)

    assert keyspace.smembers('set2') == {'member1', 'member2', 'member3'}
