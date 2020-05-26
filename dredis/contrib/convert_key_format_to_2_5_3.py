"""
In case the migration to the new key format using key IDs didn't go as expected,
you can convert them back using this script.

Example with dredis after 2.5.3 (the format isn't accurate, it's simplified to explain the idea):
    hset h name hugo

    4_h = keyID,1
    5_keyID_name = 'hugo'

The script converts those keys to the 2.5.3 format:
    4_h = 1
    5_h_name = 'hugo'


Real example (start with dredis later than 2.5.3)
-------------------------------------------------

1. Keys at the start
    "\x04\x00\x00\x00\x01h" = "h\xb8=\xb7\x8c\xfeK9\x8b\xdc`\x03\x81D9\b1"
    "\x05\x00\x00\x00\x10h\xb8=\xb7\x8c\xfeK9\x8b\xdc`\x03\x81D9\bname" = "hugo"

2. Convert the keys using this script
    $ PYTHONPATH=. python dredis/contrib/convert_key_format_to_2_5_3.py  --dir /tmp/dredis-data --backend leveldb
    Converting '\x04\x00\x00\x00\x01h'
    batch.put('\x04\x00\x00\x00\x10h\xb8=\xb7\x8c\xfeK9\x8b\xdc`\x03\x81D9\x08', '1')
    batch.delete('\x04\x00\x00\x00\x01h')
    [TO RUN] RENAME "h\xb8=\xb7\x8c\xfeK9\x8b\xdc`\x03\x81D9\x08" "h"

At the end of step 2, the keys will be properly set up but have weird names:
    "\x04\x00\x00\x00\x10h\xb8=\xb7\x8c\xfeK9\x8b\xdc`\x03\x81D9\b" = "1"
    "\x05\x00\x00\x00\x10h\xb8=\xb7\x8c\xfeK9\x8b\xdc`\x03\x81D9\bname" = "hugo"

3. Run dredis 2.5.3 or ealier

4. Run the RENAMEs from step 2
    $ redis-cli
    127.0.0.1:6379> RENAME "h\xb8=\xb7\x8c\xfeK9\x8b\xdc`\x03\x81D9\x08" "h"
    OK

5. Keys at the end
    "\x04\x00\x00\x00\x01h" = "1"
    "\x05\x00\x00\x00\x01hname" = "hugo"
"""
import argparse

from dredis.db import NUMBER_OF_REDIS_DATABASES, DB_MANAGER, KEY_CODEC, UUID_LENGTH_IN_BYTES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', help='dredis data directory', required=True)
    parser.add_argument('--backend', choices=['lmdb', 'leveldb'], help='output file type', required=True)
    args = parser.parse_args()

    DB_MANAGER.setup_dbs(args.dir, args.backend, {})
    revert_collections_to_dredis_2_5_3_format()


def revert_collections_to_dredis_2_5_3_format():
    for db_id in range(NUMBER_OF_REDIS_DATABASES):
        db = DB_MANAGER.get_db(db_id)
        for key_prefix in [
            KEY_CODEC.SET_TYPE,
            KEY_CODEC.HASH_TYPE,
            KEY_CODEC.ZSET_TYPE,
        ]:
            with db.write_batch() as batch:
                _convert(db, batch, chr(key_prefix))


def _convert(db, batch, key_prefix):
    for db_key, db_value in db.iterator(prefix=key_prefix):
        print('Converting %r' % db_key)
        _convert_key(batch, db_key, db_value)


def _convert_key(batch, db_key, db_value):
    type_id, _, key = KEY_CODEC.decode_key(db_key)
    if len(db_value) < UUID_LENGTH_IN_BYTES:
        # older schema before uuid
        key_id = key
        length = db_value
    else:
        # new schema with uuid
        key_id = db_value[:UUID_LENGTH_IN_BYTES]
        length = db_value[UUID_LENGTH_IN_BYTES:]
    print('batch.put({!r}, {!r})'.format(KEY_CODEC.get_key(key_id, type_id), length))
    batch.put(KEY_CODEC.get_key(key_id, type_id), length)
    if key != key_id:
        batch.delete(db_key)
        print('batch.delete({!r})'.format(db_key))
        print('[TO RUN] RENAME {} {}'.format(_rediscli_str(key_id), _rediscli_str(key)))


def _rediscli_str(s):
    """
    convert to a string that the redis-cli understands and escape properly
    """
    return '"{}"'.format(repr(s)[1:-1].replace('"', r'\"'))


if __name__ == '__main__':
    main()
