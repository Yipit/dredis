import glob
import os.path

from dredis import __version__
from tests.helpers import fresh_redis


def test_flushall():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.flushall() is True

    assert r0.keys('*') == []
    assert r1.keys('*') == []


def test_flush_db():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.flushdb() is True

    assert r0.keys('*') == []
    assert r1.keys('*') == ['test2']


def test_ping():
    r = fresh_redis()

    assert r.execute_command('ping') == 'PONG'
    assert r.execute_command('ping', 'msg') == 'msg'


def test_dbsize():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    assert r0.dbsize() == 0
    assert r1.dbsize() == 0

    r0.set('test', 'value')
    assert r0.dbsize() == 1
    assert r1.dbsize() == 0


def test_save_creates_an_rdb_file():
    r = fresh_redis()
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # 2 directory levels up
    rdb_files_before = set(glob.glob(os.path.join(root_dir, 'dump*.rdb')))

    r.set('test', 'value')
    assert r.save()
    assert len(set(glob.glob(os.path.join(root_dir, 'dump*.rdb'))) - rdb_files_before) == 1


def test_info():
    """
    Test dredis's version of INFO
    dredis INFO output is different than Redis and doesn't support all sections

    -----

    Redis INFO https://redis.io/commands/info:

        Available since 1.0.0.

        The INFO command returns information and statistics about the server in a format
        that is simple to parse by computers and easy to read by humans.

        The optional parameter can be used to select a specific section of information:

        * server: General information about the Redis server
        * clients: Client connections section
        * memory: Memory consumption related information
        * persistence: RDB and AOF related information
        * stats: General statistics
        * replication: Master/replica replication information
        * cpu: CPU consumption statistics
        * commandstats: Redis command statistics
        * cluster: Redis Cluster section
        * keyspace: Database related statistics


        It can also take the following values:

        * all: Return all sections
        * default: Return only the default set of sections

        When no parameter is provided, the default option is assumed.
    """

    # garbage collect old redis connections.
    # this is necessary to ensure only this test client is connected at this time
    import gc
    gc.collect()

    r = fresh_redis(db=0)
    r.set_response_callback('INFO', lambda x: x)  # ignore redis-py parsing of the response and use the raw response
    r.set('mystr', 'myvalue')

    server_section_output = '\r\n'.join([
        '# Server',
        'dredis_version:{dredis_version}'.format(dredis_version=__version__),
    ])
    client_section_output = '\r\n'.join([
        '# Clients',
        'connected_clients:{connected_clients}'.format(connected_clients=1),
    ])
    keyspace_section_output = '\r\n'.join([
        '# Keyspace',
        'db{db_num}:keys={key_count},expires={expire_count},avg_ttl={avg_ttl}'.format(
            db_num=0, key_count=1, expire_count=0, avg_ttl=0),
    ])
    default_section_output = '\r\n\r\n'.join([server_section_output, client_section_output, keyspace_section_output])
    assert r.info() == default_section_output
    assert r.info('default') == default_section_output
    assert r.info('server') == server_section_output
    assert r.info('clients') == client_section_output
    assert r.info('keyspace') == keyspace_section_output
    assert r.info('randomname') == ''
