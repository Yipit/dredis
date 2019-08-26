# DRedis: Disk-based Redis implementation

Redis is a great key-value database and it's extremely fast because it's in-memory.
Some people want Redis's rich data types without having to worry about the memory limitations. For those
that can afford slower performance and want unlimited storage, DRedis may be an alternative.

**WARNING: This project is still experimental and it doesn't implement all Redis commands!**



## Installing

Make sure to install the [LevelDB](https://github.com/google/leveldb) C++ library (`apt-get install libleveldb-dev` or `brew install leveldb`) and then run:

```shell
$ pip install dredis
```

Note: The LMDB backend doesn't require external dependencies.

## Running


```shell
$ dredis --dir /tmp/dredis-data
```

To know about all of the options, use `--help`:

```shell
$ dredis --help
usage: dredis [-h] [-v] [--host HOST] [--port PORT] [--dir DIR]
              [--backend {lmdb,leveldb,memory}]
              [--backend-option BACKEND_OPTION] [--rdb RDB] [--debug]
              [--flushall] [--readonly]

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  --host HOST           server host (defaults to 127.0.0.1)
  --port PORT           server port (defaults to 6377)
  --dir DIR             directory to save data (defaults to a temporary
                        directory)
  --backend {lmdb,leveldb,memory}
                        key/value database backend (defaults to leveldb)
  --backend-option BACKEND_OPTION
                        database backend options (e.g., --backend-option
                        map_size=BYTES)
  --rdb RDB             RDB file to seed dredis
  --debug               enable debug logs
  --flushall            run FLUSHALL on startup
  --readonly            accept read-only commands
```


Running dredis with Docker locally (port 6377 on the host):

```shell
$ docker-compose up
```



## Backends

There's support for LevelDB, LMDB, and an experimental memory backend.
All backend options should be passed in the command line as `--backend-option NAME1=value1 --backend-option NAME2=value2` (the values must be JSON-compatible).

### LevelDB
LevelDB is the easiest persistent backend because it doesn't require any option tweaking to get it to work reliably.

#### Options

We use [plyvel](https://github.com/wbolster/plyvel) as the LevelDB backend. All available options are parameters of [plyvel.DB](https://plyvel.readthedocs.io/en/latest/api.html#DB).

The current default options for LevelDB are:
* `name`: The same value as the `--dir` option
* `create_if_missing`: `True`


### LMDB

The performance of LMDB can be better than LevelDB and we're considering making it the default backend in the future.

#### Options

We use [py-lmdb](https://github.com/dw/py-lmdb/) as the LMDB backend. All available options are parameters of [lmdb.Environment](https://lmdb.readthedocs.io/en/release/#environment-class).
We recommend that you think ahead and change the `map_size` parameter according to your needs — this is the maximum size of the LMDB database file on disk.

The current default options for LMDB are:
* `path`: The same value as the `--dir` option
* `map_size`: `1073741824` (1GB)
* `map_async`: `True`
* `writemap`: `True`
* `readahead`: `False`
* `metasync`: `False`

### Memory

This is experimental and doesn't persist to disk. It was created to have a baseline to compare persistent backends.


#### Options
None.


## Supported Commands

Command signature                            | Type
---------------------------------------------|-----
COMMAND\*                                    | Server
DBSIZE                                       | Server
FLUSHALL                                     | Server
FLUSHDB                                      | Server
SAVE                                         | Server
DEL key [key ...]                            | Keys
DUMP key                                     | Keys
EXISTS key [key ...]                         | Keys
EXPIRE key ttl\**                            | Keys
KEYS pattern                                 | Keys
RENAME key newkey                            | Keys
RESTORE key ttl serialized-value [REPLACE]\***| Keys
TTL key                                      | Keys
TYPE key                                     | Keys
AUTH                                         | Connection
PING [msg]                                   | Connection
SELECT db                                    | Connection
GET key                                      | Strings
GETRANGE key start end                       | Strings
INCR key                                     | Strings
INCRBY key increment                         | Strings
SET key value                                | Strings
SADD key value [value ..]                    | Sets
SCARD key                                    | Sets
SISMEMBER key value                          | Sets
SMEMBERS key                                 | Sets
EVAL script numkeys [key ...] [arg ...]      | Scripting
ZADD key score member [score member ...]     | Sorted Sets
ZCARD key                                    | Sorted Sets
ZCOUNT key min_score max_score               | Sorted Sets
ZRANGE key start top [WITHSCORES]            | Sorted Sets
ZRANGEBYSCORE key min_score max_score [WITHSCORES] [LIMIT offset count] | Sorted Sets
ZRANK key member                             | Sorted Sets
ZREM key member [member ...]                 | Sorted Sets
ZSCAN key cursor [MATCH pattern] [COUNT count]|Sorted Sets
ZSCORE key member                            | Sorted Sets
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] | Sorted Sets
HDEL key field [field ...]                   | Hashes
HGET key value                               | Hashes
HGETALL key                                  | Hashes
HINCRBY key field increment                  | Hashes
HKEYS key                                    | Hashes
HLEN key                                     | Hashes
HSET key field value [field value ...]       | Hashes
HSETNX key field value                       | Hashes
HVALS value                                  | Hashes

Footnotes:

* \*`COMMAND`'s reply is incompatible at the moment, it returns a flat array with command names (their arity, flags, positions, or step count are not returned).
* \**`EXPIRE` doesn't set key expiration yet, it's a no-op command
* \***`RESTORE` doesn't work with Redis strings compressed with LZF or encoded as `OBJ_ENCODING_INT`; also doesn't work with sets encoded as `OBJ_ENCODING_INTSET`, nor hashes and sorted sets encoded as `OBJ_ENCODING_ZIPLIST`.

## How is DRedis implemented

Initially DRedis had its own filesystem structure, but then it was converted to use [LevelDB](https://github.com/google/leveldb), which is a lot more reliable and faster (nowadays there's also the LMDB backend).

Other projects implement similar features to what's available on DRedis, but they aren't what Yipit needed when the project started. Some of them
rely on multiple threads and compromise on consistency, don't implement Lua scripts, or don't implement sorted sets correctly. We ran the DRedis tests against a few solutions and they failed (which means they're not fully compatible).

Similar projects:

* https://github.com/yinqiwen/ardb
  * `ardb` seems to be the most similar in scope and a good candidate for contributions or a fork. Their sorted sets implementation has a bug with negative scores. It's a large C++ project with lots of features.
* https://github.com/Qihoo360/pika
  * no Lua support. This is a large C++ project and its documentation is in Chinese. The project seems to be stable and is used by large Chinese companies.
* https://github.com/KernelMaker/blackwidow
  * a C++ library, not a Redis-like server
* https://github.com/siddontang/ledisdb
  * Similar to Redis but different commands
* https://github.com/reborndb/qdb
  * No Lua support and no longer maintained
* https://github.com/alash3al/redix
  * No Lua and no sorted set support
* https://github.com/meitu/titan
  * No Lua support and not enough sorted set support


## Lua support

Lua is supported through the [lupa](https://github.com/scoder/lupa) library.


## Challenges

### Data Consistency

We rely on the backends' consistency properties and we use batches/transactions to stay consistent. Tweaking the backend options may impair consistency (e.g., `sync=false` for LMDB).

### Cluster mode & Replication

Replication, key distribution, and cluster mode are not supported.
If you want higher availability you can create multiple servers that share or replicate a disk (consistency may suffer when replicating).
Use DNS routing or a network load balancer to route requests properly.

### Backups

The command `SAVE` creates a snapshot in the same format as Redis's RDB version 7 (compatible with Redis 3.x).
We recommend you to run `SAVE` on a secondary `dredis` process, otherwise the server will hang during the snapshot (consistency guarantees are higher with LMDB as the backend).
The command `BGSAVE` may be supported in the future.

Other backups solutions involve backing up the files created by the backend.
A straightforward approach is to have periodic backups to an object storage such as Amazon S3 orr use a block storage solution and perform periodic backups (e.g., AWS EBS).

If you use `SAVE` from a secondary process or backup the data directory, there shouldn't be any significant impact on the main server.


## Why Python

Because it's a good language to get things off the ground quickly and everybody at Yipit knows it well.
If this becomes a valuable project, other languages will be evaluated — the language of choice won't affect much of the I/O bottleneck, but it may bring good performance benefits.
We're experimenting with [Cython](https://cython.org/) to get better performance without having to rewrite large chunks in C.

The project will migrate to Python 3 soon.


## Didn't you have better names?

[@andrewgross](https://github.com/andrewgross) suggested [REDISK](https://twitter.com/awgross/status/1031962830633934849). The name will only matter if this project survives, it's still an experiment.
Also, [other projects use the name redisk](https://github.com/search?q=redisk&type=Repositories).

[@nadlerjessie](https://github.com/nadlerjessie) suggested we pronounce dredis as "Doctor Redis".


## Releasing dredis

1. Make sure you have all important changes in the top section of `CHANGELOG.md`
1. Make sure your PyPI credentials are correct in `~/.pypirc`
1. Run `make release`
1. Enter the new version (e.g., `1.0.0`)
