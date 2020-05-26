## Not released yet

## 2.6.0

* Change zset, set, and hash implementation to use pointers to allow asynchronous deletion
* Add a key garbage collector as a background thread to delete keys marked for deletion
* Fix hash length bug introduced in commit 53db515d6271784399a38256e08eb290eab153ef

## 2.5.3

* Improve parser performance significantly for large commands (https://github.com/Yipit/dredis/pull/57)

## 2.5.2

* Fix bug with big replies and `socket.send` (https://github.com/Yipit/dredis/pull/56)

## 2.5.1

* Fix bug with stop clause of HSCAN and ZSCAN (https://github.com/Yipit/dredis/issues/53)

## 2.5.0

* Add support to NX and XX parameters of ZADD (https://github.com/Yipit/dredis/pull/52)

## 2.4.3

* Fix log levels when changing configuration
 
## 2.4.2

* Fix duplicated logs due to multiple logger handlers

## 2.4.1

* Fix bug where INFO messages weren't being displayed

## 2.4.0

* Add support to CONFIG GET and CONFIG SET (https://github.com/Yipit/dredis/pull/49)

## 2.3.0

* Add support to HSCAN (https://github.com/Yipit/dredis/pull/47)

## 2.2.0

* Add support to ZSCAN (https://github.com/Yipit/dredis/pull/43)
* Add static version of TTL and no-op version of EXPIRE (https://github.com/Yipit/dredis/pull/44)
* Add contrib/snapshot_lmdb.py and remove contrib/dredis-snapshot (https://github.com/Yipit/dredis/pull/40)

## 2.1.0

* Add AUTH support (https://github.com/Yipit/dredis/pull/37)

## 2.0.0

* Add RENAME command (https://github.com/Yipit/dredis/pull/36)
* Fix negative score order in sorted sets (backward incompatible change: https://github.com/Yipit/dredis/pull/35)

## 1.1.0

* Add more storage backends (https://github.com/Yipit/dredis/pull/23)
  - New CLI arguments: `--backend` and `--backend-option`
  - Add [LMDB](http://www.lmdb.tech/doc/) as a backend (performed better than LevelDB on Linux with the ext4 filesystem)
  - Add memory backend (for tests & development)
* Make `KEYS` faster by looking at fewer backend keys (https://github.com/Yipit/dredis/pull/31)
* Add `DUMP` and `RESTORE` commands (https://github.com/Yipit/dredis/pull/27 and https://github.com/Yipit/dredis/pull/28)
  - Based on the RDB version 7 implementation
* Add `SAVE` command and `--rdb` CLI option (https://github.com/Yipit/dredis/pull/32)
  - `SAVE` creates an RDB dump (e.g., `dump_2019-06-21T15:55:09.rdb`)
  - `--rdb` seeds dredis with a redis RDB file
  - Only RDB version 7 and older are supported
* Add `--read-only` CLI option to accept read-only commands (https://github.com/Yipit/dredis/pull/33)

## 1.0.2

* Set TCP_NODELAY flag to client sockets (https://github.com/Yipit/dredis/pull/16)

## 1.0.1

* Improve TCP communication (https://github.com/Yipit/dredis/pull/15)

## 1.0.0

* Change storage to use LevelDB instead of our own directory structure and file formats (https://github.com/Yipit/dredis/pull/12)


## 0.1.1

* Minimize send() calls


## 0.1.0

* New binary file format (https://github.com/Yipit/dredis/pull/6)
* Faster directory checks (https://github.com/Yipit/dredis/pull/5)


## 0.0.7

* Add dredis-snapshot script

## 0.0.6

* Optimize Lua initialization


## 0.0.5

Change ZREM to check for scores_path emptiness


## 0.0.4

* Minimize `write()` calls
* Remove score file when there are no lines left


## 0.0.3

* Fix mismatching ZADD reply of existing elements
* Performance improvements by using select.poll
* Use generator when parsing instructions instead of list (should yield faster)
* Use asyncore.dispatcher without buffering output
* Rewrite file in place when removing lines


## 0.0.2

* Re-upload 0.0.1 with README as Markdown


## 0.0.1

* Implement many commands and Lua scripting
* Data is stored on the filesystem using different directory structures for each data structure (string, set, hash, sorted set)
