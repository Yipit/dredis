## 0.1.1

* Minimize send() calls


## 0.1.0

* New binary file format
* Faster directory checks


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
