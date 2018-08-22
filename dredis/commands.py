REDIS_COMMANDS = {}


def command(cmd_name):
    def decorator(fn):
        REDIS_COMMANDS[cmd_name] = fn
        return fn
    return decorator


"""
*******************
* Server commands *
*******************
"""


@command('COMMAND')
def cmd_command(keyspace):
    result = []
    for cmd in REDIS_COMMANDS:
        result.append(cmd.upper())
    return result


@command('FLUSHALL')
def cmd_flushall(keyspace):
    keyspace.flushall()
    return 'OK'


@command('FLUSHDB')
def cmd_flushdb(keyspace):
    # FIXME: doesn't support multiple DBs currently
    #keyspace.flushdb()
    keyspace.flushall()
    return 'OK'


"""
****************
* Key commands *
****************
"""


@command('DEL')
def cmd_del(keyspace, key):
    return keyspace.delete(key)


@command('TYPE')
def cmd_type(keyspace, key):
    return keyspace.type(key)


@command('KEYS')
def cmd_keys(keyspace, pattern):
    return keyspace.keys(pattern)


"""
***********************
* Connection commands *
***********************
"""


@command('PING')
def cmd_ping(keyspace):
    return 'PONG'


@command('SELECT')
def cmd_select(keyspace, db):
    return 'OK'


"""
*******************
* String commands *
*******************
"""


@command('SET')
def cmd_set(keyspace, key, value, *args):
    keyspace.set(key, value)
    return 'OK'


@command('GET')
def cmd_get(keyspace, key):
    return keyspace.get(key)


@command('INCR')
def cmd_incr(keyspace, key):
    return str(keyspace.incrby(key, 1))


@command('INCRBY')
def cmd_incrby(keyspace, key, increment):
    return str(keyspace.incrby(key, int(increment)))


"""
****************
* Set commands *
****************
"""


@command('SADD')
def cmd_sadd(keyspace, key, *values):
    count = 0
    for value in values:
        count += keyspace.sadd(key, value)
    return count


@command('SMEMBERS')
def cmd_smembers(keyspace, key):
    return keyspace.smembers(key)


@command('SCARD')
def cmd_scard(keyspace, key):
    return keyspace.scard(key)


@command('SISMEMBER')
def cmd_sismember(keyspace, key, value):
    return int(keyspace.sismember(key, value))


"""
**********************
* Scripting commands *
**********************
"""


@command('EVAL')
def cmd_eval(keyspace, script, numkeys, *keys):
    result = keyspace.eval(script, int(numkeys), keys)
    if isinstance(result, dict):
        raise ValueError(result['err'])

    # the Redis protocol doesn't support booleans, so `True` becomes 1 and `False` becomes nil
    if result is True:
        return 1
    elif result is False:
        return None
    else:
        # assumes `result` is either a list or string
        return result


"""
***********************
* Sorted set commands *
***********************
"""


@command('ZADD')
def cmd_zadd(keyspace, key, score, *values):
    count = 0
    for value in values:
        count += keyspace.zadd(key, score, value)
    return count


@command('ZRANGE')
def cmd_zrange(keyspace, key, start, stop, with_scores=False):
    return keyspace.zrange(key, int(start), int(stop), bool(with_scores))

@command('ZCARD')
def cmd_zcard(keyspace, key):
    return keyspace.zcard(key)


@command('ZREM')
def cmd_zrem(keyspace, key, *members):
    return keyspace.zrem(key, *members)


@command('ZSCORE')
def cmd_zscore(keyspace, key, member):
    return keyspace.zscore(key, member)


@command('ZRANK')
def cmd_zrank(keyspace, key, member):
    return keyspace.zrank(key, member)


@command('ZRANGEBYSCORE')
def cmd_zrangebyscore(keyspace, key, min_score, max_score, *args):
    withscores = any(arg.lower() == 'withscores' for arg in args)
    offset = 0
    count = float('+inf')
    while args:
        arg, args = args[0], args[1:]
        if arg.lower() == 'limit':
            offset, args = int(args[0]), args[1:]
            count, args = int(args[0]), args[1:]
            break

    members = keyspace.zrangebyscore(
        key, int(min_score), int(max_score), withscores=withscores, offset=offset, count=count)
    return members


"""
*******************
* Hash commands *
*******************
"""


@command('HSET')
def cmd_hset(keyspace, key, field, value):
    return keyspace.hset(key, field, value)


@command('HSETNX')
def cmd_hsetnx(keyspace, key, field, value):
    return keyspace.hsetnx(key, field, value)


@command('HGET')
def cmd_hget(keyspace, key, value):
    return keyspace.hget(key, value)


@command('HKEYS')
def cmd_hkeys(keyspace, key):
    return keyspace.hkeys(key)


@command('HVALS')
def cmd_hvals(keyspace, key):
    return keyspace.hvals(key)


@command('HLEN')
def cmd_hlen(keyspace, key):
    return keyspace.hlen(key)


def run_command(keyspace, cmd, args):
    return REDIS_COMMANDS[cmd.upper()](keyspace, *args)