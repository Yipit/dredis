from functools import wraps


REDIS_COMMANDS = {}


def _check_arity(expected_arity, passed_arity, cmd_name):
    syntax_err = SyntaxError("Wrong number of arguments for '{}' command".format(cmd_name.lower()))
    if expected_arity < 0:  # minimum arity
        if passed_arity < -expected_arity:
            raise syntax_err
    else:  # exact match
        if passed_arity != expected_arity:  # exact arity
            raise syntax_err


def command(cmd_name, arity):
    def decorator(fn):
        @wraps(fn)
        def newfn(keyspace, *args, **kwargs):
            # redis includes the command name in the arity, thus adding 1
            passed_arity = 1 + len(args) + len(kwargs)
            _check_arity(arity, passed_arity, cmd_name)
            return fn(keyspace, *args, **kwargs)
        newfn.arity = arity
        REDIS_COMMANDS[cmd_name] = newfn
        return newfn
    return decorator


class SimpleString(str):
    pass


"""
*******************
* Server commands *
*******************
"""


@command('COMMAND', arity=0)
def cmd_command(keyspace):
    result = []
    for cmd in REDIS_COMMANDS:
        result.append(cmd.upper())
    return result


@command('FLUSHALL', arity=-1)
def cmd_flushall(keyspace, *args):
    # TODO: we don't support ASYNC flushes
    keyspace.flushall()
    return SimpleString('OK')


@command('FLUSHDB', arity=-1)
def cmd_flushdb(keyspace, *args):
    # TODO: we don't support ASYNC flushes
    keyspace.flushdb()
    return SimpleString('OK')


"""
****************
* Key commands *
****************
"""


@command('DEL', arity=-2)
def cmd_del(keyspace, *keys):
    return keyspace.delete(*keys)


@command('TYPE', arity=2)
def cmd_type(keyspace, key):
    return keyspace.type(key)


@command('KEYS', arity=2)
def cmd_keys(keyspace, pattern):
    return keyspace.keys(pattern)


@command('EXISTS', arity=-2)
def cmd_exists(keyspace, *keys):
    return keyspace.exists(*keys)


"""
***********************
* Connection commands *
***********************
"""


@command('PING', arity=-1)
def cmd_ping(keyspace, message='PONG'):
    return SimpleString(message)


@command('SELECT', arity=2)
def cmd_select(keyspace, db):
    keyspace.select(db)
    return SimpleString('OK')


"""
*******************
* String commands *
*******************
"""


@command('SET', arity=-3)
def cmd_set(keyspace, key, value, *args):
    if len(args):
        raise SyntaxError('No support for EX|PX and NX|XX at the moment.')
    keyspace.set(key, value)
    return SimpleString('OK')


@command('GET', arity=2)
def cmd_get(keyspace, key):
    return keyspace.get(key)


@command('INCR', arity=2)
def cmd_incr(keyspace, key):
    return keyspace.incrby(key, 1)


@command('INCRBY', arity=3)
def cmd_incrby(keyspace, key, increment):
    return keyspace.incrby(key, int(increment))


@command('GETRANGE', arity=4)
def cmd_getrange(keyspace, key, start, end):
    return keyspace.getrange(key, int(start), int(end))


"""
****************
* Set commands *
****************
"""


@command('SADD', arity=-3)
def cmd_sadd(keyspace, key, *values):
    count = 0
    for value in values:
        count += keyspace.sadd(key, value)
    return count


@command('SMEMBERS', arity=2)
def cmd_smembers(keyspace, key):
    return keyspace.smembers(key)


@command('SCARD', arity=2)
def cmd_scard(keyspace, key):
    return keyspace.scard(key)


@command('SISMEMBER', arity=3)
def cmd_sismember(keyspace, key, value):
    return int(keyspace.sismember(key, value))


"""
**********************
* Scripting commands *
**********************
"""


@command('EVAL', arity=-3)
def cmd_eval(keyspace, script, numkeys, *keys):
    return keyspace.eval(script, int(numkeys), keys)


"""
***********************
* Sorted set commands *
***********************
"""


@command('ZADD', arity=-4)
def cmd_zadd(keyspace, key, *flat_pairs):
    count = 0
    pairs = zip(flat_pairs[0::2], flat_pairs[1::2])  # [1, 2, 3, 4] -> [(1,2), (3,4)]
    for score, value in pairs:
        count += keyspace.zadd(key, score, value)
    return count


@command('ZRANGE', arity=-4)
def cmd_zrange(keyspace, key, start, stop, with_scores=False):
    return keyspace.zrange(key, int(start), int(stop), bool(with_scores))


@command('ZCARD', arity=2)
def cmd_zcard(keyspace, key):
    return keyspace.zcard(key)


@command('ZREM', arity=-3)
def cmd_zrem(keyspace, key, *members):
    return keyspace.zrem(key, *members)


@command('ZSCORE', arity=3)
def cmd_zscore(keyspace, key, member):
    return keyspace.zscore(key, member)


@command('ZRANK', arity=3)
def cmd_zrank(keyspace, key, member):
    return keyspace.zrank(key, member)


@command('ZCOUNT', arity=4)
def cmd_zcount(keyspace, key, min_score, max_score):
    return keyspace.zcount(key, float(min_score), float(max_score))


@command('ZRANGEBYSCORE', arity=-4)
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
        key, float(min_score), float(max_score), withscores=withscores, offset=offset, count=count)
    return members


@command('ZUNIONSTORE', arity=-4)
def cmd_zunionstore(keyspace, destination, numkeys, *args):
    keys = []
    weights = []
    args = list(args)
    while args:
        arg = args.pop(0)
        if arg == 'WEIGHTS':
            weights = map(float, args)
            break
        else:
            keys.append(arg)
    assert len(weights) <= len(keys)  # FIXME: probably want nicer errors
    ones = [1] * (len(keys) - len(weights))  # fill in with default weight of 1
    weights = weights + ones
    return keyspace.zunionstore(destination, keys, weights)

"""
*******************
* Hash commands *
*******************
"""


@command('HSET', arity=-4)
def cmd_hset(keyspace, key, *pairs):
    if len(pairs) % 2 != 0:
        # HSET is going to replace HMSET,
        # see https://github.com/antirez/redis/pull/5334#issuecomment-419194180 for more details
        raise SyntaxError('wrong number of arguments for HMSET')
    count = 0
    for field, value in zip(pairs[0::2], pairs[1::2]):
        count += keyspace.hset(key, field, value)
    return count


@command('HDEL', arity=-3)
def cmd_hdel(keyspace, key, *fields):
    return keyspace.hdel(key, *fields)


@command('HSETNX', arity=4)
def cmd_hsetnx(keyspace, key, field, value):
    return keyspace.hsetnx(key, field, value)


@command('HGET', arity=3)
def cmd_hget(keyspace, key, value):
    return keyspace.hget(key, value)


@command('HKEYS', arity=2)
def cmd_hkeys(keyspace, key):
    return keyspace.hkeys(key)


@command('HVALS', arity=2)
def cmd_hvals(keyspace, key):
    return keyspace.hvals(key)


@command('HLEN', arity=2)
def cmd_hlen(keyspace, key):
    return keyspace.hlen(key)


@command('HINCRBY', arity=4)
def cmd_hincrby(keyspace, key, field, increment):
    return keyspace.hincrby(key, field, increment)


@command('HGETALL', arity=2)
def cmd_hincrby(keyspace, key):
    return keyspace.hgetall(key)


def run_command(keyspace, cmd, args):
    str_args = map(str, args)
    return REDIS_COMMANDS[cmd.upper()](keyspace, *str_args)
