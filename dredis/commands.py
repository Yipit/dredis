import logging
from functools import wraps

from dredis.utils import to_float

logger = logging.getLogger(__name__)


REDIS_COMMANDS = {}
SYNTAXERR = SyntaxError('syntax error')


def _check_arity(expected_arity, passed_arity, cmd_name):
    syntax_err = SyntaxError("wrong number of arguments for '{}' command".format(cmd_name.lower()))
    if expected_arity < 0:  # minimum arity
        if passed_arity < -expected_arity:
            raise syntax_err
    elif expected_arity > 0:  # exact match
        if passed_arity != expected_arity:  # exact arity
            raise syntax_err
    else:  # ignore
        # ignore arity of 0 (at the moment it's only for `COMMAND`).
        # it could be set to 1 but the redis source has it as 0, just following their implementation.
        # source: https://github.com/antirez/redis/blob/cb51bb4320d2240001e8fc4a522d59fb28259703/src/server.c#L296
        return


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


@command('DBSIZE', arity=1)
def cmd_dbsize(keyspace):
    return keyspace.dbsize()


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
def cmd_ping(keyspace, message=SimpleString('PONG')):
    return message


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
def cmd_eval(keyspace, script, numkeys, *args):
    numkeys = int(numkeys)
    keys = args[:numkeys]
    argv = args[numkeys:]
    return keyspace.eval(script, keys, argv)


"""
***********************
* Sorted set commands *
***********************
"""


@command('ZADD', arity=-4)
def cmd_zadd(keyspace, key, *flat_pairs):
    if len(flat_pairs) % 2 != 0:
        raise SYNTAXERR

    count = 0
    pairs = zip(flat_pairs[0::2], flat_pairs[1::2])  # [1, 2, 3, 4] -> [(1,2), (3,4)]
    for score, value in pairs:
        _validate_zset_score(score)
        count += keyspace.zadd(key, score, value)
    return count


@command('ZRANGE', arity=-4)
def cmd_zrange(keyspace, key, start, stop, *args):
    with_scores = False
    if args:
        if args[0].lower() == 'withscores':
            with_scores = True
        else:
            raise SYNTAXERR
    return keyspace.zrange(key, int(start), int(stop), with_scores)


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
    return keyspace.zcount(key, min_score, max_score)


@command('ZRANGEBYSCORE', arity=-4)
def cmd_zrangebyscore(keyspace, key, min_score, max_score, *args):
    withscores = False
    offset = 0
    count = float('+inf')
    args = list(args)
    while args:
        arg = args.pop(0)
        if len(args) >= 0 and arg.lower() == 'withscores':
            withscores = True
        elif len(args) >= 2 and arg.lower() == 'limit':
            offset = int(args.pop(0))
            count = int(args.pop(0))
        else:
            raise SYNTAXERR

    _validate_zset_score(min_score)
    _validate_zset_score(max_score)

    members = keyspace.zrangebyscore(
        key, min_score, max_score, withscores=withscores, offset=offset, count=count)
    return members


def _validate_zset_score(score):
    clean_score = score.strip('(').lower().replace('nan', 'invalid')
    try:
        to_float(clean_score)
    except ValueError:
        raise SyntaxError("min or max is not a float")


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
    if len(weights) > len(keys):
        raise SYNTAXERR
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
def cmd_hgetall(keyspace, key):
    return keyspace.hgetall(key)


class CommandNotFound(Exception):
    """Exception to flag not found Redis command"""


def run_command(keyspace, cmd, args):
    logger.debug('[run_command] cmd={}, args={}'.format(repr(cmd), repr(args)))

    str_args = map(str, args)
    if cmd.upper() not in REDIS_COMMANDS:
        raise CommandNotFound()
    else:
        return REDIS_COMMANDS[cmd.upper()](keyspace, *str_args)
