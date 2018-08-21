#!/Users/hugo/.virtualenvs/dredis/bin/python

import asyncore
import json
import os.path
import socket
import sys
import traceback

from dredis.keyspace import RedisScriptError, DiskKeyspace
from dredis.parser import parse_instructions


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
def cmd_command():
    result = []
    for cmd in REDIS_COMMANDS:
        result.append(cmd.upper())
    return result


@command('FLUSHALL')
def cmd_flushall():
    keyspace.flushall()
    return 'OK'


@command('FLUSHDB')
def cmd_flushdb():
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
def cmd_del(key):
    return keyspace.delete(key)


@command('TYPE')
def cmd_type(key):
    return keyspace.type(key)


@command('KEYS')
def cmd_keys(pattern):
    return keyspace.keys(pattern)


"""
***********************
* Connection commands *
***********************
"""


@command('PING')
def cmd_ping():
    return 'PONG'


@command('SELECT')
def cmd_select(db):
    return 'OK'


"""
*******************
* String commands *
*******************
"""


@command('SET')
def cmd_set(key, value, *args):
    keyspace.set(key, value)
    return 'OK'


@command('GET')
def cmd_get(key):
    return keyspace.get(key)


@command('INCR')
def cmd_incr(key):
    return str(keyspace.incrby(key, 1))


@command('INCRBY')
def cmd_incrby(key, increment):
    return str(keyspace.incrby(key, int(increment)))


"""
****************
* Set commands *
****************
"""


@command('SADD')
def cmd_sadd(key, *values):
    count = 0
    for value in values:
        count += keyspace.sadd(key, value)
    return count


@command('SMEMBERS')
def cmd_smembers(key):
    return keyspace.smembers(key)


@command('SCARD')
def cmd_scard(key):
    return keyspace.scard(key)


@command('SISMEMBER')
def cmd_sismember(key, value):
    return int(keyspace.sismember(key, value))


"""
**********************
* Scripting commands *
**********************
"""


@command('EVAL')
def cmd_eval(script, numkeys, *keys):
    result = keyspace.eval(script, int(numkeys), keys)
    if isinstance(result, dict):
        raise ValueError(result['err'])
    return result


"""
***********************
* Sorted set commands *
***********************
"""


@command('ZADD')
def cmd_zadd(key, score, *values):
    count = 0
    for value in values:
        count += keyspace.zadd(key, score, value)
    return count


@command('ZRANGE')
def cmd_zrange(key, start, stop, with_scores=False):
    return keyspace.zrange(key, int(start), int(stop), bool(with_scores))

@command('ZCARD')
def cmd_zcard(key):
    return keyspace.zcard(key)


@command('ZREM')
def cmd_zcard(key, *members):
    return keyspace.zrem(key, *members)


@command('ZSCORE')
def cmd_zcard(key, member):
    return keyspace.zscore(key, member)


@command('ZRANK')
def cmd_zcard(key, member):
    return keyspace.zrank(key, member)


@command('ZRANGEBYSCORE')
def cmd_zrangebyscore(key, min_score, max_score, *args):
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
def cmd_hset(key, field, value):
    return keyspace.hset(key, field, value)


@command('HSETNX')
def cmd_hsetnx(key, field, value):
    return keyspace.hsetnx(key, field, value)


@command('HGET')
def cmd_hget(key, value):
    return keyspace.hget(key, value)


@command('HKEYS')
def cmd_hkeys(key):
    return keyspace.hkeys(key)


@command('HVALS')
def cmd_hvals(key):
    return keyspace.hvals(key)


@command('HLEN')
def cmd_hlen(key):
    return keyspace.hlen(key)


def not_found(send_fn, cmd):
    send_fn("-ERR unknown command '{}'\r\n".format(cmd))


def err(send_fn, tb):
    send_fn("-Server exception: {}\r\n".format(json.dumps(tb)))


def execute_cmd(send_fn, cmd, *args):
    print('cmd={}, args={}'.format(repr(cmd), repr(args)))
    try:
        result = REDIS_COMMANDS[cmd.upper()](*args)
    except (ValueError, RedisScriptError) as exc:
        send_fn('-{}\r\n'.format(str(exc)))
    except KeyError:
        not_found(send_fn, cmd)
    except Exception:
        err(send_fn, traceback.format_exc())
    else:
        transmit(send_fn, result)


def transmit(send_fn, result):
    if result is None:
        send_fn('$-1\r\n')
    elif isinstance(result, int):
        send_fn(':{}\r\n'.format(result))
    elif isinstance(result, basestring):
        send_fn('${}\r\n{}\r\n'.format(len(result), result))
    elif isinstance(result, (set, list, tuple)):
        send_fn('*{}\r\n'.format(len(result)))
        for element in result:
            transmit(send_fn, element)



class CommandHandler(asyncore.dispatcher_with_send):

    def handle_read(self):
        data = self.recv(8192)
        print('data = {}'.format(repr(data)))
        if not data:
            return
        cmd = parse_instructions(data)
        execute_cmd(self.debug_send, *cmd)
        print('')

    def debug_send(self, *args):
        print("out={}".format(repr(args)))
        return self.send(*args)


class RedisServer(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(1024)

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            print 'Incoming connection from %s' % repr(addr)
            CommandHandler(sock)
            sys.stdout.flush()
            sys.stderr.flush()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 6377

    keyspace = DiskKeyspace()
    keyspace.flushall()

    RedisServer('127.0.0.1', port)
    print 'PID: {}'.format(os.getpid())
    print 'Ready to accept connections'
    sys.stdout.flush()
    asyncore.loop()
