#!/Users/hugo/.virtualenvs/dredis/bin/python

import asyncore
import json
import os.path
import socket
import sys
import traceback

from dredis.keyspace import RedisScriptError, DiskKeyspace
from dredis.parser import parse_instructions


CMDS = {}


def command(cmd_name):
    def decorator(fn):
        CMDS[cmd_name] = fn
        return fn
    return decorator


@command('COMMAND')
def cmd_command(send_fn):
    send_fn("*{}\r\n".format(len(CMDS)))
    for cmd in CMDS:
        send_fn("${}{}\r\n".format(len(cmd), cmd.upper()))


@command('PING')
def cmd_ping(send_fn, *args):
    send_fn('+PONG\r\n')


@command('SET')
def cmd_set(send_fn, key, value, *args):
    keyspace.set(key, value)
    send_fn('+OK\r\n')


@command('GET')
def cmd_get(send_fn, key):
    if keyspace.exists(key):
        value = keyspace.get(key)
        send_fn('${len}\r\n{value}\r\n'.format(len=len(value), value=value))
    else:
        send_fn("$-1\r\n")


@command('SADD')
def cmd_sadd(send_fn, key, *values):
    count = 0
    for value in values:
        count += keyspace.sadd(key, value)
    send_fn(":{}\r\n".format(count))


@command('SMEMBERS')
def cmd_smembers(send_fn, key):
    members = keyspace.smembers(key)
    send_fn("*{len}\r\n".format(len=len(members)))
    for member in members:
        send_fn("${len}\r\n{value}\r\n".format(len=len(member), value=member))


@command('SELECT')
def cmd_select(send_fn, db):
    send_fn('+OK\r\n')


@command('FLUSHALL')
def cmd_flushall(send_fn):
    keyspace.flushall()
    send_fn('+OK\r\n')


@command('DEL')
def cmd_del(send_fn, key):
    count = keyspace.delete(key)
    send_fn(':{}\r\n'.format(count))


@command('SCARD')
def cmd_scard(send_fn, key):
    count = keyspace.scard(key)
    send_fn(':{}\r\n'.format(count))


@command('ISMEMBER')
def cmd_sismember(send_fn, key, value):
    result = keyspace.sismember(key, value)
    send_fn(':{}\r\n'.format(int(result)))


@command('ZADD')
def cmd_zadd(send_fn, key, score, *values):
    count = 0
    for value in values:
        count += keyspace.zadd(key, score, value)
    send_fn(":{}\r\n".format(count))


@command('EVAL')
def cmd_eval(send_fn, script, numkeys, *keys):
    try:
        result = keyspace.eval(script, numkeys, keys)
    except RedisScriptError as exc:
        send_fn('-{}\r\n'.format(str(exc)))
    else:
        # TODO: the return could be any type
        if isinstance(result, int):
            send_fn(":{}\r\n".format(result))
        elif isinstance(result, dict):
            send_fn('-{}\r\n'.format(result['err']))
        else:
            send_fn("+{}\r\n".format(result))


@command('ZRANGE')
def cmd_zrange(send_fn, key, start, stop, with_scores=False):
    members = keyspace.zrange(key, int(start), int(stop), bool(with_scores))
    send_fn("*{len}\r\n".format(len=len(members)))
    for member in members:
        send_fn("${len}\r\n{value}\r\n".format(len=len(member), value=member))


@command('ZCARD')
def cmd_zcard(send_fn, key):
    send_fn(':{}\r\n'.format(keyspace.zcard(key)))


def not_found(send_fn, cmd):
    send_fn("-ERR unknown command '{}'\r\n".format(cmd))


def err(send_fn, tb):
    send_fn("-Server exception: {}\r\n".format(json.dumps(tb)))


def execute_cmd(send_fn, cmd, *args):
    print('cmd={}, args={}'.format(repr(cmd), repr(args)))
    try:
        CMDS[cmd.upper()](send_fn, *args)
    except KeyError:
        not_found(send_fn, cmd)
    except Exception as e:
        err(send_fn, traceback.format_exc())


keyspace = DiskKeyspace()


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

    keyspace.flushall()

    RedisServer('127.0.0.1', port)
    print 'PID: {}'.format(os.getpid())
    print 'Ready to accept connections'
    sys.stdout.flush()
    asyncore.loop()
