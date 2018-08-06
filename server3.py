#!/Users/hugo/.virtualenvs/dredis/bin/python

import asyncore
import hashlib
import shutil
import socket
import os.path
import tempfile
import traceback
import uuid

import sys


def cmd_command(send_fn):
    send_fn("*{}\r\n".format(len(CMDS)))
    for cmd in CMDS:
        send_fn("${}{}\r\n".format(len(cmd), cmd.upper()))


def cmd_ping(send_fn, *args):
    send_fn('+PONG\r\n')


def cmd_set(send_fn, key, value, *args):
    keyspace.set(key, value)
    send_fn('+OK\r\n')


def cmd_get(send_fn, key):
    if keyspace.exists(key):
        value = keyspace.get(key)
        send_fn('${len}\r\n{value}\r\n'.format(len=len(value), value=value))
    else:
        send_fn("$-1\r\n")


def cmd_sadd(send_fn, key, *values):
    count = 0
    for value in values:
        count += keyspace.sadd(key, value)
    send_fn(":{}\r\n".format(count))


def cmd_smembers(send_fn, key):
    members = keyspace.smembers(key)
    send_fn("*{len}\r\n".format(len=len(members)))
    for member in members:
        send_fn("${len}\r\n{value}\r\n".format(len=len(member), value=member))


def cmd_select(send_fn, db):
    send_fn('+OK\r\n')


def cmd_del(send_fn, key):
    count = keyspace.delete(key)
    send_fn(':{}\r\n'.format(count))


def cmd_scard(send_fn, key):
    count = keyspace.scard(key)
    send_fn(':{}\r\n'.format(count))


def cmd_sismember(send_fn, key, value):
    result = keyspace.sismember(key, value)
    send_fn(':{}\r\n'.format(int(result)))


def not_found(send_fn, cmd):
    send_fn("-ERR unknown command '{}'\r\n".format(cmd))


# class Keyspace(object):
#
#     def __init__(self):
#         self.keys = {}
#
#     def exists(self, key):
#         return key in self.keys
#
#     def get(self, key):
#         return self.keys[key]
#
#     def set(self, key, value):
#         self.keys[key] = value
#
#     def sadd(self, key, value):
#         members = self.smembers(key)
#         members.add(value)
#         self.set(key, members)
#
#     def smembers(self, key):
#         if self.exists(key):
#             return self.get(key)
#         else:
#             return set()
#
#
# keyspace = Keyspace()


class DiskKeyspace(object):

    def __init__(self):
        self.keys = {}
        self.directory = tempfile.mkdtemp("redis-test-")

    def _key_path(self, key):
        return os.path.join(self.directory, key)

    def flushall(self):
        pass
        # try:
        #     shutil.rmtree(self.directory)
        # except:
        #     pass
        # try:
        #     os.makedirs(self.directory)
        # except:
        #     pass

    def exists(self, key):
        return os.path.exists(self._key_path(key))

    def get(self, key):
        with open(self._key_path(key), 'r') as f:
            return f.read()

    def set(self, key, value):
        with open(self._key_path(key), 'w') as f:
            f.write(value)

    def sadd(self, key, value):
        key_path = self._key_path(key)
        if not self.exists(key):
            os.makedirs(key_path)
        fname = hashlib.md5(value).hexdigest()
        value_path = os.path.join(key_path, fname)
        if os.path.exists(value_path):
            return 0
        else:
            with open(value_path, 'w') as f:
                f.write(value)
            return 1

    def smembers(self, key):
        result = set()
        key_path = self._key_path(key)
        if self.exists(key):
            for fname in os.listdir(key_path):
                with open(os.path.join(key_path, fname)) as f:
                    result.add(f.read())
        return result

    def sismember(self, key, value):
        key_path = self._key_path(key)
        fname = hashlib.md5(value).hexdigest()
        value_path = os.path.join(key_path, fname)
        return os.path.exists(value_path)

    def scard(self, key):
        return len(self.smembers(key))

    def delete(self, key):
        if self.exists(key):
            key_path = self._key_path(key)
            if os.path.isfile(key_path):
                os.remove(self._key_path(key))
            else:
                shutil.rmtree(key_path)
            return 1
        else:
            return 0


keyspace = DiskKeyspace()

CMDS = {name[len("cmd_"):]: fn for (name, fn) in globals().items() if name.startswith("cmd_")}


def err(send_fn, tb):
    send_fn("-Server exception: {}\r\n".format(tb))


def execute_cmd(send_fn, cmd, *args):
    print('cmd={}, args={}'.format(repr(cmd), repr(args)))
    try:
        CMDS[cmd.lower()](send_fn, *args)
    except KeyError:
        not_found(send_fn, cmd)
    except Exception as e:
        err(send_fn, traceback.format_exc())


class CommandHandler(asyncore.dispatcher_with_send):

    def handle_read(self):
        data = self.recv(8192)
        print('data = {}'.format(repr(data)))
        if not data:
            return
        lines = data.splitlines()
        l = lines.pop(0)
        cmd = []
        if l.startswith('*'):
            for i in range(int(l[1:])):
                lines.pop(0)
                cmd.append(lines.pop(0))
        else:
            cmd = [l]
        execute_cmd(self.debug_send, *cmd)

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
