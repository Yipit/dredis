#!/Users/hugo/.virtualenvs/dredis/bin/python

import asyncore
import shutil
import socket
import os.path
import uuid

import sys


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
    for value in values:
        keyspace.sadd(key, value)
    send_fn(":{}\r\n".format(len(values)))


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
        self.directory = '/tmp/red'

    def _key_path(self, key):
        return os.path.join(self.directory, key)

    # def flushall(self):
    #     shutil.rmtree(self.directory)

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
        fname = str(uuid.uuid4())
        with open(os.path.join(key_path, fname), 'w') as f:
            f.write(value)

    def smembers(self, key):
        result = set()
        key_path = self._key_path(key)
        if self.exists(key):
            for fname in os.listdir(key_path):
                with open(os.path.join(key_path, fname)) as f:
                    result.add(f.read())
        return result

    def scard(self, key):
        count = 0
        if self.exists(key):
            count = len(os.listdir(self._key_path(key)))
        return count

    def delete(self, key):
        if self.exists(key):
            os.remove(self._key_path(key))
            return 1
        else:
            return 0


keyspace = DiskKeyspace()

CMDS = {name: fn for (name, fn) in globals().items() if name.startswith("cmd_")}


def execute_cmd(send_fn, cmd, *args):
    print('cmd={}, args={}'.format(repr(cmd), repr(args)))
    CMDS[cmd.lower()](send_fn, *args)


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
        execute_cmd(self.send, *cmd)


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


if __name__ == '__main__':
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 6377

    RedisServer('127.0.0.1', port)
    print 'PID: {}'.format(os.getpid())
    print 'Ready to accept connections'
    sys.stdout.flush()
    asyncore.loop()
