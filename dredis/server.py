#!/Users/hugo/.virtualenvs/dredis/bin/python

import asyncore
import json
import logging
import os.path
import socket
import sys
import tempfile
import traceback

from dredis.commands import run_command, SimpleString, CommandNotFound
from dredis.keyspace import DiskKeyspace
from dredis.lua import RedisScriptError
from dredis.parser import Parser


logger = logging.getLogger('dredis')


def not_found(send_fn, cmd):
    err(send_fn, "unknown command '{}'".format(cmd))


def err(send_fn, msg):
    send_fn("-ERR {}\r\n".format(msg))


def error(send_fn, msg):
    send_fn('-{}\r\n'.format(msg))


def execute_cmd(keyspace, send_fn, cmd, *args):
    logger.debug('cmd={}, args={}'.format(repr(cmd), repr(args)))
    try:
        result = run_command(keyspace, cmd, args)
    except (ValueError, RedisScriptError) as exc:
        error(send_fn, str(exc))
    except CommandNotFound:
        not_found(send_fn, cmd)
    except SyntaxError as exc:
        err(send_fn, str(exc))
    except Exception:
        err(send_fn, json.dumps(traceback.format_exc()))
    else:
        transmit(send_fn, result)


def transmit(send_fn, result):
    if result is None:
        send_fn('$-1\r\n')
    elif isinstance(result, int):
        send_fn(':{}\r\n'.format(result))
    elif isinstance(result, SimpleString):
        send_fn('+{}\r\n'.format(result))
    elif isinstance(result, basestring):
        send_fn('${}\r\n{}\r\n'.format(len(result), result))
    elif isinstance(result, (set, list, tuple)):
        send_fn('*{}\r\n'.format(len(result)))
        for element in result:
            transmit(send_fn, element)
    else:
        assert False, 'couldnt catch a response for {} (type {})'.format(repr(result), type(result))


class CommandHandler(asyncore.dispatcher_with_send):

    def handle_read(self):
        parser = Parser(self.recv)
        cmd = parser.get_instructions()
        logger.debug('{} data = {}'.format(self.addr, repr(cmd)))
        if not cmd:
            return
        execute_cmd(self.keyspace, self.debug_send, *cmd)

    def debug_send(self, *args):
        logger.debug("out={}".format(repr(args)))
        return self.send(*args)

    def handle_close(self):
        self.close()
        del KEYSPACES[self.addr]

    @property
    def keyspace(self):
        if self.addr not in KEYSPACES:
            KEYSPACES[self.addr] = DiskKeyspace(ROOT_DIR)
        return KEYSPACES[self.addr]


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
            logger.debug('Incoming connection from %s' % repr(addr))
            CommandHandler(sock)


def setup_logging():
    if DEBUG:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


KEYSPACES = {}


if __name__ == '__main__':
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = int(os.environ.get('DREDIS_PORT', '6377'))

    root_dir_env = os.environ.get('ROOT_DIR')
    if root_dir_env:
        ROOT_DIR = root_dir_env
    else:
        ROOT_DIR = tempfile.mkdtemp(prefix="redis-test-")

    DEBUG = os.environ.get('DEBUG', '1') == '1'

    setup_logging()

    keyspace = DiskKeyspace(ROOT_DIR)
    if os.environ.get('FLUSHALL_ON_STARTUP', '0') == '1':
        keyspace.flushall()
    else:
        keyspace.setup_directories()

    RedisServer('127.0.0.1', port)

    logger.info("Port: {}".format(port))
    logger.info("Root directory: {}".format(ROOT_DIR))
    logger.info('PID: {}'.format(os.getpid()))
    logger.info('Ready to accept connections')

    asyncore.loop()
