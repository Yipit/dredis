import argparse
import asyncore
import errno
import json
import logging
import os.path
import socket
import tempfile
import traceback

import sys

from dredis import __version__
from dredis.commands import run_command, SimpleString, CommandNotFound
from dredis.keyspace import Keyspace
from dredis.ldb import LEVELDB
from dredis.lua import RedisScriptError
from dredis.parser import Parser
from dredis.path import Path

logger = logging.getLogger('dredis')

KEYSPACES = {}
ROOT_DIR = None  # defined by `main()`


def not_found(send_fn, cmd):
    err(send_fn, "unknown command '{}'".format(cmd))


def err(send_fn, msg):
    send_fn("-ERR {}\r\n".format(msg))


def error(send_fn, msg):
    send_fn('-{}\r\n'.format(msg))


def execute_cmd(keyspace, send_fn, cmd, *args):
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
    to_send = []

    def _transform(elem):
        if elem is None:
            to_send.append('$-1\r\n')
        elif isinstance(elem, int):
            to_send.append(':{}\r\n'.format(elem))
        elif isinstance(elem, SimpleString):
            to_send.append('+{}\r\n'.format(elem))
        elif isinstance(elem, basestring):
            to_send.append('${}\r\n{}\r\n'.format(len(elem), elem))
        elif isinstance(elem, (set, list, tuple)):
            to_send.append('*{}\r\n'.format(len(elem)))
            for element in elem:
                _transform(element)
        else:
            assert False, 'couldnt catch a response for {} (type {})'.format(repr(result), type(result))

    _transform(result)
    send_fn(''.join(to_send))


class CommandHandler(asyncore.dispatcher):

    def __init__(self, *args, **kwargs):
        asyncore.dispatcher.__init__(self, *args, **kwargs)
        self._parser = Parser(self.recv)  # contains client message buffer

    def handle_read(self):
        try:
            for cmd in self._parser.get_instructions():
                logger.debug('{} data = {}'.format(self.addr, repr(cmd)))
                execute_cmd(self.keyspace, self.debug_send, *cmd)
        except socket.error as exc:
            # try again later if no data is available
            if exc.errno == errno.EAGAIN:
                return
            else:
                raise

    def debug_send(self, *args):
        logger.debug("out={}".format(repr(args)))
        return self.send(*args)

    def handle_close(self):
        logger.debug("closing {}".format(self.addr))
        self.close()
        if self.addr in KEYSPACES:
            del KEYSPACES[self.addr]

    @property
    def keyspace(self):
        if self.addr not in KEYSPACES:
            KEYSPACES[self.addr] = Keyspace()
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
            # disable tcp delay (Nagle's algorithm):
            # https://en.wikipedia.org/wiki/Nagle%27s_algorithm#Interactions_with_real-time_systems
            # Redis does the same thing, it seems to be a common practice to send data as soon as possible.
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            logger.debug('Incoming connection from %s' % repr(addr))
            CommandHandler(sock)


def setup_logging(level):
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    parser = argparse.ArgumentParser(version=__version__)
    parser.add_argument('--host', default='127.0.0.1', help='server host (defaults to %(default)s)')
    parser.add_argument('--port', default='6377', type=int, help='server port (defaults to %(default)s)')
    parser.add_argument('--dir', default=None,
                        help='directory to save data (defaults to a temporary directory)')
    parser.add_argument('--debug', action='store_true', help='enable debug logs')
    parser.add_argument('--flushall', action='store_true', default=False, help='run FLUSHALL on startup')
    args = parser.parse_args()

    global ROOT_DIR
    if args.dir:
        ROOT_DIR = Path(args.dir)
        ROOT_DIR.makedirs(ignore_if_exists=True)

    else:
        ROOT_DIR = tempfile.mkdtemp(prefix="redis-test-")

    if args.debug:
        setup_logging(logging.DEBUG)
    else:
        setup_logging(logging.INFO)

    LEVELDB.setup_dbs(ROOT_DIR)
    keyspace = Keyspace()
    if args.flushall:
        keyspace.flushall()

    RedisServer(args.host, args.port)

    logger.info("Port: {}".format(args.port))
    logger.info("Root directory: {}".format(ROOT_DIR))
    logger.info('PID: {}'.format(os.getpid()))
    logger.info('Ready to accept connections')

    try:
        asyncore.loop(use_poll=True)
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == '__main__':
    main()
