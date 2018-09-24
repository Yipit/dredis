import argparse
import asyncore
import json
import logging
import os.path
import socket
import tempfile
import traceback

import sys

from dredis import __version__
from dredis.commands import run_command, SimpleString, CommandNotFound
from dredis.keyspace import DiskKeyspace
from dredis.lua import RedisScriptError
from dredis.parser import Parser


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


class CommandHandler(asyncore.dispatcher):

    def handle_read(self):
        parser = Parser(self.recv)
        for cmd in parser.get_instructions():
            logger.debug('{} data = {}'.format(self.addr, repr(cmd)))
            execute_cmd(self.keyspace, self.debug_send, *cmd)

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
        ROOT_DIR = args.dir
    else:
        ROOT_DIR = tempfile.mkdtemp(prefix="redis-test-")

    if args.debug:
        setup_logging(logging.DEBUG)
    else:
        setup_logging(logging.INFO)

    keyspace = DiskKeyspace(ROOT_DIR)
    if args.flushall:
        keyspace.flushall()
    else:
        keyspace.setup_directories()

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
