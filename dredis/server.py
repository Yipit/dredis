import argparse
import asyncore
import errno
import json
import logging
import os.path
import socket
import tempfile
import time
import traceback

import sys

from dredis import __version__
from dredis import db, rdb, config
from dredis.commands import run_command, SimpleString
from dredis.exceptions import DredisError
from dredis.keyspace import Keyspace, to_float_string
from dredis.parser import Parser
from dredis.path import Path

logger = logging.getLogger('dredis')

ROOT_DIR = None  # defined by `main()`


def execute_cmd(keyspace, send_fn, cmd, *args):
    try:
        result = run_command(keyspace, cmd, args)
    except DredisError as exc:
        transmit(send_fn, exc)
    except Exception as exc:
        # no tests cover this part because it's meant for internal errors,
        # such as unexpected bugs in dredis.
        transmit(send_fn, Exception(traceback.format_exc()))
        logger.exception(str(exc))
    else:
        transmit(send_fn, result)


def transform(obj):
    result = []

    def _transform(elem):
        if elem is None:
            result.append('$-1\r\n')
        elif isinstance(elem, int):
            result.append(':{}\r\n'.format(elem))
        elif isinstance(elem, float):
            elem_as_string = to_float_string(elem)
            result.append('${}\r\n{}\r\n'.format(len(elem_as_string), elem_as_string))
        elif isinstance(elem, SimpleString):
            result.append('+{}\r\n'.format(elem))
        elif isinstance(elem, basestring):
            result.append('${}\r\n{}\r\n'.format(len(elem), elem))
        elif isinstance(elem, (set, list, tuple)):
            result.append('*{}\r\n'.format(len(elem)))
            for element in elem:
                _transform(element)
        elif isinstance(elem, DredisError):
            result.append('-{}\r\n'.format(str(elem)))
        elif isinstance(elem, Exception):
            result.append('-INTERNALERROR {}\r\n'.format(str(elem)))
        else:
            assert False, 'couldnt catch a response for {} (type {})'.format(repr(elem), type(elem))

    _transform(obj)
    return ''.join(result)


def transmit(send_fn, result):
    send_fn(transform(result))


class CommandHandler(asyncore.dispatcher):

    def __init__(self, *args, **kwargs):
        asyncore.dispatcher.__init__(self, *args, **kwargs)
        self._parser = Parser(self.recv)  # contains client message buffer
        self.keyspace = Keyspace()

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


def main():
    parser = argparse.ArgumentParser(version=__version__)
    parser.add_argument('--host', default='127.0.0.1', help='server host (defaults to %(default)s)')
    parser.add_argument('--port', default='6377', type=int, help='server port (defaults to %(default)s)')
    parser.add_argument('--dir', default=None,
                        help='directory to save data (defaults to a temporary directory)')
    parser.add_argument('--backend', default=db.DEFAULT_DB_BACKEND, choices=db.DB_BACKENDS.keys(),
                        help='key/value database backend (defaults to %(default)s)')
    parser.add_argument('--backend-option', action='append',
                        help='database backend options (e.g., --backend-option map_size=BYTES)')
    parser.add_argument('--rdb', default=None, help='RDB file to seed dredis')
    # boolean arguments
    parser.add_argument('--debug', action='store_true', help='enable debug logs')
    parser.add_argument('--flushall', action='store_true', default=False, help='run FLUSHALL on startup')
    parser.add_argument('--readonly', action='store_true', help='accept read-only commands')
    parser.add_argument('--requirepass', default='',
                        help='require clients to issue AUTH <password> before processing any other commands')
    args = parser.parse_args()

    global ROOT_DIR
    if args.dir:
        ROOT_DIR = Path(args.dir)
        ROOT_DIR.makedirs(ignore_if_exists=True)

    else:
        ROOT_DIR = tempfile.mkdtemp(prefix="redis-test-")

    if args.debug:
        config.set('debug', 'true')

    if args.readonly:
        config.set('readonly', 'true')

    if args.requirepass:
        config.set('requirepass', args.requirepass)

    db_backend_options = {}
    if args.backend_option:
        for option in args.backend_option:
            if '=' not in option:
                logger.error('Expected `key=value` pairs for --backend-option parameter')
                sys.exit(1)
            key, value = map(str.strip, option.split('='))
            db_backend_options[key] = json.loads(value)
    db.DB_MANAGER.setup_dbs(ROOT_DIR, args.backend, db_backend_options)

    keyspace = Keyspace()
    if args.flushall:
        keyspace.flushall()

    if args.rdb:
        logger.info("Loading %s..." % args.rdb)
        start_time = time.time()
        with open(args.rdb, 'rb') as f:
            rdb.load_rdb(keyspace, f)
        logger.info("Finished loading (%.2f seconds)." % (time.time() - start_time))

    RedisServer(args.host, args.port)

    logger.info("Backend: {}".format(args.backend))
    logger.info("Port: {}".format(args.port))
    logger.info("Root directory: {}".format(ROOT_DIR))
    logger.info('PID: {}'.format(os.getpid()))
    logger.info('Readonly: {}'.format(config.get('readonly')))
    logger.info('Ready to accept connections')

    try:
        asyncore.loop(use_poll=True)
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == '__main__':
    main()
