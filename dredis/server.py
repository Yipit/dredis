import argparse
import logging
import os.path
import tempfile
import traceback

import sys

from twisted.internet import protocol, reactor

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


def execute_cmd(keyspace, send_fn, cmd, *args):
    try:
        result = run_command(keyspace, cmd, args)
    except (SyntaxError, CommandNotFound, ValueError, RedisScriptError) as exc:
        transmit(send_fn, exc)
    except Exception:
        # no tests cover this part because it's meant for internal errors,
        # such as unexpected bugs in dredis.
        transmit(send_fn, Exception(traceback.format_exc()))
    else:
        transmit(send_fn, result)


def transform(obj):
    result = []

    def _transform(elem):
        if elem is None:
            result.append('$-1\r\n')
        elif isinstance(elem, int):
            result.append(':{}\r\n'.format(elem))
        elif isinstance(elem, SimpleString):
            result.append('+{}\r\n'.format(elem))
        elif isinstance(elem, basestring):
            result.append('${}\r\n{}\r\n'.format(len(elem), elem))
        elif isinstance(elem, (set, list, tuple)):
            result.append('*{}\r\n'.format(len(elem)))
            for element in elem:
                _transform(element)
        elif isinstance(elem, Exception):
            result.append('-ERR {}\r\n'.format(str(elem)))
        else:
            assert False, 'couldnt catch a response for {} (type {})'.format(repr(elem), type(elem))

    _transform(obj)
    return ''.join(result)


def transmit(send_fn, result):
    send_fn(transform(result))


class RedisProtocol(protocol.Protocol):

    def __init__(self):
        self._data = ''
        self._parser = Parser(self.read_data)  # contains client message buffer

    def connectionMade(self):
        protocol.Protocol.connectionMade(self)
        self.transport.setTcpNoDelay(True)

    def dataReceived(self, data):
        self._data = data
        for cmd in self._parser.get_instructions():
            logger.debug('{} data = {}'.format(self.transport.client, repr(cmd)))
            execute_cmd(self.keyspace, self.debug_send, *cmd)

    def read_data(self, bytes):
        data = self._data
        self._data = ''
        return data

    def debug_send(self, data):
        logger.debug("out={}".format(repr(data)))
        return self.transport.write(data)

    def connectionLost(self, *args):
        logger.debug("closing {}".format(repr(self.transport.client)))
        if self.transport.client in KEYSPACES:
            del KEYSPACES[self.transport.client]

    @property
    def keyspace(self):
        if self.transport.client not in KEYSPACES:
            KEYSPACES[self.transport.client] = Keyspace()
        return KEYSPACES[self.transport.client]


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

    # RedisServer(args.host, args.port)
    factory = protocol.ServerFactory()
    factory.protocol = RedisProtocol
    reactor.listenTCP(args.port, factory)

    logger.info("Port: {}".format(args.port))
    logger.info("Root directory: {}".format(ROOT_DIR))
    logger.info('PID: {}'.format(os.getpid()))
    logger.info('Ready to accept connections')

    try:
        reactor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == '__main__':
    main()
