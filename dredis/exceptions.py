class DredisError(Exception):

    PREFIX = 'ERR'
    DEFAULT_MSG = 'dredis error'

    def __init__(self, msg=None):
        self.msg = self.DEFAULT_MSG if msg is None else msg

    def __str__(self):
        return self.error_msg

    @property
    def error_msg(self):
        if self.PREFIX:
            return '%s %s' % (self.PREFIX, self.msg)
        else:
            return self.msg


class AuthenticationRequiredError(DredisError):

    PREFIX = 'NOAUTH'
    DEFAULT_MSG = 'Authentication required.'


class CommandNotFound(DredisError):
    """Exception to flag not found Redis command"""

    DEFAULT_MSG = 'command not found'


class DredisSyntaxError(DredisError):
    """Exception used to flag a bad command signature"""

    DEFAULT_MSG = 'syntax error'


class BusyKeyError(DredisError):

    PREFIX = 'BUSYKEY'
    DEFAULT_MSG = 'Target key name already exists'


class NoKeyError(DredisError):

    DEFAULT_MSG = "no such key"


class RedisScriptError(DredisError):
    """Indicate error from calls to redis.call()"""

    PREFIX = ''  # Lua errors don't have the ERR prefix
    DEFAULT_MSG = '@user_script: lua error'
