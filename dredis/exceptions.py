class DredisError(Exception):

    def __init__(self, msg):
        self.msg = 'ERR %s' % msg

    def __str__(self):
        return self.msg


class AuthenticationRequiredError(DredisError):

    def __init__(self):
        self.msg = 'NOAUTH Authentication required.'


class CommandNotFound(DredisError):
    """Exception to flag not found Redis command"""


class DredisSyntaxError(DredisError):
    """Exception used to flag a bad command signature"""

    def __init__(self, msg='syntax error'):
        super(DredisSyntaxError, self).__init__(msg)
