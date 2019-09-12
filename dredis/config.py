import fnmatch
import logging

from dredis.exceptions import DredisError

TRUE = 'true'
FALSE = 'false'
EMPTY = ''

_SERVER_CONFIG = {
    'debug': FALSE,
    'readonly': FALSE,
    'requirepass': EMPTY,
}


def get_all(pattern):
    result = []
    for option, value in sorted(_SERVER_CONFIG.items()):
        if not fnmatch.fnmatch(option, pattern):
            continue
        result.append(option)
        result.append(value)
    return result


def set(option, value):
    if option in _SERVER_CONFIG:
        if option == 'debug':
            value = _validate_bool(option, value)
            if value == TRUE:
                logging.getLogger('dredis').setLevel(logging.DEBUG)
            else:
                logging.getLogger('dredis').setLevel(logging.INFO)
        elif option == 'readonly':
            value = _validate_bool(option, value)
        _SERVER_CONFIG[option] = value
    else:
        raise DredisError('Unsupported CONFIG parameter: {}'.format(option))


def get(option):
    return _SERVER_CONFIG[option]


def _validate_bool(option, value):
    if value.lower() not in (TRUE, FALSE):
        raise DredisError("Invalid argument '{}' for CONFIG SET '{}'".format(value, option))
    return value.lower()
