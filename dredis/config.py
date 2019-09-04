import fnmatch

from dredis.exceptions import DredisError


_SERVER_CONFIG = {
    'debug': 'false',
    'readonly': 'false',
    'requirepass': '',
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
        _SERVER_CONFIG[option] = value
    else:
        raise DredisError('Unsupported CONFIG parameter: {}'.format(option))


def get(option):
    return _SERVER_CONFIG[option]
