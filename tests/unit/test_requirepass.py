import pytest

from dredis import config
from dredis.commands import run_command
from dredis.exceptions import AuthenticationRequiredError, DredisError
from dredis.keyspace import Keyspace


def test_raises_error_if_not_authenticated(keyspace):
    config.set('requirepass', 'test')
    with pytest.raises(AuthenticationRequiredError) as exc:
        run_command(Keyspace(), 'get', ('test',))

    assert str(exc.value) == 'NOAUTH Authentication required.'


def test_raises_error_if_password_is_wrong(keyspace):
    config.set('requirepass', 'test')
    k = Keyspace()
    with pytest.raises(DredisError) as exc:
        k.auth('wrongpass')

    assert str(exc.value) == 'ERR invalid password'


def test_allows_commands_when_password_is_valid(keyspace):
    config.set('requirepass', 'secret')
    k = Keyspace()

    assert run_command(k, 'auth', ('secret',))
    assert run_command(k, 'incrby', ('counter', '1')) == 1


def test_bad_authentication_when_authenticated_should_invalidate_the_session(keyspace):
    config.set('requirepass', 'secret')
    k = Keyspace()

    assert run_command(k, 'auth', ('secret',))
    try:
        run_command(k, 'auth', ('wrongpass',))
    except DredisError:
        pass

    with pytest.raises(AuthenticationRequiredError) as exc:
        run_command(k, 'incrby', ('counter', '1'))

    assert str(exc.value) == 'NOAUTH Authentication required.'


def test_should_raise_error_when_authenticating_when_there_is_no_password(keyspace):
    with pytest.raises(DredisError) as exc:
        run_command(keyspace, 'auth', ('secret',))

    assert str(exc.value) == 'ERR client sent AUTH, but no password is set'
