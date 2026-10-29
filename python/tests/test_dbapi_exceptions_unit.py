"""PEP 249's exceptions from the dbapi module (no server; the unreachable-server case needs the
library, ``make -C go lib``).

The module exposes Error, Warning and the rest as PEP 249 requires, and they are the driver
manager's own classes, so code written against either catches the same errors.
"""

from __future__ import annotations

import socket

import adbc_driver_manager
import adbc_driver_manager.dbapi
import pytest

from adbc_driver_chargewire import DatabaseOptions, dbapi

PEP_249_EXCEPTIONS = [
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


@pytest.mark.parametrize("name", PEP_249_EXCEPTIONS)
def test_every_pep_249_exception_is_exported(name):
    exported = getattr(dbapi, name)
    assert exported is getattr(adbc_driver_manager.dbapi, name)
    assert issubclass(exported, Exception)


def test_the_pep_249_hierarchy():
    assert issubclass(dbapi.InterfaceError, dbapi.Error)
    assert issubclass(dbapi.DatabaseError, dbapi.Error)
    for name in (
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ):
        assert issubclass(getattr(dbapi, name), dbapi.DatabaseError), name
    # Warning is not an Error: catching Error never swallows a warning.
    assert not issubclass(dbapi.Warning, dbapi.Error)


def _unused_port() -> int:
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_a_real_failure_is_caught_by_the_modules_classes():
    with pytest.raises(expected_exception=dbapi.Error) as caught:
        dbapi.connect(
            uri=f"chargewire://127.0.0.1:{_unused_port()}?tls=false",
            username="u",
            password="p",
            db_kwargs={DatabaseOptions.RETRY_MAX_ATTEMPTS.value: "1"},
        )
    assert isinstance(caught.value, dbapi.DatabaseError), type(caught.value)
    assert "[chargewire]" in str(caught.value)
