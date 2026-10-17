"""The built shared library through the ADBC driver manager's C API, with no server:
it loads, takes and refuses options, and reports errors with their status and details.
These need the library (``make -C go lib``) but no server."""

from __future__ import annotations

import socket

import adbc_driver_manager
import pytest

from adbc_driver_warpsql import DatabaseOptions, StatementOptions, dbapi
from adbc_driver_warpsql._driver import _driver_path


def _unused_port() -> int:
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_library_loads_and_takes_options():
    with adbc_driver_manager.AdbcDatabase(
        driver=_driver_path(),
        uri="warp://127.0.0.1:1?tls=false",
        username="u",
        password="p",
        **{StatementOptions.TIMEOUT_SECONDS.value: "2.5"},
    ) as db:
        assert db.get_option(key="uri") == "warp://127.0.0.1:1?tls=false"
        assert db.get_option(key=StatementOptions.TIMEOUT_SECONDS.value) == "2.5"
        with pytest.raises(adbc_driver_manager.Error):
            db.get_option(key="password")  # secrets are write-only


@pytest.mark.parametrize(
    "options",
    [
        {"uri": "grpc://127.0.0.1:1"},
        {"uri": "warp://h:1", DatabaseOptions.RETRY_MAX_ATTEMPTS.value: "0"},
        {"uri": "warp://h:1", "adbc.warp.no_such_option": "1"},
    ],
)
def test_bad_options_are_refused(options):
    with pytest.raises(adbc_driver_manager.Error):
        adbc_driver_manager.AdbcDatabase(driver=_driver_path(), **options)


def test_unreachable_server_is_an_io_error():
    with pytest.raises(adbc_driver_manager.Error) as caught:
        dbapi.connect(
            uri=f"warp://127.0.0.1:{_unused_port()}?tls=false",
            username="u",
            password="p",
            db_kwargs={DatabaseOptions.RETRY_MAX_ATTEMPTS.value: "1"},
        )
    assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.IO
    assert "[warpsql]" in str(caught.value)
