"""The built shared library through the ADBC driver manager's C API, with no server:
it loads, takes and refuses options, and reports errors with their status and details.
These need the library (``make -C go lib``) but no server."""

from __future__ import annotations

import socket

import adbc_driver_manager
import pytest

from adbc_driver_chargewire import DatabaseOptions, StatementOptions, dbapi
from adbc_driver_chargewire._driver import _driver_path


def _unused_port() -> int:
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_library_loads_and_takes_options():
    with adbc_driver_manager.AdbcDatabase(
        driver=_driver_path(),
        uri="chargewire://127.0.0.1:1?tls=false",
        username="u",
        password="p",
        **{StatementOptions.TIMEOUT_SECONDS.value: "2.5"},
    ) as db:
        assert db.get_option(key="uri") == "chargewire://127.0.0.1:1?tls=false"
        assert db.get_option(key=StatementOptions.TIMEOUT_SECONDS.value) == "2.5"
        with pytest.raises(adbc_driver_manager.Error):
            db.get_option(key="password")  # secrets are write-only


@pytest.mark.parametrize(
    "options",
    [
        {"uri": "grpc://127.0.0.1:1"},
        {"uri": "chargewire://h:1", DatabaseOptions.RETRY_MAX_ATTEMPTS.value: "0"},
        {"uri": "chargewire://h:1", "adbc.chargewire.no_such_option": "1"},
    ],
)
def test_bad_options_are_refused(options):
    with pytest.raises(adbc_driver_manager.Error):
        adbc_driver_manager.AdbcDatabase(driver=_driver_path(), **options)


def test_unreachable_server_is_an_io_error():
    with pytest.raises(adbc_driver_manager.Error) as caught:
        dbapi.connect(
            uri=f"chargewire://127.0.0.1:{_unused_port()}?tls=false",
            username="u",
            password="p",
            db_kwargs={DatabaseOptions.RETRY_MAX_ATTEMPTS.value: "1"},
        )
    assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.IO
    assert "[chargewire]" in str(caught.value)


@pytest.mark.parametrize(
    "options",
    [
        {"username": "u"},  # a certificate sign-in takes no password or token
        {DatabaseOptions.ACCESS_TOKEN.value: "tok"},
        {DatabaseOptions.TLS_CLIENT_CERT.value: ""},  # and needs a certificate
        {DatabaseOptions.AUTH_TYPE.value: "kerberos"},
    ],
)
def test_certificate_sign_in_options_are_checked(options):
    pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
    all_options = {
        "uri": "chargewire://127.0.0.1:1",
        DatabaseOptions.AUTH_TYPE.value: "client_certificate",
        DatabaseOptions.TLS_CLIENT_CERT.value: pem,
        DatabaseOptions.TLS_CLIENT_KEY.value: pem,
        **options,
    }
    with pytest.raises(adbc_driver_manager.ProgrammingError) as caught:
        adbc_driver_manager.AdbcDatabase(driver=_driver_path(), **all_options)
    assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT


def test_auth_type_reads_back():
    with adbc_driver_manager.AdbcDatabase(
        driver=_driver_path(),
        uri="chargewire://127.0.0.1:1?tls=false",
        **{DatabaseOptions.AUTH_TYPE.value: "password"},
    ) as db:
        assert db.get_option(key=DatabaseOptions.AUTH_TYPE.value) == "password"
