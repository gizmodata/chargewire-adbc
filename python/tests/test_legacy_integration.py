"""Live tests: connection strings saved before the protocol was renamed to Chargewire.

The old ``warp://`` URI scheme and the old ``adbc.warp.*`` option keys still connect, and an
option set under its old key takes effect exactly as under its current one.
"""

from __future__ import annotations

import adbc_driver_manager
import pytest
from conftest import USERNAME

from adbc_driver_chargewire import StatementOptions, dbapi

pytestmark = pytest.mark.integration

SLOW_QUERY = "SELECT sum(a.range * b.range) FROM range(100000000) a, range(100000) b"


def error_code(error: adbc_driver_manager.Error) -> str | None:
    for key, value in error.details:
        if key == b"chargewire.code":
            return value.decode()
    return None


def legacy_uri(server) -> str:
    return f"warp://{server.host}:{server.port}?tls=false"


def test_old_scheme_and_database_option(chargewire_server):
    with dbapi.connect(
        uri=legacy_uri(server=chargewire_server),
        username=USERNAME,
        password=chargewire_server.password,
        db_kwargs={"adbc.warp.statement.timeout_seconds": "0.3"},
    ) as conn:
        assert conn.adbc_database.get_option(key=StatementOptions.TIMEOUT_SECONDS.value) == "0.3"
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 42 AS answer")
            assert cur.fetch_arrow_table().column("answer")[0].as_py() == 42
        # The old key's value is in force: the server stops a long statement after 0.3 s.
        with conn.cursor() as cur:
            with pytest.raises(expected_exception=adbc_driver_manager.Error) as caught:
                cur.execute(operation=SLOW_QUERY)
                cur.fetchall()
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.TIMEOUT
            assert error_code(error=caught.value) == "statement_timeout", caught.value


def test_old_statement_option(chargewire_server):
    with dbapi.connect(
        uri=legacy_uri(server=chargewire_server),
        username=USERNAME,
        password=chargewire_server.password,
    ) as conn:
        with conn.cursor(adbc_stmt_kwargs={"adbc.warp.statement.timeout_seconds": "0.3"}) as cur:
            timeout = cur.adbc_statement.get_option(key=StatementOptions.TIMEOUT_SECONDS.value)
            assert timeout == "0.3"
            with pytest.raises(expected_exception=adbc_driver_manager.Error) as caught:
                cur.execute(operation=SLOW_QUERY)
                cur.fetchall()
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.TIMEOUT
            assert error_code(error=caught.value) == "statement_timeout", caught.value
