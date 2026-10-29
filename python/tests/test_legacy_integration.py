"""Live tests: connection strings and servers from before the protocol was renamed to
Chargewire.

The old ``warp://`` URI scheme and the old ``adbc.warp.*`` option keys still connect, and an
option set under its old key takes effect exactly as under its current one. A server from
before the rename (``warp-duckdb``, named by ``CHARGEWIRE_ADBC_TEST_LEGACY_SERVER_BINARY``;
those tests skip without it) is spoken to in its own spelling, and a current server is never
asked for the old one.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import adbc_driver_manager
import pyarrow as pa
import pytest
from conftest import USERNAME, start_server, stop_server

from adbc_driver_chargewire import StatementOptions, dbapi

LEGACY_SERVER_BINARY_ENV = "CHARGEWIRE_ADBC_TEST_LEGACY_SERVER_BINARY"

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


@pytest.fixture(scope="module")
def legacy_server(tmp_path_factory):
    """A server from before the rename: discovery at /warp, password in WARP_DUCKDB_PASSWORD."""
    binary = os.environ.get(LEGACY_SERVER_BINARY_ENV)
    if not binary or not os.path.exists(binary):
        pytest.skip(reason=f"set {LEGACY_SERVER_BINARY_ENV} to a warp-duckdb binary")
    server = start_server(
        log=tmp_path_factory.mktemp("legacy") / "warp-duckdb.log",
        binary=binary,
        password_env="WARP_DUCKDB_PASSWORD",
        root="/warp",
    )
    yield server
    stop_server(server=server)


def connect_legacy(server, uri: str | None = None, **kwargs):
    return dbapi.connect(
        uri=uri or server.uri, username=USERNAME, password=server.password, **kwargs
    )


class TestLegacyServer:
    @pytest.mark.parametrize(argnames="spelling", argvalues=["current", "old"])
    def test_connect_and_query(self, legacy_server, spelling):
        uri = legacy_server.uri
        if spelling == "old":
            uri = f"warp://{legacy_server.host}:{legacy_server.port}/warp?tls=false"
        with connect_legacy(server=legacy_server, uri=uri) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 42 AS answer")
                assert cur.fetch_arrow_table().column("answer")[0].as_py() == 42

    def test_prepared_statement_with_parameters(self, legacy_server):
        with connect_legacy(server=legacy_server) as conn:
            with conn.cursor() as cur:
                cur.adbc_prepare(operation="SELECT ? * 2 AS doubled")
                for value in (1, 21):
                    cur.execute(operation="SELECT ? * 2 AS doubled", parameters=[value])
                    assert cur.fetch_arrow_table().column("doubled")[0].as_py() == value * 2

    def test_bulk_ingest(self, legacy_server):
        table = pa.table(data={"k": [1, 2, 3], "v": ["alice", "bob", "carol"]})
        with connect_legacy(server=legacy_server) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="DROP TABLE IF EXISTS legacy_py_ingest")
                assert (
                    cur.adbc_ingest(table_name="legacy_py_ingest", data=table, mode="create") == 3
                )
                cur.execute(operation="SELECT v FROM legacy_py_ingest ORDER BY k")
                assert cur.fetch_arrow_table().column("v").to_pylist() == ["alice", "bob", "carol"]

    def test_sql_error_maps_to_its_class_and_code(self, legacy_server):
        with connect_legacy(server=legacy_server) as conn:
            with conn.cursor() as cur:
                with pytest.raises(expected_exception=dbapi.ProgrammingError) as caught:
                    cur.execute(operation="SELECT * FROM legacy_no_such_table")
                    cur.fetchall()
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_FOUND
        assert error_code(error=caught.value) == "sql_error", caught.value
        assert caught.value.sqlstate == "42P01", caught.value
        assert "legacy_no_such_table" in str(caught.value)

    def test_statement_timeout_option(self, legacy_server):
        with connect_legacy(server=legacy_server) as conn:
            with conn.cursor(
                adbc_stmt_kwargs={StatementOptions.TIMEOUT_SECONDS.value: "0.3"}
            ) as cur:
                with pytest.raises(expected_exception=adbc_driver_manager.Error) as caught:
                    cur.execute(operation=SLOW_QUERY)
                    cur.fetchall()
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.TIMEOUT
        assert error_code(error=caught.value) == "statement_timeout", caught.value

    def test_metadata(self, legacy_server):
        with connect_legacy(server=legacy_server) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    operation="CREATE OR REPLACE TABLE legacy_py_meta "
                    "(id INTEGER PRIMARY KEY, note VARCHAR)"
                )
            objects = (
                conn.adbc_get_objects(depth="columns", table_name_filter="legacy_py_meta")
                .read_all()
                .to_pylist()
            )
            tables = [
                table
                for catalog in objects
                for schema in catalog["catalog_db_schemas"] or []
                for table in schema["db_schema_tables"] or []
            ]
            assert [table["table_name"] for table in tables] == ["legacy_py_meta"]
            assert [c["column_name"] for c in tables[0]["table_columns"]] == ["id", "note"]
            schema = conn.adbc_get_table_schema(table_name="legacy_py_meta")
            assert schema.names == ["id", "note"]


def test_current_server_is_never_asked_for_the_old_spelling(chargewire_server, tmp_path):
    """Every byte a connection sends to a current server, through a recording proxy, names
    only the current spelling."""
    log = tmp_path / "sent.bin"
    log.write_bytes(data=b"")
    recorder = subprocess.Popen(
        args=[
            sys.executable,
            str(pathlib.Path(__file__).with_name("tcp_recorder.py")),
            str(chargewire_server.port),
            str(log),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        port = int(recorder.stdout.readline().strip())
        with dbapi.connect(
            uri=f"chargewire://127.0.0.1:{port}?tls=false",
            username=USERNAME,
            password=chargewire_server.password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 42 AS answer")
                assert cur.fetch_arrow_table().column("answer")[0].as_py() == 42
                cur.adbc_ingest(
                    table_name="legacy_py_proxy", data=pa.table(data={"k": [1]}), mode="replace"
                )
    finally:
        recorder.kill()
        recorder.wait()
    sent = log.read_bytes()
    assert b"GET /chargewire HTTP/1.1" in sent
    assert sent.count(b"GET /chargewire HTTP/1.1") == 1
    assert b"/warp" not in sent
    assert b"warp-" not in sent.lower()
