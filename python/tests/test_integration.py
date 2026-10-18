"""Integration tests against a live Warp SQL server (see conftest.py).

Ported from gizmosql-adbc's Python suite; what does not apply to Warp SQL
(OAuth, Flight SQL URI schemes) is listed in docs/testing.md.
"""

from __future__ import annotations

import datetime
import decimal
import os

import adbc_driver_manager
import pyarrow as pa
import pytest
from conftest import (
    CLIENT_EMAIL,
    USERNAME,
    make_certificates,
    raw_request,
    start_server,
    stop_server,
)

from adbc_driver_warpsql import ConnectionOptions, StatementOptions, dbapi

pytestmark = pytest.mark.integration


def connect(server, **kwargs):
    return dbapi.connect(uri=server.uri, username=USERNAME, password=server.password, **kwargs)


def warp_code(error: adbc_driver_manager.Error) -> str | None:
    """The Warp SQL error code the driver attached to an error."""
    for key, value in error.details:
        if key == b"warp.code":
            return value.decode()
    return None


def assert_error(
    error: adbc_driver_manager.Error, status: adbc_driver_manager.AdbcStatusCode, code: str | None
) -> None:
    """Check an error's ADBC status and Warp SQL code (None: not from the server)."""
    assert error.status_code == status, error
    assert warp_code(error=error) == code, error


class TestPasswordAuth:
    def test_select_one(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 1 AS value")
            table = cur.fetch_arrow_table()
            assert table.num_rows == 1
            assert table.column("value")[0].as_py() == 1

    def test_server_version(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="SELECT version() AS version")
            version = cur.fetch_arrow_table().column("version")[0].as_py()
            assert isinstance(version, str) and version

    def test_parameterized_query(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                operation="SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                parameters=[24],
            )
            table = cur.fetch_arrow_table()
            assert table.num_rows == 1
            assert table.column("n_name")[0].as_py() == "UNITED STATES"

    def test_named_parameters(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                operation="SELECT n_name FROM nation WHERE n_nationkey = $key",
                parameters={"key": 6},
            )
            assert cur.fetchone() == ("FRANCE",)

    def test_typed_parameters(self, conn):
        """Arrow parameters keep exact types: decimals, timestamps, binary, NULL."""
        with conn.cursor() as cur:
            cur.execute(
                operation="SELECT ? AS d, ? AS ts, ? AS b, ? AS n",
                parameters=[
                    decimal.Decimal("123.45"),
                    datetime.datetime(2026, 10, 17, 1, 2, 3),
                    b"\x00\xff",
                    None,
                ],
            )
            row = cur.fetch_arrow_table().to_pylist()[0]
            assert row["d"] == decimal.Decimal("123.45")
            assert row["ts"] == datetime.datetime(2026, 10, 17, 1, 2, 3)
            assert row["b"] == b"\x00\xff"
            assert row["n"] is None

    def test_fetch_arrow_table_type(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 1 AS a, 'hello' AS b")
            table = cur.fetch_arrow_table()
            assert isinstance(table, pa.Table)
            assert table.schema.names == ["a", "b"]

    def test_multiple_rows(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="SELECT * FROM nation ORDER BY n_nationkey LIMIT 5")
            assert cur.fetch_arrow_table().num_rows == 5

    def test_many_batches(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="SELECT i FROM range(2000000) t(i)")
            table = cur.fetch_arrow_table()
            assert table.num_rows == 2_000_000
            assert table.column("i")[1_999_999].as_py() == 1_999_999

    def test_wrong_password(self, warp_server):
        with pytest.raises(adbc_driver_manager.Error) as caught:
            dbapi.connect(
                uri=warp_server.uri, username=USERNAME, password=warp_server.password + "x"
            )
        assert_error(
            error=caught.value,
            status=adbc_driver_manager.AdbcStatusCode.UNAUTHENTICATED,
            code="invalid_credentials",
        )


class TestExecuteDDLDML:
    """cursor.execute() runs DDL/DML on the server right away."""

    def test_create_insert_query_drop(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="CREATE OR REPLACE TABLE test_auto (id INT, name VARCHAR)")
            cur.execute(operation="INSERT INTO test_auto VALUES (1, 'alice')")
            cur.execute(operation="INSERT INTO test_auto VALUES (2, 'bob')")
            cur.execute(operation="SELECT id, name FROM test_auto ORDER BY id")
            table = cur.fetch_arrow_table()
            assert table.column("id").to_pylist() == [1, 2]
            assert table.column("name")[1].as_py() == "bob"
            cur.execute(operation="DROP TABLE test_auto")

    def test_description_none_after_ddl(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="CREATE OR REPLACE TABLE test_desc_ddl (id INT)")
            assert cur.description is None
            cur.execute(operation="DROP TABLE test_desc_ddl")
            assert cur.description is None

    def test_fetch_after_ddl_returns_empty(self, conn):
        """Fetching after DDL/DML returns an empty result instead of raising (as sqlite3
        and duckdb do); generic DB-API consumers fetch unconditionally."""
        with conn.cursor() as cur:
            cur.execute(operation="CREATE OR REPLACE TABLE test_fetch_ddl (id INT)")
            assert cur.fetchall() == []
            assert cur.fetchone() is None
            assert cur.fetchmany(size=10) == []
            cur.execute(operation="INSERT INTO test_fetch_ddl VALUES (1)")
            assert cur.fetchall() == []
            cur.execute(operation="SELECT 42 AS answer")
            assert cur.fetchall() == [(42,)]

    def test_rowcount_after_dml(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="CREATE OR REPLACE TABLE test_rowcount (id INT)")
            cur.execute(operation="INSERT INTO test_rowcount SELECT * FROM range(5)")
            assert cur.rowcount == 5
            assert cur.description is None

    def test_fetch_before_execute_still_raises(self, conn):
        with conn.cursor() as cur:
            with pytest.raises(adbc_driver_manager.ProgrammingError):
                cur.fetchall()

    def test_description_present_after_select(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 1 AS a, 'hello' AS b")
            assert len(cur.description) == 2

    def test_chaining(self, conn):
        with conn.cursor() as cur:
            assert cur.execute(operation="SELECT 1") is cur

    def test_syntax_error_has_its_code(self, conn):
        with conn.cursor() as cur:
            with pytest.raises(adbc_driver_manager.ProgrammingError) as caught:
                cur.execute(operation="SELEC 1")
            assert_error(
                error=caught.value,
                status=adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT,
                code="sql_error",
            )
            assert "Parser Error" in str(caught.value)


class TestExecuteUpdate:
    def test_create_insert_query_drop(self, conn):
        with conn.cursor() as cur:
            # The server reports no row count for DDL: -1, "unknown".
            assert cur.execute_update(query="CREATE OR REPLACE TABLE test_eu (id INT)") == -1
            assert cur.execute_update(query="INSERT INTO test_eu VALUES (1)") == 1
            assert cur.execute_update(query="INSERT INTO test_eu VALUES (2), (3)") == 2
            cur.execute(operation="SELECT count(*) FROM test_eu")
            assert cur.fetchone() == (3,)

    def test_update_returns_rows_affected(self, conn):
        with conn.cursor() as cur:
            cur.execute_update(query="CREATE OR REPLACE TABLE test_eu_upd (val INT)")
            cur.execute_update(query="INSERT INTO test_eu_upd VALUES (1), (2), (3)")
            assert cur.execute_update(query="DELETE FROM test_eu_upd WHERE val >= 2") == 2
            cur.execute(operation="SELECT val FROM test_eu_upd")
            assert cur.fetchall() == [(1,)]


class TestBulkIngest:
    def test_ingest_create(self, conn):
        table = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
        with conn.cursor() as cur:
            cur.execute(operation="DROP TABLE IF EXISTS test_ingest_create")
            assert cur.adbc_ingest(table_name="test_ingest_create", data=table, mode="create") == 3
            cur.execute(operation="SELECT id, name FROM test_ingest_create ORDER BY id")
            assert cur.fetch_arrow_table().to_pylist() == table.to_pylist()

    def test_ingest_append(self, conn):
        with conn.cursor() as cur:
            cur.execute_update(
                query="CREATE OR REPLACE TABLE test_ingest_append (id BIGINT, val DOUBLE)"
            )
            cur.adbc_ingest(
                table_name="test_ingest_append",
                data=pa.table({"id": [1, 2], "val": [10.0, 20.0]}),
                mode="append",
            )
            cur.adbc_ingest(
                table_name="test_ingest_append",
                data=pa.table({"id": [3, 4], "val": [30.0, 40.0]}),
                mode="append",
            )
            cur.execute(operation="SELECT id, val FROM test_ingest_append ORDER BY id")
            result = cur.fetch_arrow_table()
            assert result.column("id").to_pylist() == [1, 2, 3, 4]
            assert result.column("val").to_pylist() == [10.0, 20.0, 30.0, 40.0]

    def test_ingest_create_append(self, conn):
        table = pa.table({"x": [100, 200]})
        with conn.cursor() as cur:
            cur.execute(operation="DROP TABLE IF EXISTS test_ingest_ca")
            cur.adbc_ingest(table_name="test_ingest_ca", data=table, mode="create_append")
            cur.adbc_ingest(table_name="test_ingest_ca", data=table, mode="create_append")
            cur.execute(operation="SELECT x FROM test_ingest_ca ORDER BY x")
            assert cur.fetch_arrow_table().column("x").to_pylist() == [100, 100, 200, 200]

    def test_ingest_replace(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="DROP TABLE IF EXISTS test_ingest_replace")
            cur.adbc_ingest(
                table_name="test_ingest_replace", data=pa.table({"a": [1, 2, 3]}), mode="create"
            )
            cur.adbc_ingest(
                table_name="test_ingest_replace", data=pa.table({"a": [99]}), mode="replace"
            )
            cur.execute(operation="SELECT a FROM test_ingest_replace")
            assert cur.fetchall() == [(99,)]

    def test_ingest_record_batch(self, conn):
        batch = pa.record_batch({"id": [10, 20], "label": ["foo", "bar"]})
        with conn.cursor() as cur:
            cur.execute(operation="DROP TABLE IF EXISTS test_ingest_rb")
            cur.adbc_ingest(table_name="test_ingest_rb", data=batch, mode="create")
            cur.execute(operation="SELECT id, label FROM test_ingest_rb ORDER BY id")
            assert cur.fetchall() == [(10, "foo"), (20, "bar")]

    def test_ingest_types_are_kept(self, conn):
        table = pa.table(
            {
                "d": pa.array([decimal.Decimal("123.45")], type=pa.decimal128(10, 2)),
                "t": pa.array(
                    [datetime.datetime(2026, 1, 2, 3, 4, 5, 678901)], type=pa.timestamp("us")
                ),
                "b": pa.array([b"\x00\xff"], type=pa.binary()),
                "l": pa.array([[1, 2]], type=pa.list_(pa.int32())),
            }
        )
        with conn.cursor() as cur:
            cur.execute(operation="DROP TABLE IF EXISTS test_ingest_types")
            cur.adbc_ingest(table_name="test_ingest_types", data=table, mode="create")
            cur.execute(operation="SELECT * FROM test_ingest_types")
            assert cur.fetch_arrow_table().to_pylist() == table.to_pylist()

    @staticmethod
    def _many_batches(rows: int, batch_rows: int) -> pa.Table:
        ids = pa.array(range(rows), type=pa.int64())
        table = pa.table(
            {
                "id": ids,
                "s": pa.array([None if i % 7 == 0 else f"row {i}" for i in range(rows)]),
                "d": pa.array([None if i % 11 == 0 else i / 4 for i in range(rows)]),
            }
        )
        return pa.Table.from_batches(batches=table.to_batches(max_chunksize=batch_rows))

    def test_ingest_in_parts_by_default(self, conn):
        # Two batches or more go as a partitioned ingest (spec §8.3): every row once,
        # unaltered, whatever order they land in.
        table = self._many_batches(rows=400_000, batch_rows=10_000)
        with conn.cursor() as cur:
            cur.execute(operation="DROP TABLE IF EXISTS test_ingest_parts")
            assert (
                cur.adbc_ingest(table_name="test_ingest_parts", data=table, mode="create")
                == 400_000
            )
            cur.execute(operation="SELECT id, s, d FROM test_ingest_parts ORDER BY id")
            assert cur.fetch_arrow_table().equals(table.combine_chunks())

    def test_ingest_one_part_keeps_the_order(self, warp_server):
        table = self._many_batches(rows=100_000, batch_rows=5_000)
        with connect(
            server=warp_server, db_kwargs={StatementOptions.INGEST_PARTS.value: "1"}
        ) as one:
            with one.cursor() as cur:
                cur.execute(operation="DROP TABLE IF EXISTS test_ingest_one_part")
                cur.adbc_ingest(table_name="test_ingest_one_part", data=table, mode="create")
                cur.execute(operation="SELECT id, s, d FROM test_ingest_one_part")
                assert cur.fetch_arrow_table().equals(table.combine_chunks())

    def test_ingest_in_parts_failure_leaves_nothing(self, conn):
        good = self._many_batches(rows=100_000, batch_rows=5_000)

        def broken():
            yield from good.to_batches()
            raise RuntimeError("the application's reader broke")

        with conn.cursor() as cur:
            cur.execute(operation="DROP TABLE IF EXISTS test_ingest_parts_broken")
            with pytest.raises(Exception, match="reader broke"):
                cur.adbc_ingest(
                    table_name="test_ingest_parts_broken",
                    data=pa.RecordBatchReader.from_batches(schema=good.schema, batches=broken()),
                    mode="create",
                )
            cur.execute(
                operation="SELECT count(*) FROM duckdb_tables() "
                "WHERE table_name = 'test_ingest_parts_broken'"
            )
            assert cur.fetchone() == (0,)

    def test_ingest_create_existing_table_fails(self, conn):
        with conn.cursor() as cur:
            cur.execute(operation="CREATE OR REPLACE TABLE test_ingest_exists (a BIGINT)")
            with pytest.raises(adbc_driver_manager.Error) as caught:
                cur.adbc_ingest(
                    table_name="test_ingest_exists", data=pa.table({"a": [1]}), mode="create"
                )
            assert_error(
                error=caught.value,
                status=adbc_driver_manager.AdbcStatusCode.ALREADY_EXISTS,
                code="sql_error",
            )

    def test_ingest_unsupported_type_is_refused(self, conn):
        table = pa.table({"d": pa.array([1_500_000_123], type=pa.duration("ns"))})
        with conn.cursor() as cur:
            with pytest.raises(adbc_driver_manager.NotSupportedError) as caught:
                cur.adbc_ingest(table_name="test_ingest_ns", data=table, mode="create")
            assert_error(
                error=caught.value,
                status=adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED,
                code="unsupported_arrow_type",
            )


class TestReturningClause:
    """INSERT/UPDATE/DELETE ... RETURNING yields rows and persists."""

    def test_insert_returning_yields_rows_and_persists(self, conn):
        with conn.cursor() as cur:
            cur.execute_update(
                query="DROP TABLE IF EXISTS test_returning; "
                "CREATE OR REPLACE SEQUENCE test_returning_seq; "
                "CREATE TABLE test_returning (id INTEGER DEFAULT "
                "nextval('test_returning_seq') PRIMARY KEY, name VARCHAR)"
            )
            cur.execute(
                operation="INSERT INTO test_returning (name) VALUES ('alice'), ('bob') "
                "RETURNING id, name"
            )
            assert cur.description is not None
            returned = cur.fetch_arrow_table()
            assert set(returned.column("name").to_pylist()) == {"alice", "bob"}
            returned_ids = returned.column("id").to_pylist()

            cur.execute(operation="SELECT id FROM test_returning")
            assert {row[0] for row in cur.fetchall()} == set(returned_ids)

            cur.execute(
                operation="UPDATE test_returning SET name = 'CAROL' WHERE name = 'alice' "
                "RETURNING name"
            )
            assert cur.fetchall() == [("CAROL",)]
            cur.execute(operation="DELETE FROM test_returning WHERE name = 'bob' RETURNING id")
            assert len(cur.fetchall()) == 1
            cur.execute(operation="SELECT name FROM test_returning")
            assert cur.fetchall() == [("CAROL",)]

    def test_insert_returning_persists_even_without_fetch(self, conn):
        """The DML has run by the time execute() returns: the next statement on the same
        cursor, which abandons the unread rows, still sees them persisted."""
        with conn.cursor() as cur:
            cur.execute_update(query="CREATE OR REPLACE TABLE test_ret_no_fetch (id INT)")
            cur.execute(operation="INSERT INTO test_ret_no_fetch VALUES (1), (2) RETURNING id")
            assert cur.description is not None
            # deliberately no fetch
            cur.execute(operation="SELECT id FROM test_ret_no_fetch ORDER BY id")
            assert cur.fetchall() == [(1,), (2,)]

    def test_insert_without_returning_has_no_result_set(self, conn):
        with conn.cursor() as cur:
            cur.execute_update(query="CREATE OR REPLACE TABLE test_no_returning (id INT)")
            cur.execute(operation="INSERT INTO test_no_returning VALUES (1), (2)")
            assert cur.description is None
            assert cur.rowcount == 2


class TestBoundParameters:
    """Parameterized DDL/DML through cursor.execute(sql, params) runs immediately, like
    the literal form."""

    UUID = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"

    def test_bound_update_and_delete_without_fetch_persist(self, conn):
        with conn.cursor() as cur:
            cur.execute_update(query="CREATE OR REPLACE TABLE test_bound_dml (id UUID, v INT)")
            cur.execute_update(query=f"INSERT INTO test_bound_dml VALUES ('{self.UUID}', 1)")
            cur.execute(
                operation="UPDATE test_bound_dml SET v = 2 WHERE id = ?", parameters=[self.UUID]
            )
            assert cur.description is None
            assert cur.rowcount == 1
            cur.execute(operation="SELECT v FROM test_bound_dml")
            assert cur.fetchone()[0] == 2
            cur.execute(operation="DELETE FROM test_bound_dml WHERE id = ?", parameters=[self.UUID])
            assert cur.rowcount == 1
            cur.execute(operation="SELECT count(*) FROM test_bound_dml")
            assert cur.fetchone()[0] == 0

    def test_bound_insert_returning_without_fetch_persists(self, conn):
        with conn.cursor() as cur:
            cur.execute_update(query="CREATE OR REPLACE TABLE test_bound_returning (msg VARCHAR)")
            cur.execute(
                operation="INSERT INTO test_bound_returning VALUES (?) RETURNING msg",
                parameters=["hello"],
            )
            assert cur.description is not None
            cur.execute(operation="SELECT count(*) FROM test_bound_returning")
            assert cur.fetchone()[0] == 1

    def test_bound_select_still_streams(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                operation="SELECT n_name FROM nation WHERE n_nationkey = ?", parameters=[24]
            )
            assert cur.description is not None
            assert cur.fetchone()[0] == "UNITED STATES"

    def test_executemany_inserts_every_row(self, conn):
        with conn.cursor() as cur:
            cur.execute_update(query="CREATE OR REPLACE TABLE test_executemany (a INT, b VARCHAR)")
            cur.executemany(
                operation="INSERT INTO test_executemany VALUES (?, ?)",
                seq_of_parameters=[(1, "x"), (2, "y"), (3, None)],
            )
            cur.execute(operation="SELECT * FROM test_executemany ORDER BY a")
            assert cur.fetchall() == [(1, "x"), (2, "y"), (3, None)]


class TestConnectionContextManager:
    def test_fresh_connection(self, warp_server):
        with connect(server=warp_server) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 42 AS answer")
                assert cur.fetch_arrow_table().column("answer")[0].as_py() == 42


class TestConnectionProfiles:
    """ADBC connection profiles (driver manager >= 1.11): TOML files the driver manager
    resolves at database init."""

    @pytest.fixture()
    def profile_dir(self, tmp_path, monkeypatch, warp_server):
        profile = tmp_path / "warp_test.toml"
        profile.write_text(
            data=(
                "profile_version = 1\n\n[Options]\n"
                f'uri = "{warp_server.uri}"\n'
                f'username = "{USERNAME}"\n'
                f'password = "{warp_server.password}"\n'
            )
        )
        monkeypatch.setenv(name="ADBC_PROFILE_PATH", value=str(tmp_path))
        return tmp_path

    def _select(self, conn, value: int) -> None:
        with conn.cursor() as cur:
            cur.execute(operation=f"SELECT {value} AS value")
            assert cur.fetch_arrow_table().column("value")[0].as_py() == value

    def test_named_profile(self, profile_dir):
        with dbapi.connect(profile="warp_test") as conn:
            self._select(conn=conn, value=1)

    def test_profile_uri_scheme(self, profile_dir):
        with dbapi.connect(uri="profile://warp_test") as conn:
            self._select(conn=conn, value=2)

    def test_absolute_path_profile(self, profile_dir):
        with dbapi.connect(profile=str(profile_dir / "warp_test.toml")) as conn:
            self._select(conn=conn, value=3)

    def test_profile_env_var_substitution(self, warp_server, tmp_path, monkeypatch):
        """Secrets can stay out of the profile with {{ env_var(NAME) }}."""
        monkeypatch.setenv(name="WARP_SQL_ADBC_TEST_PROFILE_PASSWORD", value=warp_server.password)
        profile = tmp_path / "warp_env.toml"
        profile.write_text(
            data=(
                "profile_version = 1\n\n[Options]\n"
                f'uri = "{warp_server.uri}"\n'
                f'username = "{USERNAME}"\n'
                'password = "{{ env_var(WARP_SQL_ADBC_TEST_PROFILE_PASSWORD) }}"\n'
            )
        )
        with dbapi.connect(profile=str(profile)) as conn:
            self._select(conn=conn, value=4)

    def test_profile_env_var_missing_fails_sign_in(self, warp_server, tmp_path, monkeypatch):
        """An unset {{ env_var(NAME) }} becomes an empty string, so sign-in must fail."""
        monkeypatch.delenv(name="WARP_SQL_ADBC_TEST_UNSET_PASSWORD", raising=False)
        profile = tmp_path / "warp_unset.toml"
        profile.write_text(
            data=(
                "profile_version = 1\n\n[Options]\n"
                f'uri = "{warp_server.uri}"\n'
                f'username = "{USERNAME}"\n'
                'password = "{{ env_var(WARP_SQL_ADBC_TEST_UNSET_PASSWORD) }}"\n'
            )
        )
        with pytest.raises(adbc_driver_manager.Error) as caught:
            dbapi.connect(profile=str(profile))
        assert_error(
            error=caught.value,
            status=adbc_driver_manager.AdbcStatusCode.UNAUTHENTICATED,
            code="invalid_credentials",
        )

    def test_explicit_options_override_profile(self, warp_server, tmp_path):
        profile = tmp_path / "warp_bad.toml"
        profile.write_text(
            data=(
                "profile_version = 1\n\n[Options]\n"
                f'uri = "{warp_server.uri}"\nusername = "wrong"\npassword = "wrong"\n'
            )
        )
        with dbapi.connect(
            profile=str(profile), username=USERNAME, password=warp_server.password
        ) as conn:
            self._select(conn=conn, value=5)

    def test_explicit_uri_overrides_profile(self, warp_server, profile_dir):
        with dbapi.connect(uri=warp_server.uri, profile="warp_test") as conn:
            self._select(conn=conn, value=6)

    def test_ddl_dml_through_profile_connection(self, profile_dir):
        with dbapi.connect(profile="warp_test") as conn:
            with conn.cursor() as cur:
                cur.execute(operation="CREATE OR REPLACE TABLE test_profile_ddl (id INT)")
                cur.execute(operation="INSERT INTO test_profile_ddl VALUES (1), (2)")
                assert cur.rowcount == 2

    def test_missing_profile_raises(self, profile_dir):
        with pytest.raises(Exception, match="(?i)profile"):
            dbapi.connect(profile="does_not_exist_profile")


class TestUriSpellings:
    """warp:// (TLS unless ?tls=false), https:// and http:// all reach the server."""

    @pytest.mark.parametrize(
        "template",
        [
            "warp://{host}:{port}?tls=false",
            "http://{host}:{port}",
            "http://{host}:{port}/warp",
        ],
    )
    def test_plain_http(self, warp_server, template):
        uri = template.format(host=warp_server.host, port=warp_server.port)
        with dbapi.connect(uri=uri, username=USERNAME, password=warp_server.password) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 1")
                assert cur.fetchone() == (1,)

    def test_bad_scheme(self, warp_server):
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="scheme"):
            dbapi.connect(
                uri=f"grpc+tls://{warp_server.host}:{warp_server.port}",
                username=USERNAME,
                password=warp_server.password,
            )


@pytest.fixture(scope="module")
def tls_server(tmp_path_factory, server_log):
    """A server that speaks TLS only, with a certificate from a throwaway CA."""
    certificates = make_certificates(out_dir=tmp_path_factory.mktemp("tls"))
    server = start_server(
        log=server_log,
        extra_args=["--tls-cert", str(certificates["cert"]), "--tls-key", str(certificates["key"])],
    )
    yield server, certificates
    stop_server(server=server)


class TestTLS:
    def test_custom_ca(self, tls_server):
        server, certificates = tls_server
        with connect(server=server, tls_root_certs=certificates["ca"]) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 1")
                assert cur.fetchone() == (1,)

    def test_verified_by_default(self, tls_server):
        server, _ = tls_server
        with pytest.raises(adbc_driver_manager.Error, match="certificate") as caught:
            connect(server=server)
        assert_error(error=caught.value, status=adbc_driver_manager.AdbcStatusCode.IO, code=None)

    def test_skip_verify(self, tls_server):
        server, _ = tls_server
        with connect(server=server, tls_skip_verify=True) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 2")
                assert cur.fetchone() == (2,)


@pytest.fixture(scope="module")
def mtls_server(tmp_path_factory, server_log):
    """A server that verifies client certificates and signs in CLIENT_EMAIL by its
    certificate (the client_certificate grant)."""
    certificates = make_certificates(out_dir=tmp_path_factory.mktemp("mtls"))
    server = start_server(
        log=server_log,
        extra_args=[
            "--tls-cert",
            str(certificates["cert"]),
            "--tls-key",
            str(certificates["key"]),
            "--tls-client-ca",
            str(certificates["ca"]),
            "--mtls-certificate-sign-in",
            "--mtls-admin-identities",
            CLIENT_EMAIL,
        ],
    )
    yield server, certificates
    stop_server(server=server)


def certificate_connect(server, certificates, label: str = "client", **kwargs):
    return dbapi.connect(
        uri=server.uri,
        auth_type="client_certificate",
        tls_root_certs=certificates["ca"],
        tls_client_cert=certificates[f"{label}_cert"],
        tls_client_key=certificates[f"{label}_key"],
        **kwargs,
    )


def certificate_token(server, certificates, label: str = "client") -> str:
    """A token from the client_certificate grant, outside the driver."""
    import json
    import ssl
    import urllib.request

    context = ssl.create_default_context(cafile=str(certificates["ca"]))
    context.load_cert_chain(
        certfile=str(certificates[f"{label}_cert"]), keyfile=str(certificates[f"{label}_key"])
    )
    request = urllib.request.Request(
        url=f"{server.base_url}/warp/v1/auth/token",
        method="POST",
        data=b'{"grant_type": "client_certificate"}',
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(url=request, context=context) as response:
        return json.load(fp=response)["access_token"]


class TestClientCertificate:
    def test_signs_in_without_a_password(self, mtls_server):
        server, certificates = mtls_server
        with certificate_connect(server=server, certificates=certificates) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 7")
                assert cur.fetchone() == (7,)

    def test_mtls_is_an_alias(self, mtls_server):
        server, certificates = mtls_server
        with dbapi.connect(
            uri=server.uri,
            db_kwargs={"adbc.warp.auth.type": "mtls"},
            tls_root_certs=certificates["ca"],
            tls_client_cert=certificates["client_cert"],
            tls_client_key=certificates["client_key"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 8")
                assert cur.fetchone() == (8,)

    def test_a_split_result_is_read_with_the_bound_token(self, mtls_server):
        server, certificates = mtls_server
        with certificate_connect(server=server, certificates=certificates) as conn:
            with conn.cursor(
                adbc_stmt_kwargs={StatementOptions.RESULT_PARTITIONS.value: "4"}
            ) as cur:
                cur.execute(operation="SELECT i FROM range(1000000) t(i) ORDER BY i")
                table = cur.fetch_arrow_table()
        assert table.column("i").to_pylist() == list(range(1000000))

    def test_an_unknown_identity_is_refused(self, mtls_server):
        server, certificates = mtls_server
        with pytest.raises(adbc_driver_manager.Error) as caught:
            certificate_connect(server=server, certificates=certificates, label="stranger")
        assert_error(
            error=caught.value,
            status=adbc_driver_manager.AdbcStatusCode.UNAUTHENTICATED,
            code="unknown_certificate_identity",
        )

    def test_a_bound_token_needs_its_certificate(self, mtls_server):
        server, certificates = mtls_server
        token = certificate_token(server=server, certificates=certificates)

        def with_token(label: str):
            return dbapi.connect(
                uri=server.uri,
                access_token=token,
                tls_root_certs=certificates["ca"],
                tls_client_cert=certificates[f"{label}_cert"],
                tls_client_key=certificates[f"{label}_key"],
            )

        with pytest.raises(adbc_driver_manager.Error) as caught:
            with_token(label="stranger")
        assert_error(
            error=caught.value,
            status=adbc_driver_manager.AdbcStatusCode.UNAUTHENTICATED,
            code="certificate_mismatch",
        )
        with with_token(label="client") as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 9")
                assert cur.fetchone() == (9,)

    def test_a_server_without_client_cas_does_not_offer_it(self, tls_server):
        server, certificates = tls_server
        with pytest.raises(adbc_driver_manager.Error, match="mtls_identity") as caught:
            certificate_connect(server=server, certificates=certificates)
        assert_error(
            error=caught.value, status=adbc_driver_manager.AdbcStatusCode.UNAUTHENTICATED, code=None
        )


class TestOpenTelemetryTracing:
    def test_adbcfile_exporter_writes_traces(self, warp_server, tmp_path):
        traces_dir = tmp_path / "traces"
        traces_dir.mkdir()
        with connect(
            server=warp_server,
            db_kwargs={
                "adbc.telemetry.traces_exporter": "adbcfile",
                "adbc.telemetry.traces_folder_path": str(traces_dir),
            },
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 1 AS value")
                cur.fetch_arrow_table()
        contents = "".join(f.read_text() for f in traces_dir.iterdir())
        assert "Statement.ExecuteQuery" in contents


class TestConnectCatalog:
    def test_catalog_kwarg_sets_current_catalog(self, warp_server):
        with connect(server=warp_server) as setup_conn:
            with setup_conn.cursor() as cur:
                cur.execute(operation="ATTACH IF NOT EXISTS ':memory:' AS catalog_kwarg_test")
        with connect(server=warp_server, catalog="catalog_kwarg_test", db_schema="main") as conn:
            assert conn.adbc_current_catalog == "catalog_kwarg_test"
            assert conn.adbc_current_db_schema == "main"

    def test_unknown_catalog_is_refused(self, warp_server):
        with pytest.raises(adbc_driver_manager.Error) as caught:
            connect(server=warp_server, catalog="no_such_catalog")
        assert_error(
            error=caught.value,
            status=adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT,
            code="invalid_option_value",
        )


class TestMetadata:
    def test_get_objects(self, conn):
        objects = (
            conn.adbc_get_objects(
                depth="columns", catalog_filter="memory", table_name_filter="nation"
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            table
            for catalog in objects
            for schema in catalog["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert [table["table_name"] for table in tables] == ["nation"]
        assert [column["column_name"] for column in tables[0]["table_columns"]] == [
            "n_nationkey",
            "n_name",
        ]

    def test_get_table_schema(self, conn):
        schema = conn.adbc_get_table_schema(table_name="nation")
        assert schema.names == ["n_nationkey", "n_name"]
        assert schema.field("n_nationkey").type == pa.int32()

    def test_get_table_types(self, conn):
        assert "VIEW" in conn.adbc_get_table_types()

    def test_get_info(self, conn):
        info = conn.adbc_get_info()
        assert info["vendor_name"] == (
            os.environ.get("WARP_SQL_ADBC_TEST_SERVER_NAME") or "warp-duckdb"
        )
        assert info["driver_name"] == "ADBC Warp SQL Driver - Go"


class TestTransactions:
    def test_rollback_and_commit(self, warp_server):
        with connect(server=warp_server) as setup:
            with setup.cursor() as cur:
                cur.execute(operation="CREATE OR REPLACE TABLE test_tx (v INT)")
        with connect(server=warp_server, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="INSERT INTO test_tx VALUES (1)")
                conn.rollback()
                cur.execute(operation="SELECT count(*) FROM test_tx")
                assert cur.fetchone() == (0,)
                cur.execute(operation="INSERT INTO test_tx VALUES (2)")
                conn.commit()
        with connect(server=warp_server) as other:
            with other.cursor() as cur:
                cur.execute(operation="SELECT v FROM test_tx")
                assert cur.fetchall() == [(2,)]

    def test_a_partitioned_ingest_belongs_to_the_open_transaction(self, warp_server):
        # In a transaction, a partitioned ingest's rows commit and roll back with it.
        table = pa.Table.from_batches(
            batches=pa.table({"v": pa.array(range(200_000), type=pa.int64())}).to_batches(
                max_chunksize=10_000
            )
        )
        with connect(server=warp_server) as setup:
            with setup.cursor() as cur:
                cur.execute(operation="CREATE OR REPLACE TABLE test_tx_parts (v BIGINT)")
        with connect(server=warp_server, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.adbc_ingest(table_name="test_tx_parts", data=table, mode="append")
                cur.execute(operation="SELECT count(*) FROM test_tx_parts")
                assert cur.fetchone() == (200_000,)
                conn.rollback()
                cur.execute(operation="SELECT count(*) FROM test_tx_parts")
                assert cur.fetchone() == (0,)
                cur.adbc_ingest(table_name="test_tx_parts", data=table, mode="append")
                conn.commit()
        with connect(server=warp_server) as other:
            with other.cursor() as cur:
                cur.execute(operation="SELECT count(*), sum(v) FROM test_tx_parts")
                assert cur.fetchone() == (200_000, 199_999 * 200_000 // 2)


class TestPartitionsAndOptions:
    def test_partitions_read_on_another_connection(self, warp_server):
        with connect(server=warp_server) as first, connect(server=warp_server) as second:
            with first.cursor() as cur:
                partitions, schema = cur.adbc_execute_partitions(
                    operation="SELECT i FROM range(100000) t(i)"
                )
                assert schema.names == ["i"]
            with second.cursor() as cur:
                total = 0
                for partition in partitions:
                    cur.adbc_read_partition(partition=partition)
                    total += cur.fetch_arrow_table().num_rows
                assert total == 100_000

    @pytest.mark.parametrize("order", ["preserve", "any"])
    @pytest.mark.parametrize("asynchronous", ["false", "true"])
    def test_split_result(self, warp_server, order, asynchronous):
        # A result split into partitions read at once (spec §6.10): every row once, in
        # order unless the client said order doesn't matter, NULLs and types intact.
        sql = (
            "SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE 'row ' || i END AS s, "
            "(i % 1000)::DECIMAL(18, 3) AS d FROM range(1000000) t(i) ORDER BY i DESC"
        )
        with connect(
            server=warp_server, db_kwargs={StatementOptions.RESULT_PARTITIONS.value: "1"}
        ) as plain:
            with plain.cursor() as cur:
                cur.execute(operation=sql)
                whole = cur.fetch_arrow_table()
        with connect(
            server=warp_server,
            db_kwargs={
                StatementOptions.RESULT_PARTITIONS.value: "4",
                StatementOptions.RESULT_ORDER.value: order,
                StatementOptions.ASYNC.value: asynchronous,
            },
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(operation=sql)
                split = cur.fetch_arrow_table()
        assert split.num_rows == 1_000_000
        if order == "preserve":
            assert split.equals(whole)
        else:
            assert split.sort_by(sorting="i").equals(whole.sort_by(sorting="i"))

    def test_split_result_failure_fails_the_query(self, warp_server):
        with connect(
            server=warp_server, db_kwargs={StatementOptions.RESULT_PARTITIONS.value: "4"}
        ) as conn:
            with conn.cursor() as cur:
                with pytest.raises(adbc_driver_manager.Error) as caught:
                    cur.execute(
                        operation="SELECT CASE WHEN i < 30000000 THEN i ELSE error('boom') END "
                        "AS v FROM range(100000000) t(i)"
                    )
                    cur.fetch_arrow_table()
                assert "boom" in str(caught.value)

    def test_async_and_zstd(self, warp_server):
        with connect(
            server=warp_server,
            db_kwargs={
                StatementOptions.ASYNC.value: "true",
                StatementOptions.IPC_COMPRESSION.value: "zstd",
            },
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(operation="SELECT i, i * 2 AS j FROM range(1000000) t(i)")
                table = cur.fetch_arrow_table()
                assert table.num_rows == 1_000_000
                assert table.column("j")[999_999].as_py() == 1_999_998

    def test_statement_timeout(self, warp_server):
        with connect(server=warp_server) as conn:
            with conn.cursor(
                adbc_stmt_kwargs={StatementOptions.TIMEOUT_SECONDS.value: "0.3"}
            ) as cur:
                with pytest.raises(adbc_driver_manager.Error) as caught:
                    cur.execute(
                        operation="SELECT sum(a.range * b.range) FROM range(100000000) a, "
                        "range(100000) b"
                    )
                    cur.fetchall()
                assert_error(
                    error=caught.value,
                    status=adbc_driver_manager.AdbcStatusCode.TIMEOUT,
                    code="statement_timeout",
                )

    def test_lost_session_is_replaced(self, warp_server):
        with connect(server=warp_server) as conn:
            session_id = conn.adbc_connection.get_option(key=ConnectionOptions.SESSION_ID.value)
            status, _ = raw_request(
                server=warp_server, method="DELETE", path=f"/warp/v1/sessions/{session_id}"
            )
            assert status == 204
            with conn.cursor() as cur:
                cur.execute(operation="SELECT 7")
                assert cur.fetchone() == (7,)
            assert (
                conn.adbc_connection.get_option(key=ConnectionOptions.SESSION_REPLACEMENTS.value)
                == "1"
            )
            assert (
                conn.adbc_connection.get_option(key=ConnectionOptions.SESSION_ID.value)
                != session_id
            )
