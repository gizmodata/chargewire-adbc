"""The PEP 249 exception class of each kind of failure, against a live server.

The driver gives every Chargewire error code an ADBC status, and the driver manager turns the
status into a DB-API class: a user's SQL that fails, before it runs (sql_error) or while it
runs (execution_error, HTTP 422), is a ProgrammingError, never an InternalError (which says
the driver or the server is broken); a violated constraint is an IntegrityError; a cancel or
a timeout is an OperationalError; a wrong password is the ProgrammingError of status
UNAUTHENTICATED.
"""

from __future__ import annotations

import threading
import time

import adbc_driver_manager
import pytest
from conftest import USERNAME

from adbc_driver_chargewire import StatementOptions, dbapi

pytestmark = pytest.mark.integration

Status = adbc_driver_manager.AdbcStatusCode

# CPU-bound for far longer than any test timeout: only a server-side stop ends it early.
LONG_QUERY = "SELECT sum(a.range * b.range) FROM range(100000000) a, range(100000) b"


@pytest.fixture()
def fresh_conn(chargewire_server):
    with dbapi.connect(
        uri=chargewire_server.uri, username=USERNAME, password=chargewire_server.password
    ) as connection:
        yield connection


def chargewire_code(error: adbc_driver_manager.Error) -> str | None:
    for key, value in error.details:
        if key == b"chargewire.code":
            return value.decode()
    return None


def run(conn, operation: str) -> BaseException:
    """Run a statement to the end of its result and return what it raised."""
    with conn.cursor() as cur:
        try:
            cur.execute(operation=operation)
            cur.fetchall()
        except BaseException as error:  # noqa: BLE001 - the test inspects whatever it was
            return error
    pytest.fail(reason=f"{operation} succeeded; want an error")


def assert_raised(
    error: BaseException,
    klass: type,
    status: adbc_driver_manager.AdbcStatusCode,
    code: str | None,
    sqlstate: str | None = None,
    message: str | None = None,
) -> None:
    assert type(error) is klass, f"{type(error).__name__}, want {klass.__name__}: {error}"
    assert not isinstance(error, dbapi.InternalError), error
    assert error.status_code == status, error
    assert chargewire_code(error=error) == code, error
    if sqlstate is not None:
        assert error.sqlstate == sqlstate, error
    if message is not None:
        assert message in str(error), error


class TestSQLErrors:
    @pytest.mark.parametrize(
        argnames=("operation", "message"),
        argvalues=[
            # DuckDB's / divides floating point (1/0 is no error); // divides integers.
            ("SELECT 1 // 0", "ivision by zero"),
            ("SELECT 10 // (i - 3) FROM range(5) t(i)", "ivision by zero"),
            ("SELECT CAST(s AS INTEGER) FROM (VALUES ('1'), ('x')) t(s)", "Conversion Error"),
            ("SELECT CASE WHEN i < 3 THEN i ELSE error('boom') END FROM range(10) t(i)", "boom"),
        ],
        ids=["division-by-zero", "division-by-zero-over-rows", "conversion", "error-function"],
    )
    def test_execution_error_is_a_programming_error(self, fresh_conn, operation, message):
        error = run(conn=fresh_conn, operation=operation)
        assert_raised(
            error=error,
            klass=dbapi.ProgrammingError,
            status=Status.INVALID_ARGUMENT,
            code="execution_error",
            message=message,
        )

    def test_failure_mid_stream_is_a_programming_error(self, fresh_conn):
        """A statement that fails after 3,000,000 rows: its result is usually streaming by
        then, so the error arrives through the result stream at a fetch, not at execute().
        Either way its class is the same."""
        with fresh_conn.cursor() as cur:
            with pytest.raises(expected_exception=dbapi.Error) as caught:
                cur.execute(
                    operation="SELECT CASE WHEN i < 3000000 THEN i ELSE error('boom') END AS v "
                    "FROM range(6000000) t(i)"
                )
                while cur.fetchmany(size=100_000):
                    pass
        assert_raised(
            error=caught.value,
            klass=dbapi.ProgrammingError,
            status=Status.INVALID_ARGUMENT,
            code="execution_error",
            message="boom",
        )

    def test_syntax_error_is_a_programming_error(self, fresh_conn):
        error = run(conn=fresh_conn, operation="SELEC 1")
        assert_raised(
            error=error,
            klass=dbapi.ProgrammingError,
            status=Status.INVALID_ARGUMENT,
            code="sql_error",
            sqlstate="42601",
            message="Parser Error",
        )

    def test_unknown_table_is_a_programming_error(self, fresh_conn):
        error = run(conn=fresh_conn, operation="SELECT * FROM errclass_no_such_table")
        assert_raised(
            error=error,
            klass=dbapi.ProgrammingError,
            status=Status.NOT_FOUND,
            code="sql_error",
            sqlstate="42P01",
            message="errclass_no_such_table",
        )

    def test_duplicate_key_is_an_integrity_error(self, fresh_conn):
        with fresh_conn.cursor() as cur:
            cur.execute(operation="CREATE OR REPLACE TABLE errclass_keys (id INTEGER PRIMARY KEY)")
            cur.execute(operation="INSERT INTO errclass_keys VALUES (1)")
        error = run(conn=fresh_conn, operation="INSERT INTO errclass_keys VALUES (1)")
        assert_raised(
            error=error,
            klass=dbapi.IntegrityError,
            status=Status.INTEGRITY,
            code="execution_error",
            sqlstate="23000",
        )

    def test_the_session_survives(self, fresh_conn):
        run(conn=fresh_conn, operation="SELECT 1 // 0")
        with fresh_conn.cursor() as cur:
            cur.execute(operation="SELECT 42")
            assert cur.fetchone() == (42,)


class TestStoppedStatements:
    def test_cancel_is_an_operational_error(self, fresh_conn):
        cur = fresh_conn.cursor()
        errors: list[BaseException] = []

        def execute() -> None:
            try:
                cur.execute(operation=LONG_QUERY)
                cur.fetchall()
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        worker = threading.Thread(target=execute, daemon=True)
        worker.start()
        time.sleep(1.0)
        cur.adbc_cancel()
        worker.join(timeout=30)
        assert not worker.is_alive(), "the query was not interrupted within 30s"
        cur.close()
        assert errors, "the cancelled query raised nothing"
        assert type(errors[0]) is dbapi.OperationalError, repr(errors[0])
        assert errors[0].status_code == Status.CANCELLED, errors[0]

    def test_timeout_is_an_operational_error(self, fresh_conn):
        with fresh_conn.cursor(
            adbc_stmt_kwargs={StatementOptions.TIMEOUT_SECONDS.value: "0.3"}
        ) as cur:
            with pytest.raises(expected_exception=dbapi.Error) as caught:
                cur.execute(operation=LONG_QUERY)
                cur.fetchall()
        assert_raised(
            error=caught.value,
            klass=dbapi.OperationalError,
            status=Status.TIMEOUT,
            code="statement_timeout",
        )


class TestAuthentication:
    def test_bad_password_is_unauthenticated(self, chargewire_server):
        with pytest.raises(expected_exception=dbapi.Error) as caught:
            with dbapi.connect(
                uri=chargewire_server.uri,
                username=USERNAME,
                password="not-" + chargewire_server.password,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(operation="SELECT 1")
        # The driver manager raises ProgrammingError for UNAUTHENTICATED.
        assert_raised(
            error=caught.value,
            klass=dbapi.ProgrammingError,
            status=Status.UNAUTHENTICATED,
            code="invalid_credentials",
        )
        assert chargewire_server.password not in str(caught.value)
