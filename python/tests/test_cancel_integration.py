"""Server-side query cancellation through the DB-API layer, against a live server.

Ported from gizmosql-adbc. The oracle is the session: it runs one statement at a time,
so a query the server did not stop would hold up the next statement on the same
connection (test_running_query_holds_the_session proves it).
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import threading
import time

import adbc_driver_manager
import pytest
from conftest import USERNAME

from adbc_driver_warpsql import dbapi

pytestmark = pytest.mark.integration

# CPU-bound for far longer than any test timeout, producing nothing until it ends: the
# only way it returns promptly is a server-side interrupt.
LONG_QUERY = "SELECT sum(a.range * b.range) FROM range(100000000) a, range(100000) b"
# Streams batches until something stops it.
STREAMING_QUERY = "SELECT i FROM range(100000000000) t(i)"


@pytest.fixture()
def fresh_conn(warp_server):
    """A dedicated connection, so each test owns its server session."""
    with dbapi.connect(
        uri=warp_server.uri, username=USERNAME, password=warp_server.password
    ) as connection:
        yield connection


def _assert_session_usable(conn, within: float = 30.0) -> None:
    """SELECT 42 answers promptly: nothing is still running in the session."""
    result: list = []

    def run() -> None:
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 42")
            result.append(cur.fetchone()[0])

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(timeout=within)
    assert result == [42], f"the session did not answer within {within}s: a query still runs"


def _run_in_thread(cur, operation: str) -> tuple[threading.Thread, list[BaseException]]:
    errors: list[BaseException] = []

    def run() -> None:
        try:
            cur.execute(operation=operation)
            cur.fetchall()
        except BaseException as exc:  # noqa: BLE001 - whatever the driver raised
            errors.append(exc)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    return worker, errors


class TestQueryCancellation:
    def test_running_query_holds_the_session(self, fresh_conn):
        """The oracle works: while a query runs, its session answers nothing else, and
        answers again once the query is cancelled."""
        cur = fresh_conn.cursor()
        worker, _ = _run_in_thread(cur=cur, operation=LONG_QUERY)
        time.sleep(1.0)
        with fresh_conn.cursor() as probe:
            answered = threading.Event()

            def select_42() -> None:
                probe.execute(operation="SELECT 42")
                probe.fetchall()
                answered.set()

            prober = threading.Thread(target=select_42, daemon=True)
            prober.start()
            assert not answered.wait(timeout=2.0), "SELECT 42 ran beside the long query"
            cur.adbc_cancel()
            worker.join(timeout=30)
            assert not worker.is_alive()
            prober.join(timeout=30)
            assert answered.is_set(), "SELECT 42 never ran after the cancel"
        cur.close()

    def test_adbc_cancel_from_another_thread_interrupts_query(self, fresh_conn):
        """cursor.adbc_cancel(), the entry point the driver manager's SIGINT handler uses,
        interrupts a running query on the server."""
        cur = fresh_conn.cursor()
        worker, errors = _run_in_thread(cur=cur, operation=LONG_QUERY)
        started = time.monotonic()
        time.sleep(1.0)
        cur.adbc_cancel()
        worker.join(timeout=30)

        assert not worker.is_alive(), "the query was not interrupted within 30s of adbc_cancel()"
        assert errors, "expected the long query to fail"
        error = errors[0]
        assert isinstance(error, adbc_driver_manager.Error), error
        assert error.status_code == adbc_driver_manager.AdbcStatusCode.CANCELLED, error
        assert "cancel" in str(error).lower()
        assert time.monotonic() - started < 30
        cur.close()
        _assert_session_usable(conn=fresh_conn)

    def test_cursor_close_cancels_running_query(self, fresh_conn):
        """Closing the cursor while its result is still streaming stops the query."""
        cur = fresh_conn.cursor()
        cur.execute(operation=STREAMING_QUERY)
        cur.fetchmany(size=10)
        cur.close()  # walk away without reading the rest
        _assert_session_usable(conn=fresh_conn)

    def test_fully_fetched_result_is_not_cancelled(self, fresh_conn):
        with fresh_conn.cursor() as cur:
            cur.execute(operation="SELECT range AS v FROM range(1000)")
            assert len(cur.fetchall()) == 1000
            with pytest.raises(
                adbc_driver_manager.ProgrammingError, match="no statement is running"
            ):
                cur.adbc_cancel()

    def test_ctrl_c_in_client_process_cancels_query_on_server(self, warp_server, tmp_path):
        """A user interrupts their Python process mid-query: SIGINT reaches the driver
        manager's handler, which calls AdbcStatementCancel; the driver must relay that to
        the server. The child proves it by using the same connection afterwards."""
        script = tmp_path / "client.py"
        script.write_text(
            data=textwrap.dedent(f"""
            import time
            import adbc_driver_manager
            from adbc_driver_warpsql import dbapi

            with dbapi.connect(uri={warp_server.uri!r}, username={USERNAME!r},
                               password={warp_server.password!r}) as conn:
                cur = conn.cursor()
                print("ready", flush=True)
                try:
                    cur.execute(operation={LONG_QUERY!r})
                    cur.fetchall()
                    print("finished", flush=True)
                except (KeyboardInterrupt, adbc_driver_manager.Error) as exc:
                    # The driver manager's SIGINT handler cancels the statement, which
                    # then fails with CANCELLED.
                    print("interrupted", type(exc).__name__, exc, flush=True)
                started = time.monotonic()
                with conn.cursor() as check:
                    check.execute(operation="SELECT 42")
                    assert check.fetchone()[0] == 42
                print(f"session usable after {{time.monotonic() - started:.1f}}s", flush=True)
        """)
        )
        proc = subprocess.Popen(
            args=[sys.executable, str(script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        try:
            assert proc.stdout.readline().strip() == "ready"
            time.sleep(2.0)
            proc.send_signal(signal.SIGINT)
            try:
                stdout, stderr = proc.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                pytest.fail(reason="the client did not finish within 30s of SIGINT")
        finally:
            if proc.poll() is None:
                proc.kill()
        assert "interrupted" in stdout, (stdout, stderr)
        assert "finished" not in stdout
        assert "session usable" in stdout, (stdout, stderr)
        seconds = float(stdout.split("session usable after ")[1].split("s")[0])
        assert seconds < 10, f"SELECT 42 waited {seconds}s: the query kept running on the server"
