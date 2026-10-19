"""Live tests: TLS pointed at a plain-HTTP server, and closing after the server stopped."""

from __future__ import annotations

import pathlib
import subprocess
import sys
import time

import adbc_driver_manager
import pytest
from conftest import USERNAME, start_server, stop_server

from adbc_driver_warpsql import dbapi

pytestmark = pytest.mark.integration


@pytest.fixture
def recorder(warp_server, tmp_path):
    """A recording TCP proxy in front of the shared plain-HTTP server: (port, bytes sent)."""
    log = tmp_path / "sent.bin"
    log.write_bytes(data=b"")
    process = subprocess.Popen(
        args=[
            sys.executable,
            str(pathlib.Path(__file__).with_name("tcp_recorder.py")),
            str(warp_server.port),
            str(log),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    port = int(process.stdout.readline().strip())
    yield port, log
    process.kill()
    process.wait()


def test_tls_to_a_plain_http_server_says_to_turn_tls_off(warp_server, recorder):
    port, log = recorder
    with pytest.raises(expected_exception=adbc_driver_manager.OperationalError) as error:
        dbapi.connect(
            uri=f"warp://127.0.0.1:{port}", username=USERNAME, password=warp_server.password
        )
    assert error.value.status_code == adbc_driver_manager.AdbcStatusCode.IO
    assert str(error.value).endswith(
        f"[warpsql] The server at 127.0.0.1:{port} speaks plain HTTP, not TLS. "
        "Add ?tls=false to the URI to connect without encryption."
    )
    sent = log.read_bytes()
    assert sent[:1] == b"\x16", sent[:40]  # a TLS hello, and nothing else
    for secret in (warp_server.password, USERNAME, "auth/token"):
        assert secret.encode() not in sent


def test_plain_http_through_the_recorder_twin(warp_server, recorder):
    """With ?tls=false the same proxy sees the password: the check above would catch one."""
    port, log = recorder
    with dbapi.connect(
        uri=f"warp://127.0.0.1:{port}?tls=false", username=USERNAME, password=warp_server.password
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 42")
            assert cur.fetchone() == (42,)
    assert warp_server.password.encode() in log.read_bytes()


def _open_with_cursors(server):
    conn = dbapi.connect(uri=server.uri, username=USERNAME, password=server.password)
    fetched = conn.cursor()
    fetched.execute(operation="SELECT 1")
    assert fetched.fetchone() == (1,)
    unread = conn.cursor()
    unread.execute(operation="SELECT * FROM range(1000000)")
    return conn, [fetched, unread]


def test_close_after_the_server_stopped(tmp_path):
    server = start_server(log=tmp_path / "server.log")
    try:
        opened = [_open_with_cursors(server=server) for _ in range(3)]
    finally:
        stop_server(server=server)
    start = time.monotonic()
    for conn, cursors in opened:
        for cursor in cursors:
            cursor.close()
        conn.close()
    assert time.monotonic() - start < 5
    for conn, _ in opened:
        assert conn._closed
        # Released: closing again is a no-op, and the database has no open connection
        # (it would raise "Cannot close AdbcDatabase with open AdbcConnection").
        conn.adbc_connection.close()
        conn.adbc_database.close()


def test_close_after_the_server_stopped_without_closing_cursors_first(tmp_path):
    server = start_server(log=tmp_path / "server.log")
    try:
        conn, _ = _open_with_cursors(server=server)
    finally:
        stop_server(server=server)
    conn.close()  # closes its cursors, then the connection and the database
    assert conn._closed
    conn.adbc_database.close()


def test_close_with_the_server_up_twin(warp_server):
    conn, cursors = _open_with_cursors(server=warp_server)
    for cursor in cursors:
        cursor.close()
    conn.close()
    assert conn._closed
