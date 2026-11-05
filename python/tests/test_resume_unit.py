"""A resuming server through the built shared library (``make -C go lib``), with a stub
server and no real one: the driver sends the sign-in again while the server answers
``server_resuming``, and ``resume_timeout`` (the ``connect()`` argument, or the
``adbc.chargewire.resume_timeout`` option in ``db_kwargs``, as wrappers that load this
library pass it) reaches the driver."""

from __future__ import annotations

import http.server
import json
import threading
import time
from collections.abc import Iterator

import adbc_driver_manager
import pytest

from adbc_driver_chargewire import DatabaseOptions, dbapi

RESUMING_DETAIL = "the server is resuming; retry after 1 s"


class _ResumingServer(http.server.ThreadingHTTPServer):
    """Answers the sign-in with server_resuming (Retry-After: 1) ``resuming`` times
    (-1: always), then signs in, creates and ends a session."""

    daemon_threads = True

    def __init__(self, resuming: int) -> None:
        super().__init__(server_address=("127.0.0.1", 0), RequestHandlerClass=_Handler)
        self.resuming = resuming
        self.sign_ins = 0
        self.lock = threading.Lock()

    @property
    def uri(self) -> str:
        return f"chargewire://127.0.0.1:{self.server_address[1]}?tls=false"


class _Handler(http.server.BaseHTTPRequestHandler):
    server: _ResumingServer

    def log_message(self, format, *args):  # noqa: A002 - the base class's name
        pass

    def _json(self, status: int, body: dict, headers: dict | None = None) -> None:
        data = json.dumps(obj=body).encode()
        self.send_response(code=status)
        for key, value in (headers or {}).items():
            self.send_header(keyword=key, value=value)
        if "Content-Type" not in (headers or {}):
            self.send_header(keyword="Content-Type", value="application/json")
        self.send_header(keyword="Content-Length", value=str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/chargewire":
            self._json(
                status=200,
                body={
                    "protocol": "chargewire",
                    "protocol_versions": ["1"],
                    "server": {"name": "resuming-stub", "version": "1"},
                    "capabilities": [],
                    "auth": {"grant_types": ["password"]},
                },
            )
        else:
            self._json(status=404, body={"status": 404, "code": "not_found", "detail": "no"})

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path == "/chargewire/v1/auth/token":
            with self.server.lock:
                self.server.sign_ins += 1
                resuming = self.server.resuming < 0 or self.server.sign_ins <= self.server.resuming
            if resuming:
                self._json(
                    status=503,
                    body={
                        "type": "urn:chargewire:error:server_resuming",
                        "status": 503,
                        "code": "server_resuming",
                        "detail": RESUMING_DETAIL,
                    },
                    headers={"Retry-After": "1", "Content-Type": "application/problem+json"},
                )
                return
            self._json(
                status=200,
                body={"access_token": "tok", "token_type": "Bearer", "expires_in": 3600},
            )
        elif self.path == "/chargewire/v1/sessions":
            self._json(status=201, body={"session_id": "s1", "options": {}})
        else:
            self._json(status=404, body={"status": 404, "code": "not_found", "detail": "no"})

    def do_DELETE(self):
        self.send_response(code=204)
        self.end_headers()


def _serve(resuming: int) -> Iterator[_ResumingServer]:
    server = _ResumingServer(resuming=resuming)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def resuming_four_times() -> Iterator[_ResumingServer]:
    # More than the driver's 4 attempts of its bounded retry for other 503s.
    yield from _serve(resuming=4)


@pytest.fixture
def always_resuming() -> Iterator[_ResumingServer]:
    yield from _serve(resuming=-1)


def _chargewire_code(error: adbc_driver_manager.Error) -> str | None:
    for key, value in error.details:
        if key == b"chargewire.code":
            return value.decode()
    return None


@pytest.mark.parametrize("resume_timeout", [None, 30])
def test_sign_in_waits_for_a_resuming_server(resuming_four_times, resume_timeout):
    start = time.monotonic()
    with dbapi.connect(
        uri=resuming_four_times.uri, username="u", password="p", resume_timeout=resume_timeout
    ):
        pass
    assert resuming_four_times.sign_ins == 5
    assert time.monotonic() - start >= 4  # a Retry-After (1 s) between the sign-ins


@pytest.mark.parametrize(
    "kwargs",
    [
        {"resume_timeout": 0},
        {"db_kwargs": {DatabaseOptions.RESUME_TIMEOUT.value: "0"}},
        {"db_kwargs": {"adbc.chargewire.resume_timeout": "0"}},
    ],
    ids=["argument", "enum", "option key"],
)
def test_resume_timeout_zero_reaches_the_driver(always_resuming, kwargs):
    with pytest.raises(adbc_driver_manager.OperationalError) as caught:
        dbapi.connect(uri=always_resuming.uri, username="u", password="p", **kwargs)
    assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.IO
    assert _chargewire_code(error=caught.value) == "server_resuming"
    assert RESUMING_DETAIL in str(caught.value)
    assert always_resuming.sign_ins == 1


def test_resume_deadline_surfaces_the_servers_error(always_resuming):
    with pytest.raises(adbc_driver_manager.OperationalError) as caught:
        dbapi.connect(uri=always_resuming.uri, username="u", password="p", resume_timeout=1.5)
    assert _chargewire_code(error=caught.value) == "server_resuming"
    assert RESUMING_DETAIL in str(caught.value)
    assert "still resuming after 1.5s" in str(caught.value)
    assert always_resuming.sign_ins == 2
