"""Lazy connect through the built shared library (``make -C go lib``), with a stub server:
when discovery says ``"state": "stopped"`` (a stand-in answering for a stopped server that
starts it when someone signs in), ``connect()`` signs in and opens nothing, and closing the
connection sends nothing, so a pool's refills don't start the server. Without a state,
``connect()`` signs in and opens the session as before."""

from __future__ import annotations

import http.server
import json
import threading
from collections.abc import Iterator

import pytest

from adbc_driver_chargewire import dbapi


class _StubServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, state: str | None) -> None:
        super().__init__(server_address=("127.0.0.1", 0), RequestHandlerClass=_Handler)
        self.state = state
        self.requests: list[str] = []
        self.lock = threading.Lock()

    @property
    def uri(self) -> str:
        return f"chargewire://127.0.0.1:{self.server_address[1]}?tls=false"

    def sent(self) -> list[str]:
        with self.lock:
            return list(self.requests)


class _Handler(http.server.BaseHTTPRequestHandler):
    server: _StubServer

    def log_message(self, format, *args):  # noqa: A002 - the base class's name
        pass

    def _record(self) -> None:
        with self.server.lock:
            self.server.requests.append(f"{self.command} {self.path}")

    def _json(self, status: int, body: dict) -> None:
        data = json.dumps(obj=body).encode()
        self.send_response(code=status)
        self.send_header(keyword="Content-Type", value="application/json")
        self.send_header(keyword="Content-Length", value=str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._record()
        if self.path == "/chargewire":
            body = {
                "protocol": "chargewire",
                "protocol_versions": ["1"],
                "server": {"name": "stub", "version": "1"},
                "capabilities": ["transactions"],
                "auth": {"grant_types": ["password"]},
            }
            if self.server.state is not None:
                body["state"] = self.server.state
            self._json(status=200, body=body)
        else:
            self._json(status=404, body={"status": 404, "code": "not_found", "detail": "no"})

    def do_POST(self):
        self._record()
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path == "/chargewire/v1/auth/token":
            self._json(
                status=200,
                body={"access_token": "tok", "token_type": "Bearer", "expires_in": 3600},
            )
        elif self.path == "/chargewire/v1/sessions":
            self._json(status=201, body={"session_id": "s1", "options": {}})
        else:
            self._json(status=404, body={"status": 404, "code": "not_found", "detail": "no"})

    def do_DELETE(self):
        self._record()
        self.send_response(code=204)
        self.end_headers()


def _serve(state: str | None) -> Iterator[_StubServer]:
    server = _StubServer(state=state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def stopped() -> Iterator[_StubServer]:
    yield from _serve(state="stopped")


@pytest.fixture
def serving() -> Iterator[_StubServer]:
    yield from _serve(state=None)


def test_connect_to_a_stopped_server_sends_nothing(stopped):
    for _ in range(3):
        with dbapi.connect(uri=stopped.uri, username="u", password="p", catalog="lake") as conn:
            assert conn.adbc_get_info()["vendor_name"] == "stub"
    assert stopped.sent() == ["GET /chargewire"] * 3


def test_connect_to_a_serving_server_signs_in(serving):
    with dbapi.connect(uri=serving.uri, username="u", password="p"):
        pass
    assert serving.sent() == [
        "GET /chargewire",
        "POST /chargewire/v1/auth/token",
        "POST /chargewire/v1/sessions",
        "DELETE /chargewire/v1/sessions/s1",
    ]
