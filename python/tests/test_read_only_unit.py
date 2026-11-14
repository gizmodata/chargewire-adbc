"""Read-only connection setup through the real C driver and a protocol stub."""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager

import adbc_driver_manager
import pytest
from test_lazy_connect_unit import _Handler, _StubServer

from adbc_driver_chargewire._driver import _driver_path

KEY = "adbc.connection.readonly"


class _ReadOnlyHandler(_Handler):
    def do_POST(self):
        if self.path != "/chargewire/v1/sessions":
            return super().do_POST()
        self._record()
        body = json.loads(s=self.rfile.read(int(self.headers.get("Content-Length") or 0)))
        self.server.creations.append(body)
        options = {"read_only": self.server.confirmation}
        self._json(status=201, body={"session_id": "s1", "options": options})


@contextmanager
def stub(*, lazy, confirmation):
    server = _StubServer(state="stopped" if lazy else None)
    server.RequestHandlerClass = _ReadOnlyHandler
    server.confirmation = confirmation
    server.creations = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.mark.parametrize("lazy", [False, True])
@pytest.mark.parametrize("confirmation", [True, False, None, "true"])
def test_creation_confirmation_through_c_api(lazy, confirmation):
    with stub(lazy=lazy, confirmation=confirmation) as server:
        with adbc_driver_manager.AdbcDatabase(
            driver=_driver_path(), uri=server.uri, username="u", password="p"
        ) as db:
            if confirmation is True or lazy:
                with adbc_driver_manager.AdbcConnection(database=db, **{KEY: "true"}) as conn:
                    if lazy:
                        assert server.creations == []
                    if confirmation is True:
                        assert conn.get_option(key=KEY) == "true"
                        with pytest.raises(adbc_driver_manager.Error):
                            conn.set_options(**{KEY: "false"})
                    else:
                        with pytest.raises(adbc_driver_manager.Error, match="did not confirm"):
                            conn.get_option(key=KEY)
            else:
                with pytest.raises(adbc_driver_manager.Error, match="did not confirm"):
                    adbc_driver_manager.AdbcConnection(database=db, **{KEY: "true"})
        assert server.creations == [{"options": {"read_only": True}}]
        assert not any(request.startswith("PATCH") for request in server.sent())
        assert "DELETE /chargewire/v1/sessions/s1" in server.sent()
