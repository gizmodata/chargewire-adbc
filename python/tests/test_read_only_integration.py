"""Read-only connections against a server implementing Chargewire section 5.8."""

from __future__ import annotations

import adbc_driver_manager
import pytest
from conftest import USERNAME

from adbc_driver_chargewire import dbapi

pytestmark = pytest.mark.integration
KEY = "adbc.connection.readonly"


def test_read_only_connection_and_write_refusal(chargewire_server):
    with dbapi.connect(
        uri=chargewire_server.uri,
        username=USERNAME,
        password=chargewire_server.password,
        conn_kwargs={KEY: "true"},
    ) as conn:
        assert conn.adbc_connection.get_option(key=KEY) == "true"
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 42 AS answer")
            assert cur.fetchone() == (42,)
            with pytest.raises(adbc_driver_manager.Error) as caught:
                cur.execute(operation="CREATE TABLE readonly_client_probe (id INTEGER)")
            assert (b"chargewire.code", b"read_only") in caught.value.details
            cur.execute(
                operation="SELECT count(*) FROM information_schema.tables "
                "WHERE table_name='readonly_client_probe'"
            )
            assert cur.fetchone() == (0,)
        with pytest.raises(adbc_driver_manager.Error):
            conn.adbc_connection.set_options(**{KEY: "false"})
        assert conn.adbc_connection.get_option(key=KEY) == "true"


def test_existing_connection_can_become_read_only(chargewire_server):
    with dbapi.connect(
        uri=chargewire_server.uri, username=USERNAME, password=chargewire_server.password
    ) as conn:
        assert conn.adbc_connection.get_option(key=KEY) == "false"
        conn.adbc_connection.set_options(**{KEY: "true"})
        assert conn.adbc_connection.get_option(key=KEY) == "true"
        with conn.cursor() as cur:
            cur.execute(operation="SELECT 42 AS answer")
            assert cur.fetchone() == (42,)
            with pytest.raises(adbc_driver_manager.Error) as caught:
                cur.execute(operation="CREATE TABLE readonly_transition_probe (id INTEGER)")
            assert (b"chargewire.code", b"read_only") in caught.value.details
