"""connect(read_only=True) asks for a server-enforced read-only connection through the ADBC
standard option adbc.connection.readonly (no server)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from adbc_driver_chargewire import dbapi

_PATCH_DB = "adbc_driver_chargewire.dbapi.adbc_driver_manager.AdbcDatabase"
_PATCH_CONN = "adbc_driver_chargewire.dbapi.adbc_driver_manager.AdbcConnection"
_PATCH_CLS = "adbc_driver_chargewire.dbapi.Connection"
_READ_ONLY = "adbc.connection.readonly"


@patch(_PATCH_CLS)
@patch(_PATCH_CONN)
@patch(_PATCH_DB)
class TestReadOnlyArgument:
    def test_read_only_sets_the_standard_option(self, mock_db, mock_conn, mock_cls):
        dbapi.connect(uri="chargewire://h:1", username="u", password="p", read_only=True)
        assert mock_conn.call_args.kwargs[_READ_ONLY] == "true"

    def test_not_asked_by_default(self, mock_db, mock_conn, mock_cls):
        dbapi.connect(uri="chargewire://h:1", username="u", password="p")
        assert _READ_ONLY not in mock_conn.call_args.kwargs
        dbapi.connect(uri="chargewire://h:1", username="u", password="p", read_only=False)
        assert _READ_ONLY not in mock_conn.call_args.kwargs

    def test_conn_kwargs_still_work(self, mock_db, mock_conn, mock_cls):
        dbapi.connect(
            uri="chargewire://h:1", username="u", password="p", conn_kwargs={_READ_ONLY: "true"}
        )
        assert mock_conn.call_args.kwargs[_READ_ONLY] == "true"
        dbapi.connect(
            uri="chargewire://h:1",
            username="u",
            password="p",
            read_only=True,
            conn_kwargs={_READ_ONLY: "TRUE"},
        )
        assert mock_conn.call_args.kwargs[_READ_ONLY] == "true"

    def test_disagreeing_conn_kwargs_are_refused(self, mock_db, mock_conn, mock_cls):
        with pytest.raises(expected_exception=ValueError, match="disagree"):
            dbapi.connect(
                uri="chargewire://h:1",
                username="u",
                password="p",
                read_only=True,
                conn_kwargs={_READ_ONLY: "false"},
            )
        mock_db.assert_not_called()

    def test_only_a_bool(self, mock_db, mock_conn, mock_cls):
        with pytest.raises(expected_exception=TypeError, match="read_only must be a bool"):
            dbapi.connect(uri="chargewire://h:1", username="u", password="p", read_only="false")
