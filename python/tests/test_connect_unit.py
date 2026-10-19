"""Unit tests for dbapi.connect() and the cursor shims (no server).

Ported from gizmosql-adbc's suite: every database is built with
``adbc_driver_manager.AdbcDatabase`` and the bundled library, so those are
mocked; the URI handling itself lives in the Go driver and is tested there.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

_PATCH_DB = "adbc_driver_warpsql.dbapi.adbc_driver_manager.AdbcDatabase"
_PATCH_CONN = "adbc_driver_warpsql.dbapi.adbc_driver_manager.AdbcConnection"
_PATCH_CLS = "adbc_driver_warpsql.dbapi.Connection"
_PATCH_LIB = "adbc_driver_warpsql.dbapi._driver_path"

FAKE_LIB = "/fake/libadbc_driver_warpsql.dylib"
PEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"


def _db_kwargs(mock_db) -> dict:
    """The option map handed to AdbcDatabase (minus the driver path)."""
    kwargs = dict(mock_db.call_args[1])
    kwargs.pop("driver", None)
    return kwargs


@patch(_PATCH_LIB, return_value=FAKE_LIB)
@patch(_PATCH_CLS)
@patch(_PATCH_CONN)
@patch(_PATCH_DB)
class TestConnect:
    def test_password_auth(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        result = connect(
            uri="warp://localhost:31337",
            username="myuser",
            password="my:p@ss",
            tls_skip_verify=True,
        )

        assert result == mock_conn_cls.return_value
        kwargs = _db_kwargs(mock_db=mock_db)
        assert kwargs == {
            "uri": "warp://localhost:31337",
            "username": "myuser",
            "password": "my:p@ss",
            "adbc.warp.tls.skip_verify": "true",
        }
        assert mock_db.call_args[1]["driver"] == FAKE_LIB

    def test_tls_verified_unless_asked(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(uri="warp://localhost:31337", username="u", password="p")
        assert "adbc.warp.tls.skip_verify" not in _db_kwargs(mock_db=mock_db)

    def test_access_token(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(uri="warp://h:1", access_token="tok")
        assert _db_kwargs(mock_db=mock_db) == {
            "uri": "warp://h:1",
            "adbc.warp.auth.access_token": "tok",
        }

    def test_client_certificate_auth(
        self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib, tmp_path
    ):
        from adbc_driver_warpsql.dbapi import connect

        key = "-----BEGIN PRIVATE KEY-----\nAAA\n-----END PRIVATE KEY-----\n"
        connect(uri="warp://h:1", auth_type="mtls", tls_client_cert=PEM, tls_client_key=key)
        assert _db_kwargs(mock_db=mock_db) == {
            "uri": "warp://h:1",
            "adbc.warp.auth.type": "mtls",
            "adbc.warp.tls.client_cert": PEM,
            "adbc.warp.tls.client_key": key,
        }

    def test_browser_sign_in(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(uri="warp://h:1", auth_type="oauth_browser")
        assert _db_kwargs(mock_db=mock_db) == {
            "uri": "warp://h:1",
            "adbc.warp.auth.type": "oauth_browser",
        }

    def test_browser_sign_in_options(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(
            uri="warp://h:1",
            auth_type="oauth_browser",
            open_browser=False,
            oauth_timeout_seconds=60,
        )
        assert _db_kwargs(mock_db=mock_db) == {
            "uri": "warp://h:1",
            "adbc.warp.auth.type": "oauth_browser",
            "adbc.warp.oauth.browser": "none",
            "adbc.warp.oauth.timeout_seconds": "60",
        }
        mock_db.reset_mock()
        connect(uri="warp://h:1", auth_type="oauth_browser", open_browser="firefox --new-window")
        assert _db_kwargs(mock_db=mock_db)["adbc.warp.oauth.browser"] == "firefox --new-window"

    def test_pem_text_and_files(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib, tmp_path):
        from adbc_driver_warpsql.dbapi import connect

        key_file = tmp_path / "client-key.pem"
        key_file.write_text(data="-----BEGIN PRIVATE KEY-----\nAAA\n-----END PRIVATE KEY-----\n")
        connect(
            uri="warp://h:1",
            username="u",
            password="p",
            tls_root_certs=PEM,
            tls_client_cert=PEM.encode(),
            tls_client_key=key_file,
        )
        kwargs = _db_kwargs(mock_db=mock_db)
        assert kwargs["adbc.warp.tls.root_certs"] == PEM
        assert kwargs["adbc.warp.tls.client_cert"] == PEM
        assert kwargs["adbc.warp.tls.client_key"].startswith("-----BEGIN PRIVATE KEY-----")

    def test_db_kwargs_passthrough(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(
            uri="warp://h:1",
            username="user",
            password="pass",
            db_kwargs={"adbc.warp.statement.async": "true"},
        )
        kwargs = _db_kwargs(mock_db=mock_db)
        assert kwargs["adbc.warp.statement.async"] == "true"
        assert kwargs["username"] == "user"

    def test_explicit_db_kwargs_win(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(uri="warp://h:1", username="from_arg", db_kwargs={"username": "from_db_kwargs"})
        assert _db_kwargs(mock_db=mock_db)["username"] == "from_db_kwargs"

    def test_conn_kwargs_passthrough(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(
            uri="warp://h:1",
            username="user",
            password="pass",
            conn_kwargs={"conn_option": "conn_value"},
        )
        mock_adbc_conn.assert_called_once_with(
            database=mock_db.return_value, conn_option="conn_value"
        )

    def test_catalog_and_db_schema_kwargs(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(
            uri="warp://h:1",
            username="user",
            password="pass",
            catalog="analytics",
            db_schema="reporting",
        )
        conn_kwargs = dict(mock_adbc_conn.call_args[1])
        conn_kwargs.pop("database")
        assert conn_kwargs == {
            "adbc.connection.catalog": "analytics",
            "adbc.connection.db_schema": "reporting",
        }

    def test_catalog_kwarg_does_not_override_explicit_conn_kwargs(
        self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib
    ):
        from adbc_driver_warpsql.dbapi import connect

        connect(
            uri="warp://h:1",
            username="user",
            password="pass",
            catalog="from_kwarg",
            conn_kwargs={"adbc.connection.catalog": "from_conn_kwargs"},
        )
        assert mock_adbc_conn.call_args[1]["adbc.connection.catalog"] == "from_conn_kwargs"

    def test_autocommit_default_true(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(uri="warp://h:1", username="u", password="p")
        mock_conn_cls.assert_called_once_with(
            db=mock_db.return_value, conn=mock_adbc_conn.return_value, autocommit=True
        )

    def test_uri_passed_through(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        for uri in ("warp://h:1?tls=false", "https://h:1/custom", "http://127.0.0.1:8080"):
            connect(uri=uri, username="u", password="p")
            assert _db_kwargs(mock_db=mock_db)["uri"] == uri

    def test_failed_connection_closes_database(
        self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib
    ):
        from adbc_driver_warpsql.dbapi import connect

        mock_adbc_conn.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            connect(uri="warp://h:1", username="u", password="p")
        mock_db.return_value.close.assert_called_once()


@patch(_PATCH_LIB, return_value=FAKE_LIB)
@patch(_PATCH_CLS)
@patch(_PATCH_CONN)
@patch(_PATCH_DB)
class TestConnectProfile:
    def test_no_uri_and_no_profile_raises(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        with pytest.raises(ValueError, match="at least one of 'uri' or 'profile'"):
            connect()
        mock_db.assert_not_called()

    def test_profile_only(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(profile="my_profile")
        assert _db_kwargs(mock_db=mock_db) == {"profile": "my_profile"}

    def test_profile_only_explicit_credentials_override(
        self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib
    ):
        from adbc_driver_warpsql.dbapi import connect

        connect(profile="my_profile", username="u", password="p")
        assert _db_kwargs(mock_db=mock_db) == {
            "profile": "my_profile",
            "username": "u",
            "password": "p",
        }

    def test_uri_with_profile(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(uri="warp://h:1", profile="my_profile")
        assert _db_kwargs(mock_db=mock_db) == {"uri": "warp://h:1", "profile": "my_profile"}

    def test_profile_uri_scheme_passthrough(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(uri="profile://my_profile")
        assert _db_kwargs(mock_db=mock_db) == {"profile": "my_profile"}

    def test_profile_only_tls_skip_verify(self, mock_db, mock_adbc_conn, mock_conn_cls, mock_lib):
        from adbc_driver_warpsql.dbapi import connect

        connect(profile="my_profile", tls_skip_verify=True)
        assert _db_kwargs(mock_db=mock_db)["adbc.warp.tls.skip_verify"] == "true"


class TestCursorShims:
    """The cursor collapses the driver's empty schema into DB-API's "no result set"."""

    @staticmethod
    def _cursor():
        from adbc_driver_warpsql.dbapi import Cursor

        cursor = Cursor.__new__(Cursor)
        cursor._results = None
        cursor._stmt = MagicMock()
        cursor._closed = True  # nothing for the destructor to close
        return cursor

    @patch("adbc_driver_manager.dbapi.Cursor.execute")
    def test_empty_schema_result_collapsed(self, mock_super_execute):
        cursor = self._cursor()
        results = MagicMock()
        results.description = []

        def fake_execute(operation, parameters=None):
            cursor._results = results

        mock_super_execute.side_effect = fake_execute
        assert cursor.execute(operation="CREATE TABLE t (i INTEGER)") is cursor
        assert cursor._results is None
        results.close.assert_called_once()
        assert cursor.fetchall() == []
        assert cursor.fetchone() is None
        assert cursor.fetchmany(size=5) == []

    @patch("adbc_driver_manager.dbapi.Cursor.execute")
    def test_result_with_columns_kept(self, mock_super_execute):
        cursor = self._cursor()
        results = MagicMock()
        results.description = [("a", None, None, None, None, None, None)]

        def fake_execute(operation, parameters=None):
            cursor._results = results

        mock_super_execute.side_effect = fake_execute
        cursor.execute(operation="SELECT 1 AS a")
        assert cursor._results is results
        results.close.assert_not_called()

    def test_execute_update_returns_rows_affected(self):
        cursor = self._cursor()
        statement = MagicMock()
        statement.execute_update.return_value = 7
        with patch("adbc_driver_manager.dbapi.Cursor.adbc_statement", new=statement):
            assert cursor.execute_update(query="DELETE FROM t") == 7
        statement.set_sql_query.assert_called_once_with("DELETE FROM t")
        assert cursor.rowcount == 7
        assert cursor.fetchall() == []


class TestDriverPath:
    def test_environment_override(self, monkeypatch):
        from adbc_driver_warpsql import _driver

        _driver._driver_path.cache_clear()
        monkeypatch.setenv(name="WARP_SQL_ADBC_DRIVER_LIB", value="/custom/lib.so")
        try:
            assert _driver._driver_path() == "/custom/lib.so"
        finally:
            _driver._driver_path.cache_clear()
