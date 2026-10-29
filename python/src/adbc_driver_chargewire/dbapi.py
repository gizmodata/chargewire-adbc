"""DB-API 2.0 interface for Chargewire servers, backed by the native Go driver.

Example::

    from adbc_driver_chargewire import dbapi

    with dbapi.connect(
        uri="chargewire://db.example.com:31337", username="joe", password="..."
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT region, sum(amount) FROM sales WHERE year = ? GROUP BY 1",
                        parameters=[2026])
            print(cur.fetch_arrow_table())

``chargewire://host:port`` is HTTPS (TLS on); add ``?tls=false`` for plain HTTP to a
development server. ``https://`` and ``http://`` URLs work too.

With mutual TLS, the client certificate can be the credential itself::

    dbapi.connect(uri="chargewire://db.example.com:31337", auth_type="client_certificate",
                  tls_client_cert="client.pem", tls_client_key="client-key.pem")
"""

from __future__ import annotations

import os
from typing import Any, Optional, Union

import adbc_driver_manager
from adbc_driver_manager.dbapi import Connection as _BaseConnection
from adbc_driver_manager.dbapi import Cursor as _BaseCursor

# Re-export the DB-API 2.0 module globals, type constructors and exceptions (PEP 249: a
# driver's module exposes Error, Warning and the rest). They are the driver manager's own
# classes, so `except dbapi.ProgrammingError` and `except adbc_driver_manager.ProgrammingError`
# catch the same errors.
from adbc_driver_manager.dbapi import (  # noqa: F401
    DatabaseError,
    DataError,
    Date,
    DateFromTicks,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    Warning,
    apilevel,
    paramstyle,
    threadsafety,
)

from ._driver import _driver_path
from ._options import DatabaseOptions

PemSource = Union[str, bytes, "os.PathLike[str]"]


class Cursor(_BaseCursor):
    """Chargewire cursor.

    Two DB-API niceties on top of the driver manager's cursor: after a
    statement without a result set (DDL, DML) ``description`` is ``None``
    (the driver reports an empty schema, which this collapses), and fetching
    returns an empty result (``None``/``[]``) instead of raising, as
    ``sqlite3`` and ``duckdb`` do; generic DB-API consumers fetch
    unconditionally after ``execute()``. Fetching before any ``execute()``
    still raises. ``execute_update()`` returns the rows affected directly.
    """

    # True after an execute that produced no result set.
    _executed_without_result_set = False

    def execute(self, operation, parameters=None):
        """Execute a statement. Returns this cursor."""
        self._executed_without_result_set = False
        super().execute(operation=operation, parameters=parameters)
        if self._results is not None and len(self._results.description) == 0:
            self._results.close()
            self._results = None
            self._executed_without_result_set = True
        return self

    def execute_update(self, query: str) -> int:
        """Execute a statement without a result set; return the rows affected
        (-1 when the server does not report a count, e.g. for DDL)."""
        self.adbc_statement.set_sql_query(query)
        rowcount = self.adbc_statement.execute_update()
        self._rowcount = rowcount
        self._results = None
        self._last_query = query
        self._executed_without_result_set = True
        return rowcount

    def fetchone(self):
        """Fetch one row, or ``None`` after a statement without a result set."""
        if self._results is None and self._executed_without_result_set:
            return None
        return super().fetchone()

    def fetchmany(self, size=None):
        """Fetch rows, or ``[]`` after a statement without a result set."""
        if self._results is None and self._executed_without_result_set:
            return []
        if size is None:
            return super().fetchmany()
        return super().fetchmany(size=size)

    def fetchall(self):
        """Fetch all rows, or ``[]`` after a statement without a result set."""
        if self._results is None and self._executed_without_result_set:
            return []
        return super().fetchall()


class Connection(_BaseConnection):
    """Chargewire connection that returns :class:`Cursor` instances."""

    def cursor(self, *, adbc_stmt_kwargs: Optional[dict[str, Any]] = None) -> Cursor:
        """Create a new :class:`Cursor`."""
        cursor = Cursor(conn=self, adbc_stmt_kwargs=adbc_stmt_kwargs, dbapi_backend=self._backend)
        self._cursors.add(cursor)
        return cursor


def _pem(value: PemSource) -> str:
    """PEM text given directly, or read from a file path."""
    if isinstance(value, bytes):
        return value.decode(encoding="ascii")
    text = os.fspath(value)
    if "-----BEGIN" in text:
        return text
    with open(file=text, encoding="ascii") as f:
        return f.read()


def connect(
    uri: Optional[str] = None,
    *,
    profile: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    access_token: Optional[str] = None,
    auth_type: Optional[str] = None,
    open_browser: Union[bool, str] = True,
    oauth_timeout_seconds: Optional[float] = None,
    tls_skip_verify: bool = False,
    tls_root_certs: Optional[PemSource] = None,
    tls_client_cert: Optional[PemSource] = None,
    tls_client_key: Optional[PemSource] = None,
    catalog: Optional[str] = None,
    db_schema: Optional[str] = None,
    db_kwargs: Optional[dict[str, str]] = None,
    conn_kwargs: Optional[dict[str, str]] = None,
    autocommit: bool = True,
) -> Connection:
    """Connect to a Chargewire server (DB-API 2.0).

    Args:
        uri: Server URI: ``chargewire://host:port`` (HTTPS; ``?tls=false`` for plain
            HTTP), ``https://…`` or ``http://…``, optionally with a base path
            (default ``/chargewire``). A ``profile://<name>`` URI names a connection
            profile. Optional if ``profile`` supplies it.
        profile: Name (or absolute path) of an ADBC connection profile.
        username: User name for password sign-in.
        password: Password for password sign-in.
        access_token: A Chargewire access token obtained elsewhere, instead of a
            username and password.
        auth_type: ``"password"`` (the default) or ``"client_certificate"``
            (``"mtls"``): sign in with the TLS client certificate
            (``tls_client_cert`` and ``tls_client_key``) and no password. The
            server binds the token to the certificate. ``"oauth_browser"``:
            sign in through a browser with the server's identity provider
            (Chargewire spec §4.3), when the connection opens; the server must
            list the ``oauth_browser`` capability.
        open_browser: Browser sign-in: True (the default) opens the system's
            browser, False prints the sign-in URL to stderr instead, and a
            string is the command that opens it (the URL is appended).
        oauth_timeout_seconds: Browser sign-in: how long the person has to
            sign in (default 300).
        tls_skip_verify: Do not verify the server's certificate (development
            only).
        tls_root_certs: CA certificates to trust, as PEM text or a file path.
        tls_client_cert: Client certificate chain for mutual TLS (PEM text or
            path).
        tls_client_key: The client certificate's private key (PEM text or
            path).
        catalog: Catalog to make current for the session.
        db_schema: Schema to make current for the session.
        db_kwargs: Extra ADBC database options (``adbc.chargewire.*``, telemetry).
        conn_kwargs: Extra ADBC connection options.
        autocommit: Enable autocommit (default True).

    Returns:
        A DB-API 2.0 :class:`Connection`.
    """
    if uri is None and profile is None:
        raise ValueError("Must provide at least one of 'uri' or 'profile'.")

    db_kwargs = dict(db_kwargs or {})
    conn_kwargs = dict(conn_kwargs or {})

    if uri is not None and uri.startswith("profile://"):
        db_kwargs.setdefault("profile", uri[len("profile://") :])
    elif uri is not None:
        db_kwargs.setdefault("uri", uri)
    if profile is not None:
        db_kwargs.setdefault("profile", profile)

    credentials = {
        "username": username,
        "password": password,
        DatabaseOptions.ACCESS_TOKEN.value: access_token,
        DatabaseOptions.AUTH_TYPE.value: auth_type,
    }
    for key, value in credentials.items():
        if value is not None:
            db_kwargs.setdefault(key, value)
    if open_browser is False:
        db_kwargs.setdefault(DatabaseOptions.OAUTH_BROWSER.value, "none")
    elif isinstance(open_browser, str):
        db_kwargs.setdefault(DatabaseOptions.OAUTH_BROWSER.value, open_browser)
    if oauth_timeout_seconds is not None:
        db_kwargs.setdefault(
            DatabaseOptions.OAUTH_TIMEOUT_SECONDS.value, str(oauth_timeout_seconds)
        )
    if tls_skip_verify:
        db_kwargs.setdefault(DatabaseOptions.TLS_SKIP_VERIFY.value, "true")
    pem_options = {
        DatabaseOptions.TLS_ROOT_CERTS.value: tls_root_certs,
        DatabaseOptions.TLS_CLIENT_CERT.value: tls_client_cert,
        DatabaseOptions.TLS_CLIENT_KEY.value: tls_client_key,
    }
    for key, value in pem_options.items():
        if value is not None:
            db_kwargs.setdefault(key, _pem(value=value))

    conn_options = adbc_driver_manager.ConnectionOptions
    if catalog is not None:
        conn_kwargs.setdefault(conn_options.CURRENT_CATALOG.value, catalog)
    if db_schema is not None:
        conn_kwargs.setdefault(conn_options.CURRENT_DB_SCHEMA.value, db_schema)

    db = None
    conn = None
    try:
        db = adbc_driver_manager.AdbcDatabase(driver=_driver_path(), **db_kwargs)
        conn = adbc_driver_manager.AdbcConnection(database=db, **conn_kwargs)
        return Connection(db=db, conn=conn, autocommit=autocommit)
    except Exception:
        if conn:
            conn.close()
        if db:
            db.close()
        raise
