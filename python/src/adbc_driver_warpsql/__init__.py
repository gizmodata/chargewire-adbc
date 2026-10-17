"""ADBC driver for the Warp SQL protocol.

A thin Python binding over the native Go driver (``libadbc_driver_warpsql``),
which talks to any Warp SQL server: SQL over plain HTTP, with results,
parameters and uploads as Arrow IPC streams.

Quick start::

    from adbc_driver_warpsql import dbapi

    with dbapi.connect("warp://db.example.com:31337", username="joe", password="...") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 42 AS answer")
            print(cur.fetch_arrow_table())
"""

from __future__ import annotations

from ._options import ConnectionOptions, DatabaseOptions, StatementOptions
from ._version import __version__

__all__ = [
    "ConnectionOptions",
    "DatabaseOptions",
    "StatementOptions",
    "__version__",
]
