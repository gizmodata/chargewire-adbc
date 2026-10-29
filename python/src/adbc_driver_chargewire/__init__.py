"""ADBC driver for the Chargewire protocol.

A thin Python binding over the native Go driver (``libadbc_driver_chargewire``),
which talks to any Chargewire server: SQL over plain HTTP, with results,
parameters and uploads as Arrow IPC streams.

Quick start::

    from adbc_driver_chargewire import dbapi

    with dbapi.connect("chargewire://db.example.com:31337", username="joe", password="...") as conn:
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
