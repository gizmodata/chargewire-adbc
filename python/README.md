# adbc-driver-warpsql

A Python [ADBC](https://arrow.apache.org/adbc/) driver for the
[Warp SQL protocol](https://github.com/gizmodata/warp-sql): SQL over plain HTTP, with
results, parameters and bulk uploads as Arrow IPC streams. It is generic: it talks to any
Warp SQL server.

The driver itself is a native Go library (`libadbc_driver_warpsql`) bundled in the
platform wheel; this package adds a DB-API 2.0 `connect()`.

## Usage

```python
from adbc_driver_warpsql import dbapi

with dbapi.connect(
    uri="warp://db.example.com:31337",  # HTTPS; add ?tls=false for plain HTTP
    username="joe",
    password="…",
) as conn:
    with conn.cursor() as cur:
        cur.execute(operation="SELECT n_name FROM nation WHERE n_nationkey = ?", parameters=[24])
        print(cur.fetch_arrow_table())

        cur.execute(operation="CREATE TABLE t (id INTEGER)")  # runs right away
        cur.adbc_ingest(table_name="t", data=my_arrow_table, mode="append")
```

`connect()` takes:

| Argument | Meaning |
|---|---|
| `uri` | `warp://host:port[/base-path]` (TLS), `?tls=false` for plain HTTP; `https://` and `http://` URLs work too. `profile://name` names a connection profile. |
| `username`, `password` | Password sign-in (any characters, `:` included). |
| `access_token` | A Warp SQL access token obtained elsewhere, instead of a password. |
| `tls_root_certs` | CA certificates to trust (PEM text or file path). |
| `tls_client_cert`, `tls_client_key` | Client certificate for mutual TLS (PEM text or path). |
| `tls_skip_verify` | Skip certificate verification (development only; off unless set). |
| `catalog`, `db_schema` | Make these current for the session. |
| `autocommit` | `True` by default; `False` starts a transaction (`conn.commit()`, `conn.rollback()`). |
| `profile` | An ADBC connection profile. |
| `db_kwargs`, `conn_kwargs` | Any other driver option, e.g. `adbc.warp.statement.async`. |

Driver options are in `adbc_driver_warpsql.DatabaseOptions`, `ConnectionOptions` and
`StatementOptions`; the repository README lists them all.

Errors are `adbc_driver_manager` exceptions whose `details` carry the server's error code
(`b"warp.code"`, e.g. `b"sql_error"`) and request id (`b"warp.request_id"`).

## Development

See the [repository](https://github.com/gizmodata/warp-sql-adbc): build the library with
`make -C go lib`, then `pip install -e "python[test]"`. Unit tests need no server; the
integration tests start the server named by `WARP_SQL_ADBC_TEST_SERVER_BINARY` and fail
without it.
