# warp-sql-adbc

A native [ADBC](https://arrow.apache.org/adbc/) driver for the
[Warp SQL protocol](https://github.com/gizmodata/warp-sql), written in Go, with Python
bindings.

It is a **generic** driver: it talks to any server that implements the Warp SQL spec,
not one particular database. Compiled to a C shared library, one implementation
serves every ADBC language: Python, Go, R, C/C++, C#, Rust and JavaScript.

> **Status: not started.** The protocol is at draft 0.1.0. The driver is written
> against the spec once a reference server exists to test it with.

## Planned layout

The same shape as [`gizmosql-adbc`](https://github.com/gizmodata/gizmosql-adbc):

```
go/       Go driver (ADBC Go interfaces over plain net/http) + cgo C exports
python/   Python bindings that ship the shared library in their wheels
docs/     Design notes and conformance results
```

## What it maps

| ADBC | Warp SQL |
|---|---|
| `AdbcDatabaseInit` (URI, credentials, TLS options) | `GET /warp` discovery, `POST /warp/v1/auth/token` |
| `AdbcConnectionInit` / `Release` | `POST` / `DELETE /warp/v1/sessions` |
| Connection options (current catalog, schema, autocommit) | `PATCH /warp/v1/sessions/{id}` |
| `AdbcConnectionCommit` / `Rollback` | `POST …/commit`, `POST …/rollback` |
| `AdbcStatementExecuteQuery` | `POST …/statements`, streamed or async with resumable fetch |
| `AdbcStatementExecuteUpdate` | Same, `kind: update` |
| `AdbcStatementPrepare`, `Bind`, `GetParameterSchema` | `POST …/prepared`, `POST …/prepared/{id}/execute` with Arrow parameters |
| `AdbcStatementExecutePartitions` / `AdbcConnectionReadPartition` | The statement's `partitions` |
| Bulk ingest (`adbc.ingest.*` options) | `POST …/ingest` |
| `AdbcStatementCancel` | `POST …/statements/{id}/cancel` |
| `AdbcConnectionGetObjects`, `GetTableTypes`, `GetTableSchema` | `GET …/metadata/*` (Flight SQL's result schemas) |
| `AdbcConnectionGetInfo` | `GET …/metadata/server-info` |

The driver handles retries the protocol makes safe: it sends an `Idempotency-Key`
with every submit and ingest, resumes a dropped result download from the last batch it
received, and on `session_not_found` opens a new session and replays its options
instead of hiding the loss.

## Dependencies

- `github.com/apache/arrow-adbc/go/adbc` for the ADBC interfaces
- `github.com/apache/arrow-go` for Arrow IPC
- Go's standard `net/http` and `crypto/tls`; no gRPC

## License

[Apache License 2.0](LICENSE).
