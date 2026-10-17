# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Repository created: README with the driver's scope, planned layout and the
  mapping from ADBC calls to Warp SQL operations.
- **Go ADBC driver** (`go/warpsql`) for any Warp SQL server (spec draft 0.1.0), on
  `driverbase-go`, over Go's `net/http` with Arrow IPC from `arrow-go`:
  - `warp://host:port` URIs (HTTPS; `?tls=false` for plain HTTP; `https://`/`http://` and
    custom base paths too). TLS is verified unless skip-verify is set explicitly; custom CA,
    mutual TLS and server-name options.
  - Password, token-exchange and pre-issued-token sign-in, with tokens renewed before they
    expire and once when refused.
  - Queries on the streamed path, or asynchronous (submit, long-poll, retained result) with
    the async option; DDL/DML through `ExecuteQuery` returns an empty schema and the rows
    affected; `ExecuteUpdate`; `ExecutePartitions`/`ReadPartition`, also on other connections
    and from partition locations on other servers.
  - Prepared statements created on the server only when needed, Arrow parameters (one row
    per execution, `Bind` and `BindStream`), `GetParameterSchema`, `ExecuteSchema`.
  - Bulk ingest with the standard `adbc.ingest.*` options (create, append, replace,
    create_append, temporary, target catalog and schema), streamed above 16 MiB.
  - `GetObjects` (one request per level), `GetTableSchema`, `GetTableTypes`, `GetInfo` with
    the server's name and version; autocommit, commit, rollback; current catalog and schema.
  - Server-side cancel from `Cancel`, Ctrl+C, closing the statement, releasing an unread
    result or a Go context ending, including statements whose answer has not arrived.
  - Statement timeouts, query tags, zstd Arrow compression of results and uploads.
  - Resilience: `Idempotency-Key` on every submit, execute and ingest; retries of requests
    that are safe to repeat (with `Retry-After`); broken downloads of retained results
    resume from the next batch; streams without their end-of-stream marker are failures;
    lost sessions are replaced with their options replayed and the loss reported (never
    inside a transaction); finished statements are freed on the server in the background.
  - Errors carry the ADBC status for the server's error code, its `sqlstate`, and details
    `warp.code` and `warp.request_id`; `warpsql.ErrorCode` reads the code in Go.
- **C shared library** `libadbc_driver_warpsql` (entry point `AdbcDriverWarpsqlInit`),
  generated cgo exports, a driver manifest template, and a Linux build against glibc 2.17.
- **Python bindings** `adbc-driver-warpsql`: the library in a `py3-none` platform wheel and
  `dbapi.connect()`.
- Tests: Go and Python unit tests that need no server, and live-server suites ported from
  gizmosql-adbc plus Warp SQL resilience tests through a fault-injecting proxy; they start
  the server named by `WARP_SQL_ADBC_TEST_SERVER_BINARY` and fail without it.
- CI (`.github/workflows/ci.yml`): gofmt, vet, Go and Python unit tests, the library on
  Linux (amd64, arm64), macOS and Windows with a glibc check, and the wheel. The live-server
  suites are not in CI yet: the reference server is built from a private repository.
- `docs/testing.md` (what is tested, what was not ported and why) and
  `docs/spec-feedback.md` (spec gaps and reference-server issues found).
