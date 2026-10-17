# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `warpsql.NewDriverWithName(alloc, displayName)`: the driver under another GetInfo driver
  name, for drivers built on this one (hardcharger-adbc).
- Partitioned ingest (Warp SQL spec §8.3): `adbc.warp.ingest.parts` (default 8). On a
  server with the `partitioned_ingest` capability, an ingest of two batches or more is
  opened once, sent as up to that many parts at once over separate connections, and
  committed as one; any failed part aborts it, so nothing is written. `1`, a one-batch
  upload, or a server without the capability sends one ordered stream as before. Python:
  `StatementOptions.INGEST_PARTS`.

### Changed

- `adbc.warp.statement.result_partitions` defaults to 8 (was 1): large results are split
  and read over several connections at once. The server decides: a result of fewer than
  two batches still comes as one stream, and a server without `result_partitions` never
  sees the request. Set it to 1 for one stream.

### Added (earlier)

- Split results (Warp SQL spec §6.10): `adbc.warp.statement.result_partitions` (most
  partitions, default 1) and `adbc.warp.statement.result_order` (`preserve` or `any`).
  The driver reads every partition of a split result at once, one connection each, and
  merges them into one reader: round robin for `interleaved` results (the server's order),
  as they arrive for `any`. A failed or cut-off partition fails the whole result. Python:
  `StatementOptions.RESULT_PARTITIONS` and `RESULT_ORDER`.
- The HTTP transport keeps up to 256 idle connections per host (one per partition).

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

### Changed

- Follows warp-sql `188cd84`, which resolves every item in `docs/spec-feedback.md`:
  - A statement whose answer has not arrived is cancelled with the session cancel
    (`POST /sessions/{id}/cancel`) when it is the connection's only statement running;
    beside other statements, or on a server without it, the driver still resends with the
    `Idempotency-Key`. Explicit stops (Cancel, Close while running, a Go context ending)
    send `POST …/cancel`; releasing a result deletes the statement, which lets a write
    finish.
  - `ExecuteSchema` returns the prepared statement's `result_kind`/`result_schema` (the
    empty schema for statements without rows) instead of NotImplemented.
  - `GetObjects` reports primary and foreign keys, with one request to each key endpoint.
  - SQLSTATEs `42P01` and `42P07` are ADBC NotFound and AlreadyExists.
  - Removed workarounds the server no longer needs: growing long-poll waits (back to a
    fixed 15 s) and precision/scale taken from Arrow decimal types.

### Release engineering

- The release's live suites pull `hardcharger-server` from the private image
  `ghcr.io/gizmodata/hardcharger` with `GITHUB_TOKEN` instead of a personal token.
- `.github/workflows/release.yml`: on a `vX.Y.Z` tag, every suite on the tagged commit (ci.yml,
  and the Go and Python live-server suites against `hardcharger-server` from Hardcharger's
  releases), then the library and a wheel for Linux amd64/arm64 (manylinux, glibc
  checked, installed and loaded on Oracle Linux 8), macOS arm64/amd64 and Windows amd64 (Windows
  arm64: the library only; its dependencies have no wheels there), a GitHub Release, and PyPI through the `pypi` environment (trusted publishing, approval).
