# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Python 3.11 is the minimum (`requires-python = ">=3.11"`): pyarrow 26, the floor,
  needs 3.11, so on 3.10 the package could never be installed.

### Fixed

- `TestIntegrationIngestPartAnswerLost` failed now and then ("10 part requests, want 9"):
  the answer to the part sent again was itself lost, and the part went a third time. The
  server answers a part or upload sent again with its `Idempotency-Key` from the first
  upload without reading the body, and the Warp SQL SDK then closed the connection with
  the body unread on Linux and macOS, which resets it, so a client still sending got a
  broken pipe or a reset instead of the answer (fixed in warp-sql ce22b15, which drains
  before closing on every platform; Hardcharger 4556928 takes it). The tests' fault proxy
  had the same defect on its own hop (the answer passed on, the rest of the body unread):
  it now reads every request body to its end. No row was ever written twice: every run
  checked the table row by row. New live test `TestIntegrationIngestAnswerToARetryArrives`
  sends a part and a one-request upload (about 2 MB) again 40 times each, directly and
  through the proxy, and asserts every answer arrives and the rows count once; it fails
  on servers before warp-sql ce22b15 (8 of 10 runs on warp-duckdb 17e4609) and passes
  with it.
- The `dbapi` module exports PEP 249's exception classes (`Error`, `Warning`, `InterfaceError`,
  `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`,
  `ProgrammingError`, `NotSupportedError`), as PEP 249 requires of a driver module. They are
  `adbc_driver_manager`'s own classes, so `except dbapi.ProgrammingError` and
  `except adbc_driver_manager.ProgrammingError` catch the same errors.

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

### Fixed

- A split result whose partition's connection broke could fail with "the statement was
  cancelled" (the server's reaction) instead of the broken connection and its advice to
  retain results, whenever another partition saw the cancellation first. A cancellation
  the application didn't ask for now waits (up to 2 s) for the partition that caused it.

### Added (earlier)

- Client certificate sign-in (Warp SQL spec §4.4, servers with capability `mtls_identity`):
  `adbc.warp.auth.type` = `client_certificate` (alias `mtls`; default `password`) signs in
  with `{"grant_type": "client_certificate"}` over the TLS client certificate
  (`adbc.warp.tls.client_cert`/`client_key`), with no password, and renews the token the
  same way before it expires. Refused before connecting without a client certificate,
  over plain HTTP, or beside a username, password, subject token or access token; refused
  with a clear error by a server that doesn't offer it. The new problem codes
  (`client_certificate_required`, `unknown_certificate_identity`, `certificate_revoked`,
  `certificate_mismatch`, `certificate_identity_mismatch`) map to `Unauthenticated` and
  carry `warp.code`. Python: `DatabaseOptions.AUTH_TYPE` and `connect(auth_type=...)`.
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
