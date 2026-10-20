# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Browser sign-in (Warp SQL spec §4.3, servers with capability `oauth_browser`):
  `adbc.warp.auth.type=oauth_browser` signs in through a browser with the server's identity
  provider when the first connection opens. The driver creates a flow, opens its
  `authorize_url` (`adbc.warp.oauth.browser`: a command, or `none` to print the URL to stderr;
  default the system's browser), polls it with its secret at the server's `poll_interval_s`
  and uses the server's access token it gets once (`adbc.warp.oauth.timeout_seconds`, default
  300). A refusal fails with status `UNAUTHENTICATED` and the server's code (`access_denied`,
  `invalid_credentials`, `flow_not_found`) and detail; a server without the capability is
  refused before any flow is created; a username, password or token beside it is refused.
  The poll secret and the token are never logged. For drivers built on this one,
  `warpsql.BrowserSignIn(ctx, options, opener, timeout)` runs the flow and returns the access
  token, its lifetime and the provider's name, and `warpsql.SystemBrowser(command)` is the
  default opener. Python: `connect(auth_type="oauth_browser", open_browser=False|"cmd",
  oauth_timeout_seconds=...)`, `DatabaseOptions.OAUTH_BROWSER`, `OAUTH_TIMEOUT_SECONDS`.
  Tests: unit tests against a fake server (the token used, the capability required, each
  failure's code and detail, timeout and cancellation, conflicting credentials, the exported
  function) and live ones against warp-duckdb with a mock OpenID Connect provider that checks
  PKCE (a sign-in, the exported token opening a session, a refusal at the provider, an
  unauthorized user, a server without the capability); Python unit and live tests.
- `adbc.warp.auth.access_token` can be set again on an open database that signs in with an
  access token or through the browser: its connections send the new token from their next
  request on (a wrapper that renews or shares tokens needs no new database). On a database
  that signs in another way it is refused (`INVALID_STATE`).
- `adbc.warp.session.transaction` (read-only, connection; Python
  `ConnectionOptions.SESSION_TRANSACTION`): the session's transaction as the server reports
  it (`none`, `open`, `aborted`, or `unknown`), which sees SQL `BEGIN` and `COMMIT` too.
- The live-test servers (Go and Python) run with a `HOME` of their own.
- LZ4_FRAME Arrow buffer compression (Warp SQL §8.2, capability
  `ipc_compression_lz4_frame`), for results and uploads: `adbc.warp.ipc_compression=lz4_frame`.
  Every request that returns Arrow sends `Warp-IPC-Compression`, each partition's included.
- `adbc.warp.ipc_compression=auto`, the new default: `lz4_frame` on a link with a round
  trip under 10 ms, `zstd` on a slower one. The round trip is the discovery request's,
  timed from its last byte sent to the answer's first.
- `adbc.warp.ingest.parts=auto`, the new default: 16 parts on a near link, 32 on a far one.

### Changed

- **Warp SQL protocol 1.0.0.** The driver follows the protocol's first stable version (spec
  1.0.0), with no fallbacks for the draft's names:
  - Discovery has no `draft` field and no `result_retention` / `partitioned_results`
    capabilities (retention is core: the driver may ask, and reads `retained`). Unknown
    capabilities are ignored.
  - Enumerations are open (§12): a statement or ingest state the driver doesn't know is not
    terminal (it keeps polling), an unknown `reason` / `cancel_reason` is a generic one, an
    unknown code is handled by its HTTP status. The code lists now hold every 1.0 code
    (`unsupported_grant_type`, `partition_not_found`, `internal_error`, …);
    `client_certificate_required` is now `certificate_required`.
  - Codes the driver makes up start with `client_`: `client_protocol_error` (was
    `protocol_error`), `client_http_error` for an error status without a problem document (a
    proxy's page; it used to borrow a server code from the status, such as
    `payload_too_large` or `execution_error`), and `client_statement_failed` /
    `client_statement_cancelled` for a failed or cancelled document without its error.
  - A statement cancelled or failed by the time the server answers its submit is an error
    response (409 `statement_cancelled` with `cancel_reason`, 422 `execution_error`, …) naming
    the statement in `Warp-Statement-Id`: the driver reports it (`StatusCancelled` for a
    cancel) and frees that statement. A `failed`/`cancelled` document is still an error
    whatever the status. 502 and 421 without a known code are now `StatusIO`.
  - A split result must state its order: `Warp-Result-Partitions` above 1 without a
    known `Warp-Result-Order` (or a document's `partition_order`) is `client_protocol_error`,
    no longer read as `any`.
  - Partitions on other servers: the driver sends `Warp-Result-Locations: remote` only to a
    server listing `result_locations`, refuses a location it did not ask for or one that
    would drop TLS (the access token goes along), and reads remote partitions at once with
    the others (they were read one after another). On the streamed path it finds them in the
    statement document (§6.2).
  - Browser sign-in follows the same rules: an unknown flow `state` keeps polling; a failed
    flow's `error.code` (`access_denied`, `invalid_credentials`, `identity_provider_error`,
    or an unknown one, which is a generic failure) is `StatusUnauthenticated` with no HTTP
    status attached (it arrives in a 200 poll answer); a failed flow without an error is
    `client_sign_in_failed`, and a malformed flow or token answer `client_protocol_error`.
  - Partitioned ingest commits with `{"part_count": n}` (was `{"parts": n}`).
  - `adbc.warp.statement.query_tag` must be a JSON object (`null` was accepted); it is sent
    compact and ASCII-escaped (`\uXXXX`), refused over 4,096 bytes, and now also labels
    ingests.
  - `invalid_token` is answered by signing in again (once) only without a reason, with
    `token_expired` or with an unknown reason; after `token_revoked`, `credentials_changed`
    or `principal_dropped` the error comes back with its reason (error detail
    `warp.reason`; `warp.cancel_reason` for cancels). A session ended with `token_expired`
    is replaced only after a new token; with a token issued elsewhere it is not replaced.
  - A TLS alert `certificate_required` / `certificate_revoked` at the handshake is reported
    as that refusal (`StatusUnauthenticated`, code `certificate_required` /
    `certificate_revoked`), not as an unreachable server (`StatusIO`).
  - `Retry-After` is read as delta-seconds only.
- Compressed record batches are decompressed by the driver, not Arrow's reader: every
  buffer of a batch at once, straight into one new body, while a goroutine keeps reading
  the stream's next messages; buffers sent as they are (length -1) are copied. Bodies come
  from a recycler over the C library's allocator (calloc), which faulted in fresh pages
  for every batch. Over a 100 Gbit/s LAN, SF10 lineitem in 32 partitions with lz4_frame:
  0.99 s -> 0.89 s through Python; one stream: 5.8 s -> 3.7 s on a laptop.
- Results may be split into up to 32 partitions by default (was 8); the server chooses
  how many by the result's size.
- Uploads go as 16,384-row slices of larger batches, and in about one part per two
  slices: SF1 lineitem into Hardcharger over a LAN took 0.45-0.67 s with whole 122,880-row
  batches (a few of the server's insert threads got one more row group to compress than
  the others) and 0.31-0.36 s in slices. A small upload no longer opens every part.

- Python 3.11 is the minimum (`requires-python = ">=3.11"`): pyarrow 26, the floor,
  needs 3.11, so on 3.10 the package could never be installed.

### Fixed

- A user's SQL that failed while running (`execution_error`, HTTP 422: a division by zero, a
  failed conversion, `error()`) was reported as ADBC `StatusInternal`, which says the driver
  or server is broken, so Python raised `InternalError`. It is now `StatusInvalidArgument`
  (`ProgrammingError`), like `sql_error` and like the DuckDB and PostgreSQL ADBC drivers. The
  mapping is now one table that covers every code in the spec's error table (§11) and the
  driver's `client_` codes; other changes: SQLSTATE class `23` on a SQL error is
  `StatusIntegrity` (`IntegrityError`; Hardcharger sends `23000` for a violated constraint);
  SQLSTATE `42P01`/`42P07` refine only `sql_error`/`execution_error`, not another code that
  carries one; `method_not_allowed` (and an unknown code with HTTP 405) is
  `StatusNotImplemented`, not `StatusInvalidState`; `ingest_part_exists` is
  `StatusAlreadyExists`; an unknown code with HTTP 408 is `StatusIO` (retryable), and one
  with no HTTP status at all (inside a failed statement document) is `StatusUnknown`, not
  `StatusInternal`. Tests: a unit test per code, one that fails when the spec's table
  (`go/warpsql/testdata/warp-sql-errors.md`, or a `warp-sql.md` named by
  `WARP_SQL_ADBC_TEST_SPEC`) or the protocol package has a code without a status, live Go
  tests and Python tests of the DB-API class of a division by zero, a conversion, a failure
  mid-stream, a syntax error, an unknown table, a duplicate key, a cancel, a timeout and a
  wrong password.
- A TLS connection (`warp://`, the default, or `https://`) to a server that speaks plain HTTP
  failed with Go's "server gave HTTP response to HTTPS client", after four tries. It now fails
  at once with what to do: `The server at HOST:PORT speaks plain HTTP, not TLS. Add ?tls=false
  to the URI to connect without encryption.` (for an `https://` URI: "Use an http:// URI
  instead of https://"), status `IO`. Only the TLS hello reaches the server: no request and no
  credential (live tests record the bytes through a TCP proxy, Go and Python). The driver never
  falls back to plaintext.
- Closing a connection after the server stopped returned an error ("closing the session: …
  connection refused"), so Python's `Connection.close()` raised and left the driver's database
  handle open ("Cannot close AdbcDatabase with open AdbcConnection"). Ending the session is now
  best-effort: at most two tries within 5 s (was four within 10 s), and a server that is gone,
  hangs or refuses no longer fails `Close`, which always releases the connection's local
  resources; the session ends on the server when it idles out. Live tests stop the server (or
  freeze it behind a proxy) and close statements, connections and the database, with no error,
  no leaked goroutine (goleak) and no Arrow memory left.
- `TestIntegrationIngestPartAnswerLost` failed now and then ("10 part requests, want 9"):
  the answer to the part sent again was itself lost, and the part went a third time. The
  server answers a part or upload sent again with its `Idempotency-Key` from the first
  upload without reading the body, and the Warp SQL SDK then closed the connection with
  the body unread on Linux and macOS, which resets it, so a client still sending got a
  broken pipe or a reset instead of the answer (fixed in warp-sql 2dbcd84, which drains
  before closing on every platform; Hardcharger main cadc02e takes it, through warp-sql 3aefd27). The tests' fault proxy
  had the same defect on its own hop (the answer passed on, the rest of the body unread):
  it now reads every request body to its end. No row was ever written twice: every run
  checked the table row by row. New live test `TestIntegrationIngestAnswerToARetryArrives`
  sends a part and a one-request upload (about 2 MB) again 40 times each, directly and
  through the proxy, and asserts every answer arrives and the rows count once; it fails
  on servers before warp-sql 2dbcd84 (8 of 10 runs on warp-duckdb 17e4609) and passes
  with it.
- The `dbapi` module exports PEP 249's exception classes (`Error`, `Warning`, `InterfaceError`,
  `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`,
  `ProgrammingError`, `NotSupportedError`), as PEP 249 requires of a driver module. They are
  `adbc_driver_manager`'s own classes, so `except dbapi.ProgrammingError` and
  `except adbc_driver_manager.ProgrammingError` catch the same errors.
- The browser sign-in live tests (`TestIntegrationBrowserSignIn`, `…Refused`) started every
  server with warp-duckdb's flags (`--oauth-issuer`, `--oauth-identities`), so against
  hardcharger-server, the server CI runs, they failed at startup. Their server flags and
  client-secret variable now default by `WARP_SQL_ADBC_TEST_SERVER_NAME`: for Hardcharger,
  `--token-allowed-issuer`, `--token-allowed-audience`, `--token-authorized-emails`,
  `--oauth-client-id` and `--oauth-base-url` (browser sign-in on the server's own port, §4.3)
  with `HARDCHARGER_OAUTH_CLIENT_SECRET`; both still take an override.
- Live tests that count ingest parts or cut a result after so many bytes failed now and then
  on a loaded machine (`TestIntegrationIngestInParts`: 32 parts, want 16;
  `TestIntegrationResumeSplitResult`: 0 resumed downloads): the one discovery round trip that
  `auto` decides by took over 10 ms on loopback, so they got the far link's zstd and 32 parts.
  The proxied live tests now count their link as near; the choice by round trip stays
  unit-tested. (Forcing every link far made 8 live tests fail before and none after.)

### Changed (CI)

- CI (`ci.yml`, every push and pull request) runs the live-server suites, Go (race detector)
  and Python, against hardcharger-server from `ghcr.io/gizmodata/hardcharger:main`; they ran
  only in the release workflow before. The release workflow gets them through `ci.yml`.

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
