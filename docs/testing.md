# Tests

| Suite | Where | Needs a server | Tests |
|---|---|---|---|
| Go protocol unit | `go/internal/protocol/*_test.go` | no (httptest fake) | 17 |
| Go driver unit | `go/warpsql/unit_test.go`, `fakeserver_test.go` | no (in-process fake) | 12 |
| Go live | `go/warpsql/*_integration_test.go` (tag `integration`) | yes | 72 (+14 subtests) |
| Python unit | `python/tests/test_connect_unit.py`, `test_c_abi_unit.py` | no (the C-ABI test needs the built library) | 27 |
| Python live | `python/tests/test_integration.py`, `test_cancel_integration.py` | yes | 68 |

The live suites start the binary named by `WARP_SQL_ADBC_TEST_SERVER_BINARY` on free ports,
with a fresh password containing `:` and `@`: one shared plain-HTTP server, plus servers of
their own for TLS and mutual TLS. Without the variable they fail, never skip; the Go suite
also fails when no integration test ran (`go test -run Nothing`), and pytest exits 5 when
nothing is collected. The Go live tests run with the race detector (`make integration-test`)
and on a checked Arrow allocator: a test that leaks Arrow memory fails.

Any Warp SQL server with warp-duckdb's flags works. For Hardcharger, name its password
variable, client-CA flag and vendor name:

```bash
export WARP_SQL_ADBC_TEST_SERVER_BINARY=/path/to/hardcharger
export WARP_SQL_ADBC_TEST_SERVER_PASSWORD_ENV=HARDCHARGER_PASSWORD
export WARP_SQL_ADBC_TEST_SERVER_MTLS_FLAG=--mtls-ca-cert
export WARP_SQL_ADBC_TEST_SERVER_NAME=Hardcharger
make -C go integration-test && pytest python/tests
```

Negative tests assert the ADBC status and the Warp SQL error code (`warp.code` in the error
details), not just that something failed.

Some tests prove their oracle first: `TestIntegrationCancelOracle` and
`test_running_query_holds_the_session` show that a running statement holds its session, so
"the session answers `SELECT 42` again" really means the server stopped the statement; and
disabling the server-side cancel in the driver makes the cancel tests fail (checked by hand).

## Ported from gizmosql-adbc

Everything that applies was ported; the Warp SQL server runs statements on submit, so the
gizmosql tests about routing DDL/DML around lazy execution became tests that DDL/DML runs
and persists without a fetch.

| gizmosql-adbc | Here |
|---|---|
| Go `TestIntegrationSelectOne` | `TestIntegrationSelectOne` |
| Go `TestIntegrationLegacySchemeParity` | `TestIntegrationURISpellings` (warp://, http://, base paths) |
| Go bind tests (5) | `TestIntegrationBindWithoutPrepare{Query,Update}`, `ExplicitPrepareThenBindPreparesOnce` (the prepared id is reused), `RebindAfterNewQueryReprepares`; bulk ingest never prepares (`TestIntegrationIngest*`) |
| Go cancel tests: interrupt, close while executing, abandoned result, close while updating | the same four, plus cancel with an unknown statement id, async cancel, context deadline, statement timeout, nothing to cancel |
| Go routing tests: DDL/DML without fetch, RETURNING rows and persistence, string literal, select streams, bound DML/RETURNING/select | `TestIntegrationDDLDMLThroughExecuteQuery`, `ReturningYieldsRowsAndPersists`, `ReturningPersistsWithoutFetch`, `StringLiteralReturning`, `ManyBatches`, `BoundDMLAndSelect`; bound RETURNING in Python |
| Python `TestPasswordAuth`, `TestExecuteAutoDetect`, `TestExecuteUpdate`, `TestBulkIngest`, `TestReturningClause`, `TestBoundParameterRouting`, `TestConnectionContextManager`, `TestConnectionProfiles`, `TestOpenTelemetryTracing`, `TestConnectCatalog` | the same classes (`TestExecuteDDLDML`, `TestBoundParameters`), with `SELECT version()` for `GIZMOSQL_VERSION()` and DDL reporting -1 rows (the server gives no count) |
| Python `TestGizmoSqlUriScheme` | `TestUriSpellings`, `TestTLS` |
| Python cancel tests (4) | the same four; Ctrl+C now also proves the session is free afterwards |
| Python connect unit tests | ported, without the OAuth cases |

Not ported, and why:

| gizmosql-adbc | Why not |
|---|---|
| OAuth/SSO tests (Go `oauth_test.go`, Python `test_oauth_unit.py`, external-auth connect cases) | GizmoSQL's own OAuth server. Warp SQL's browser flow is the optional `oauth_browser` capability, which the reference server does not offer and the driver does not implement yet |
| SQL classification unit tests (Go `sqlrouting_test.go`, Python `test_sql_routing_unit.py`, `TestIsDdlDml`) | There is no client-side routing: a Warp SQL server runs every statement on submit |
| Flight cancel plumbing unit tests (`cancel_test.go`), log filter (`logging_test.go`) | Specific to Flight's `CancelFlightInfo` and its log noise; the Warp SQL equivalents are the protocol and fake-server tests |
| `TestIntegrationConnectionCancelQuery` | A GizmoSQL extension that cancelled the session's statement from the connection; `AdbcConnectionCancel` cancels connection-level calls, which the C layer does through contexts |
| `TestIntegrationConnectionOptionsDelegate`, scheme-rewrite tests | The gizmosql wrapper around the upstream Flight SQL driver; there is no wrapper here (URI parsing is `TestParseURI`) |
| `TestIntegrationGeoArrowIngest` | GizmoSQL's geometry-preserving ingest; a Warp SQL server must keep or refuse types itself (§8.1), tested by the unsupported-type refusal |
| Python module-level `execute_update()` and the `get_info` cache | 1.x compatibility shims of adbc-driver-gizmosql |

## Added for Warp SQL

Resumed downloads (streamed with retention, async, twice in one result) and unretained ones
failing with the remedy, through a fault-injecting proxy; lost answers to submits and
uploads retried with the same `Idempotency-Key` and run once; retried status polls;
failures mid-stream; lost sessions (replaced with options replayed, temporary state
reported, refused inside a transaction, commit refused, reconnect off, prepared statement
re-prepared, retained result lost); partitions read on another connection; zstd results and
uploads; query tags; ingest modes, temporary tables, target catalog/schema, a 2M-row
streamed upload; GetObjects at every depth with LIKE patterns and escaping; GetTableSchema
resolution; GetInfo; current catalog/schema; transactions; TLS, mutual TLS, server name,
skip-verify; wrong passwords, tokens, unsupported grants, unreachable and non-Warp servers,
bad URIs. The fake servers cover what the reference server cannot be made to do: token
expiry mid-connection, partitions on other servers, concurrent session loss, `session_ended`
reasons, `Retry-After`.
