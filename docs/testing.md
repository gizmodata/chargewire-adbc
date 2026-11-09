# Tests

| Suite | Where | Needs a server | Tests |
|---|---|---|---|
| Go protocol unit | `go/internal/protocol/*_test.go` | no (httptest fake; a TLS one for the certificate_required alert) | 65 |
| Go driver unit | `go/chargewire/*_test.go` without the `integration` tag (`unit_test.go`, `errors_unit_test.go`, `fakeserver_test.go`, `attach_unit_test.go`, `hook_unit_test.go`, `resume_unit_test.go`, `browser_unit_test.go`, `mtls_unit_test.go`, `token_unit_test.go`, …) | no (in-process fakes; a TLS one for certificate sign-in) | 75 |
| Go live | `go/chargewire/*_integration_test.go` (tag `integration`) | yes | 103 |
| Python unit | `python/tests/test_connect_unit.py`, `test_c_abi_unit.py`, `test_dbapi_exceptions_unit.py` | no (the C-ABI test needs the built library) | 33 |
| Python live | `python/tests/test_integration.py`, `test_cancel_integration.py`, `test_polish_integration.py`, `test_error_classes_integration.py` | yes | 95 |
| ADBC validation (conformance) | `validation/` ([adbc-drivers/validation](https://github.com/adbc-drivers/validation), pinned; a test dependency only) | yes (a Hardcharger server of its own: `validation/README.md`) | 332 passed, 4 skipped, 2 expected failures |

(Test functions; parametrized cases and subtests count once.)

The live suites start the binary named by `CHARGEWIRE_ADBC_TEST_SERVER_BINARY` on free ports,
with a fresh password containing `:` and `@`: one shared plain-HTTP server, plus servers of
their own for TLS and mutual TLS. Without the variable they fail, never skip; the Go suite
also fails when no integration test ran (`go test -run Nothing`), and pytest exits 5 when
nothing is collected. The Go live tests run with the race detector (`make integration-test`)
and on a checked Arrow allocator: a test that leaks Arrow memory fails.

Any Chargewire server with chargewire-duckdb's flags works (the certificate sign-in tests also pass
`--mtls-certificate-sign-in --mtls-admin-identities`, which chargewire-duckdb and Hardcharger share). For Hardcharger, name its password
variable, client-CA flag, user-name flag and vendor name:

```bash
export CHARGEWIRE_ADBC_TEST_SERVER_BINARY=/path/to/hardcharger
export CHARGEWIRE_ADBC_TEST_SERVER_PASSWORD_ENV=HARDCHARGER_ADMIN_PASSWORD
export CHARGEWIRE_ADBC_TEST_SERVER_MTLS_FLAG=--mtls-ca-cert
export CHARGEWIRE_ADBC_TEST_SERVER_USERNAME_FLAG=--admin-username
export CHARGEWIRE_ADBC_TEST_SERVER_NAME=Hardcharger
make -C go integration-test && pytest python/tests
```

Servers from before the protocol's rename (its 1.0.x reference server) are tested when
`CHARGEWIRE_ADBC_TEST_LEGACY_SERVER_BINARY` names one: Go `TestIntegrationLegacyServer` and Python
`TestLegacyServer` (connect, query, prepared parameters, bulk ingest, a SQL error's class and
code, a statement timeout, GetObjects and GetTableSchema). Without the variable those tests skip;
the rest of the suite still runs. The same files show that a current server is never asked for
the earlier spelling (a recording proxy sees only `/chargewire`).

The browser sign-in tests start a server of their own against a mock OpenID Connect provider.
Its flags come from `CHARGEWIRE_ADBC_TEST_SERVER_OAUTH_ARGS` (a template with `{issuer}`,
`{client_id}`, `{identity}` and `{base_url}`) and its client secret goes in the variable
`CHARGEWIRE_ADBC_TEST_SERVER_OAUTH_SECRET_ENV` names; both default by
`CHARGEWIRE_ADBC_TEST_SERVER_NAME`: chargewire-duckdb's `--oauth-issuer …` flags, or, for
`Hardcharger`, `--token-allowed-issuer … --oauth-client-id … --oauth-base-url …` with
`HARDCHARGER_OAUTH_CLIENT_SECRET` (browser sign-in on the server's own port, §4.3).

Negative tests assert the ADBC status and the Chargewire error code (`chargewire.code` in the error
details), not just that something failed.

Some tests prove their oracle first: `TestIntegrationCancelOracle` and
`test_running_query_holds_the_session` show that a running statement holds its session, so
"the session answers `SELECT 42` again" really means the server stopped the statement; and
disabling the server-side cancel in the driver makes the cancel tests fail (checked by hand).

## Ported from gizmosql-adbc

Everything that applies was ported; the Chargewire server runs statements on submit, so the
gizmosql tests about routing DDL/DML around lazy execution became tests that DDL/DML runs
and persists without a fetch.

| gizmosql-adbc | Here |
|---|---|
| Go `TestIntegrationSelectOne` | `TestIntegrationSelectOne` |
| Go `TestIntegrationLegacySchemeParity` | `TestIntegrationURISpellings` (chargewire://, http://, base paths) |
| Go bind tests (5) | `TestIntegrationBindWithoutPrepare{Query,Update}`, `ExplicitPrepareThenBindPreparesOnce` (the prepared id is reused), `RebindAfterNewQueryReprepares`; bulk ingest never prepares (`TestIntegrationIngest*`) |
| Go cancel tests: interrupt, close while executing, abandoned result, close while updating | the same four, plus cancel with an unknown statement id (alone, and beside another statement), async cancel, context deadline, statement timeout, nothing to cancel |
| Go routing tests: DDL/DML without fetch, RETURNING rows and persistence, string literal, select streams, bound DML/RETURNING/select | `TestIntegrationDDLDMLThroughExecuteQuery`, `ReturningYieldsRowsAndPersists`, `ReturningPersistsWithoutFetch`, `StringLiteralReturning`, `ManyBatches`, `BoundDMLAndSelect`; bound RETURNING in Python |
| Python `TestPasswordAuth`, `TestExecuteAutoDetect`, `TestExecuteUpdate`, `TestBulkIngest`, `TestReturningClause`, `TestBoundParameterRouting`, `TestConnectionContextManager`, `TestConnectionProfiles`, `TestOpenTelemetryTracing`, `TestConnectCatalog` | the same classes (`TestExecuteDDLDML`, `TestBoundParameters`), with `SELECT version()` for `GIZMOSQL_VERSION()` and DDL reporting -1 rows (the server gives no count) |
| Python `TestGizmoSqlUriScheme` | `TestUriSpellings`, `TestTLS` |
| Python cancel tests (4) | the same four; Ctrl+C now also proves the session is free afterwards |
| Python connect unit tests | ported, without the OAuth cases |

Not ported, and why:

| gizmosql-adbc | Why not |
|---|---|
| OAuth/SSO tests (Go `oauth_test.go`, Python `test_oauth_unit.py`, external-auth connect cases) | GizmoSQL's own OAuth server and its routes. Chargewire's browser flow (`oauth_browser`, spec §4.3) has its own tests: `browser_unit_test.go`, `browser_integration_test.go` (chargewire-duckdb with a mock OpenID Connect provider), Python `test_connect_unit.py` and `test_browser_integration.py` |
| SQL classification unit tests (Go `sqlrouting_test.go`, Python `test_sql_routing_unit.py`, `TestIsDdlDml`) | There is no client-side routing: a Chargewire server runs every statement on submit |
| Flight cancel plumbing unit tests (`cancel_test.go`), log filter (`logging_test.go`) | Specific to Flight's `CancelFlightInfo` and its log noise; the Chargewire equivalents are the protocol and fake-server tests |
| `TestIntegrationConnectionCancelQuery` | A GizmoSQL extension that cancelled the session's statement from the connection; `AdbcConnectionCancel` cancels connection-level calls, which the C layer does through contexts |
| `TestIntegrationConnectionOptionsDelegate`, scheme-rewrite tests | The gizmosql wrapper around the upstream Flight SQL driver; there is no wrapper here (URI parsing is `TestParseURI`) |
| `TestIntegrationGeoArrowIngest` | GizmoSQL's geometry-preserving ingest; a Chargewire server must keep or refuse types itself (§8.1), tested by the unsupported-type refusal |
| Python module-level `execute_update()` and the `get_info` cache | 1.x compatibility shims of adbc-driver-gizmosql |

## Added for Chargewire

Resumed downloads (streamed with retention, async, twice in one result) and unretained ones
failing with the remedy, through a fault-injecting proxy; lost answers to submits and
uploads retried with the same `Idempotency-Key` and run once; the answer to a part or
upload sent again arriving every time, directly and through the proxy (which reads every
request body to its end, so it loses no answer itself); retried status polls;
failures mid-stream; lost sessions (replaced with options replayed, temporary state
reported, refused inside a transaction begun with autocommit off or with SQL `BEGIN`, for plain,
queried and prepared statements and metadata calls, run again after a SQL `COMMIT`, refused after
SQL `USE` (and run again without it, or with the state marked restored), commit
refused, reconnect off, prepared statement re-prepared, retained result lost); a session the
server ended for idling in a transaction (`idle_in_transaction`, a server of its own started with
the idle-in-transaction timeout flag named by `CHARGEWIRE_ADBC_TEST_SERVER_IDLE_IN_TRANSACTION_FLAG`,
Hardcharger's `--idle-in-transaction-session-timeout`): replaced, the statement not run again,
the connection usable; nested schemas (`sales.payroll`, and a top-level `"sales.payroll"`) in
GetObjects, its schema filter and GetTableSchema; partitions read on another connection; zstd and lz4_frame results and
uploads; query tags; ingest modes, temporary tables, target catalog/schema, a 2M-row
streamed upload; GetObjects at every depth with LIKE patterns and escaping; GetTableSchema
resolution; GetInfo; current catalog/schema; transactions; TLS, mutual TLS, server name,
skip-verify; client certificate sign-in (no password; a split result read with the bound token; an unknown identity refused with `unknown_certificate_identity`; a bound token over another certificate refused with `certificate_mismatch`, and accepted over its own; a server without client CAs refused before any grant is sent); wrong passwords, tokens, unsupported grants, unreachable and non-Chargewire servers,
bad URIs; a TLS URI pointed at a plain-HTTP server (the hint to add `?tls=false`, and a
recording TCP proxy showing that only the TLS hello, no credential, was sent); closing
statements, connections and the database after the server stopped or while it hangs behind a
frozen proxy (no error, at most 5 s, no leaked goroutine by goleak). The fake servers cover what the reference server cannot be made to do: token
expiry mid-connection, partitions on other servers, concurrent session loss, `session_ended`
reasons, `Retry-After`, and a resuming server (`server_resuming` answered to the sign-in and to
a statement more often than `retry.max_attempts`, then success, each repeat the same request;
the resume deadline returning the server's error and message; no `Retry-After` waiting 3 s, `0`
waiting 1 s, a long one capped at 60 s; `resume_timeout=0` sending once; streamed and best-effort requests not
repeated; other 503s keeping their bounded retry; a sign-in held 25 s through the driver's real
transport; and in Python, through the shared library, `resume_timeout` given as a `connect()`
argument and as a `db_kwargs` option), and a stopped server (discovery `"state": "stopped"`:
connecting, a pool's checks and closing send nothing but discovery, in Go and through
Python's `connect()`; the first statement signs in, opens the session with the options set
before it and runs through a `server_resuming` wait; a wrong password fails that statement,
not the connect; without `state` connecting signs in at once; one database whose server stops
between Opens reads discovery again and opens lazily, and one whose server started signs in at
Open; a connection opened after a stop that revoked the database's token signs in afresh, while
one from before the stop doesn't sign in again).
