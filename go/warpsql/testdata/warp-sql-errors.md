<!--
The error table of the Warp SQL spec (spec/warp-sql.md §11), copied from
gizmodata/warp-sql spec/v1.0 at eaf71d3. TestErrorStatusesCoverTheSpec reads
it: every code in it must have a status in codeStatuses (errors.go). When the
spec gains a code, copy the new table here; with WARP_SQL_ADBC_TEST_SPEC set to
a warp-sql.md, the test also fails while this copy is behind that spec.
-->

| Code | Status | Meaning |
|---|---|---|
| `bad_request` | 400 | Malformed request |
| `unknown_option`, `invalid_option_value` | 400 | A session option the server doesn't have, or a bad value (§5.3) |
| `invalid_return_to` | 400 | A browser sign-in's `return_to` that is not a path on the server's own origin (§4.3.3) |
| `sql_error` | 400 | The engine rejected the SQL (syntax, binding, missing object) |
| `unsupported_grant_type` | 400 | A grant this server does not offer, such as `client_certificate` where certificate sign-in is off (§4.4) |
| `invalid_credentials` | 401 | §4.1 |
| `invalid_token` | 401 | §4.2 |
| `session_ended` | 401 | §5.6 |
| `certificate_required`, `unknown_certificate_identity`, `certificate_revoked` | 401 | §4.4 |
| `certificate_mismatch` | 401 | A certificate-bound token on a connection without that certificate (§4.4) |
| `certificate_identity_mismatch` | 401 | A sign-in for another identity than the connection's certificate (§4.4) |
| `permission_denied` | 403 | The principal lacks a privilege |
| `read_only` | 403 | The server or session is read-only |
| `session_not_found`, `statement_not_found`, `prepared_not_found`, `ingest_not_found` | 404 | No such handle, or not the caller's (§9.2); `statement_not_found` also for a statement the server has forgotten (§6.5) |
| `flow_not_found` | 404 | No such browser sign-in flow, a wrong poll secret, or one already collected or expired (§4.3.2) |
| `partition_not_found` | 404 | The result has no partition with that index (§6.4) |
| `not_found` | 404 | No such endpoint |
| `method_not_allowed` | 405 | The endpoint exists but not for this method |
| `result_not_ready` | 409 | The statement has no result schema yet (§6.4) |
| `result_not_resumable` | 409 | §6.5 |
| `ingest_incomplete`, `ingest_part_exists` | 409 | §8.3 |
| `statement_cancelled` | 409 | The statement was cancelled (§6.2, §6.8); `cancel_reason` says why |
| `statement_timeout` | 409 | The statement ran out of time and was cancelled (§6.9) |
| `result_expired` | 410 | §6.5 |
| `ingest_expired` | 410 | §8.3 |
| `payload_too_large` | 413 | A JSON request body over the server's limit |
| `unsupported_arrow_type` | 415 | §8.1 |
| `session_elsewhere` | 421 | §5.6 |
| `idempotency_key_reused` | 422 | §6.7 |
| `execution_error` | 422 | The statement failed while running: the engine could not complete what it asked (a division by zero, a constraint, a conversion) |
| `too_many_sessions`, `result_quota_exceeded`, `rate_limited` | 429 | A limit was reached (with `Retry-After` when retrying can help) |
| `headers_too_large` | 431 | §2.5 |
| `internal_error` | 500 | The server failed in a way that is not the request's doing (a bug, a broken invariant) |
| `not_implemented` | 501 | The server does not implement this operation (browser sign-in on a server without `oauth_browser`, §4.3) |
| `server_draining` | 503 | The server is shutting down; with `Retry-After` |
| `access_denied`, `identity_provider_error` | — | A failed browser sign-in's `error.code` in the poll answer, as is `invalid_credentials` (§4.3.2); never a response's status |
