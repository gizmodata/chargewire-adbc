# Spec and server findings

What building the driver found in the Warp SQL spec (draft 0.1.0) and in `warp-duckdb`
(warp-sql commit `d07e697`). Each has what the driver does about it. The repros assume a
server started with `WARP_DUCKDB_PASSWORD=pw warp-duckdb --host 127.0.0.1 --port 47001` and:

```bash
B=http://127.0.0.1:47001/warp/v1
T=$(curl -s $B/auth/token -d '{"grant_type":"password","username":"warp","password":"pw"}' | jq -r .access_token)
S=$(curl -s $B/sessions -H "Authorization: Bearer $T" -d '{}' | jq -r .session_id)
A="Authorization: Bearer $T"
```

## Server

1. **A long poll on a finished statement waits the whole wait.** §6.3 holds the answer
   "until the state changes or N seconds pass"; a terminal statement never changes, so the
   answer should be immediate. Every async statement that finishes between the submit's
   answer and the first poll costs the full wait.
   ```bash
   Q=$(curl -s $B/sessions/$S/statements -H "$A" -H 'Prefer: respond-async' \
        -d '{"sql":"SELECT count(*) FROM range(300000000)"}' | jq -r .statement_id)
   sleep 1; time curl -s $B/sessions/$S/statements/$Q -H "$A" -H 'Prefer: wait=5' | jq .state
   # "succeeded" after 5.0 s
   ```
   Driver: polls with waits of 1, 2, 4, 8, then 15 seconds.

2. **Finished statements stay in their session.** A session keeps every statement until it is
   deleted or the session ends (`Session::statements_` is only erased by DELETE), so a
   long-lived connection grows without bound. The spec says nothing about how long a
   statement handle lives. Driver: deletes each statement in the background once its result
   is read or abandoned (not partitions, which are read later).

3. **The streamed path sends the schema only with the first batch.** Headers (with
   `Warp-Statement-Id`) come at once, but the Arrow schema message waits for the first
   record batch. For an aggregate, `ExecuteQuery` therefore waits for the whole query, and
   the response is silent meanwhile, which a proxy's idle timeout cuts. The schema is known
   earlier: the running statement's document already carries it.
   ```bash
   time (curl -sN $B/sessions/$S/statements -H "$A" -H 'Accept: application/vnd.apache.arrow.stream' \
        -d '{"sql":"SELECT sum(a.range*b.range) FROM range(10000000) a, range(1000) b"}' | head -c 1 | wc -c)
   # the first byte arrives when the query ends (18 s on an M-series Mac)
   ```
   Suggest writing the schema message as soon as it is known. Driver: documents `async` for
   long queries behind proxies.

4. **A prepared query's `result_schema` is null.** `POST …/prepared {"sql":"SELECT x FROM t
   WHERE x > ?"}` returns `"result_schema": null` although DuckDB knows the schema at
   prepare. Driver: `ExecuteSchema` returns NotImplemented.

5. **`table_schema` drops nullability and most Flight SQL column metadata.** For
   `CREATE TABLE t (a INT PRIMARY KEY, b VARCHAR NOT NULL, c DECIMAL(10,3))`,
   `GET …/metadata/tables?include_schema=true` reports every field nullable and only
   `TYPE_NAME` (plus catalog/schema/table names): no `PRECISION`, `SCALE`,
   `IS_AUTO_INCREMENT`, `IS_NULLABLE`. Driver: takes precision and scale of decimals from the
   Arrow type; `xdbc_nullable` reflects what the server says (always nullable).

6. **The session document does not follow SQL `USE`.** After `USE other.s2`,
   `GET /sessions/{id}` still reports the old `schema`. So ADBC's current catalog/schema
   (read from the document) is stale after `USE`. §5.2 should say whether the document
   reports the session's live state.

7. **Ingest create/append failures have no stable code.** `mode=create` on an existing table
   and `mode=append` on a missing one answer `sql_error` without `sqlstate` (the server does
   send `42601` for parser errors). ADBC callers expect AlreadyExists / NotFound. Suggest
   `42P07` / `42P01`, or codes.

8. **An Arrow-body execute with zero parameter rows** answers `{"kind":"update",
   "rows_affected":0}` even for a `SELECT`; a query should give its (empty) rows.

9. **Token lifetime and idle timeout are not configurable** in warp-duckdb, so a client's
   renewal and eviction handling can only be tested against fakes. Flags would help the
   conformance suite too.

10. **The conformance suite's "runs until stopped" query is not slow:**
    `SELECT count(*) FROM range(100000000000)` finishes in about 4 s on an M-series Mac, so
    the cancel and timeout tests pass only because they act within that time. A cross join
    (`… FROM range(100000000) a, range(100000) b`) really runs until stopped.

## Spec

11. **A statement cannot be cancelled before its answer arrives.** A JSON submit held by
    `Prefer: wait=N` (or a streamed submit on a server that answers late) gives the client no
    statement id to cancel. The driver re-sends the same request with the same
    `Idempotency-Key` and `Prefer: respond-async`, which §6.7 answers with the existing
    statement, but if the original never reached the server this creates the statement
    (which is then cancelled at once). Suggest client-chosen statement ids, cancel by
    `Idempotency-Key`, or a session-level cancel.

12. **`rows_affected` is optional in practice.** DDL answers `{"kind":"update"}` without it;
    the spec's example always has it. Say it is optional (the driver reports -1, unknown)
    or 0 for DDL.

13. **A partition's `location` form is unspecified.** §6.2 calls it "the base URL of another
    server". The driver treats it as that server's protocol base path (like
    `https://host/warp`) and requests `{location}/v1/sessions/{sid}/statements/{qid}/result?partition=i`
    there, assuming the same ids. The spec should give the URL form (see also Appendix A.4).

14. **`result_schema: null` means two things**: "returns no rows" or "not known until
    execution". A client cannot tell DDL from an undescribed query. Suggest a separate
    `result_kind` or "unknown" marker.

15. **Key metadata needs a table.** `primary-keys` and `imported-keys` require `table`, as in
    Flight SQL, so ADBC `GetObjects` cannot report constraints without one request per table.
    Allowing them without `table` (all visible tables) would make constraints one request.

16. **server-info key names differ:** the spec's example names `sql_transactions_supported`;
    the server returns `transactions_supported` (Appendix A.2 is still open).
