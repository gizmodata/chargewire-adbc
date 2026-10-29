# Spec and server findings

> History: these were found against the spec's draft 0.1.0. The protocol is now version 1
> (spec 1.0.0), which changed some names quoted below (for example, server-info keys are
> Flight SQL `SqlInfo` names such as `sql_transactions_supported`).

What building the driver found in the Chargewire spec (draft 0.1.0) and in `chargewire-duckdb`
(chargewire commit `d07e697`). **All sixteen are resolved in chargewire `188cd84`**; each item
names the commit that fixed it and what the driver does now. The repros below show the
old behaviour. The repros assume a
server started with `CHARGEWIRE_DUCKDB_PASSWORD=pw chargewire-duckdb --host 127.0.0.1 --port 47001` and:

```bash
B=http://127.0.0.1:47001/chargewire/v1
T=$(curl -s $B/auth/token -d '{"grant_type":"password","username":"chargewire","password":"pw"}' | jq -r .access_token)
S=$(curl -s $B/sessions -H "Authorization: Bearer $T" -d '{}' | jq -r .session_id)
A="Authorization: Bearer $T"
```

## Server

1. **Resolved in chargewire `756fff9`.** The driver polls with a fixed 15 s wait again (the 1/2/4/8 s escalation is gone). **A long poll on a finished statement waits the whole wait.** §6.3 holds the answer
   "until the state changes or N seconds pass"; a terminal statement never changes, so the
   answer should be immediate. Every async statement that finishes between the submit's
   answer and the first poll costs the full wait.
   ```bash
   Q=$(curl -s $B/sessions/$S/statements -H "$A" -H 'Prefer: respond-async' \
        -d '{"sql":"SELECT count(*) FROM range(300000000)"}' | jq -r .statement_id)
   sleep 1; time curl -s $B/sessions/$S/statements/$Q -H "$A" -H 'Prefer: wait=5' | jq .state
   # "succeeded" after 5.0 s
   ```

2. **Resolved in chargewire `756fff9`.** The server forgets finished statements after 10 minutes, or beyond 1,000 per session; the driver still deletes each one when done, in the background. **Finished statements stay in their session.** A session keeps every statement until it is
   deleted or the session ends (`Session::statements_` is only erased by DELETE), so a
   long-lived connection grows without bound. The spec says nothing about how long a
   statement handle lives.

3. **Resolved in chargewire `756fff9`.** No driver change needed. **The streamed path sends the schema only with the first batch.** Headers (with
   `Chargewire-Statement-Id`) come at once, but the Arrow schema message waits for the first
   record batch. For an aggregate, `ExecuteQuery` therefore waits for the whole query, and
   the response is silent meanwhile, which a proxy's idle timeout cuts. The schema is known
   earlier: the running statement's document already carries it.
   ```bash
   time (curl -sN $B/sessions/$S/statements -H "$A" -H 'Accept: application/vnd.apache.arrow.stream' \
        -d '{"sql":"SELECT sum(a.range*b.range) FROM range(10000000) a, range(1000) b"}' | head -c 1 | wc -c)
   # the first byte arrives when the query ends (18 s on an M-series Mac)
   ```
   Suggest writing the schema message as soon as it is known.

4. **Resolved in chargewire `eb8d730`.** `ExecuteSchema` uses `result_kind` and `result_schema`: rows give their schema, an update the empty schema. **A prepared query's `result_schema` is null.** `POST …/prepared {"sql":"SELECT x FROM t
   WHERE x > ?"}` returns `"result_schema": null` although DuckDB knows the schema at
   prepare.

5. **Resolved in chargewire `dff3e78`.** The driver no longer takes precision and scale from the Arrow decimal type; nullability comes from the server. **`table_schema` drops nullability and most Flight SQL column metadata.** For
   `CREATE TABLE t (a INT PRIMARY KEY, b VARCHAR NOT NULL, c DECIMAL(10,3))`,
   `GET …/metadata/tables?include_schema=true` reports every field nullable and only
   `TYPE_NAME` (plus catalog/schema/table names): no `PRECISION`, `SCALE`,
   `IS_AUTO_INCREMENT`, `IS_NULLABLE`.

6. **Resolved in chargewire `dff3e78`.** No driver change needed: current catalog/schema follow `USE`. **The session document does not follow SQL `USE`.** After `USE other.s2`,
   `GET /sessions/{id}` still reports the old `schema`. So ADBC's current catalog/schema
   (read from the document) is stale after `USE`. §5.2 should say whether the document
   reports the session's live state.

7. **Resolved in chargewire `dff3e78`.** The driver reports `42P01` as NotFound and `42P07` as AlreadyExists. **Ingest create/append failures have no stable code.** `mode=create` on an existing table
   and `mode=append` on a missing one answer `sql_error` without `sqlstate` (the server does
   send `42601` for parser errors). ADBC callers expect AlreadyExists / NotFound. Suggest
   `42P07` / `42P01`, or codes.

8. **Resolved in chargewire `eb8d730`.** No driver change needed. **An Arrow-body execute with zero parameter rows** answers `{"kind":"update",
   "rows_affected":0}` even for a `SELECT`; a query should give its (empty) rows.

9. **Resolved in chargewire `dff3e78`.** `--token-lifetime-seconds`, `--session-idle-timeout-seconds`, `--finished-statement-ttl-seconds`. **Token lifetime and idle timeout are not configurable** in chargewire-duckdb, so a client's
   renewal and eviction handling can only be tested against fakes. Flags would help the
   conformance suite too.

10. **Resolved in chargewire `756fff9`.** The conformance query is `sum(i)` now. **The conformance suite's "runs until stopped" query is not slow:**
    `SELECT count(*) FROM range(100000000000)` finishes in about 4 s on an M-series Mac, so
    the cancel and timeout tests pass only because they act within that time. A cross join
    (`… FROM range(100000000) a, range(100000) b`) really runs until stopped.

## Spec

11. **Resolved in chargewire `80c903b`.** The driver cancels an unanswered statement with `POST /sessions/{id}/cancel` when it is the connection's only statement running; beside other statements (which a session cancel would also stop) or on a server without it, it still resends with the `Idempotency-Key`. **A statement cannot be cancelled before its answer arrives.** A JSON submit held by
    `Prefer: wait=N` (or a streamed submit on a server that answers late) gives the client no
    statement id to cancel. The driver re-sends the same request with the same
    `Idempotency-Key` and `Prefer: respond-async`, which §6.7 answers with the existing
    statement, but if the original never reached the server this creates the statement
    (which is then cancelled at once). Suggest client-chosen statement ids, cancel by
    `Idempotency-Key`, or a session-level cancel.

12. **Resolved in chargewire `80c903b`.** No driver change needed: absent means unknown, -1. **`rows_affected` is optional in practice.** DDL answers `{"kind":"update"}` without it;
    the spec's example always has it. Say it is optional (the driver reports -1, unknown)
    or 0 for DDL.

13. **Resolved in chargewire `80c903b`.** The spec's URL form is the one the driver used. **A partition's `location` form is unspecified.** §6.2 calls it "the base URL of another
    server". The driver treats it as that server's protocol base path (like
    `https://host/chargewire`) and requests `{location}/v1/sessions/{sid}/statements/{qid}/result?partition=i`
    there, assuming the same ids. The spec should give the URL form (see also Appendix A.4).

14. **Resolved in chargewire `eb8d730`.** See 4. **`result_schema: null` means two things**: "returns no rows" or "not known until
    execution". A client cannot tell DDL from an undescribed query. Suggest a separate
    `result_kind` or "unknown" marker.

15. **Resolved in chargewire `80c903b`.** `GetObjects` fills `table_constraints` (primary and foreign keys) with one request to each key endpoint. **Key metadata needs a table.** `primary-keys` and `imported-keys` require `table`, as in
    Flight SQL, so ADBC `GetObjects` cannot report constraints without one request per table.
    Allowing them without `table` (all visible tables) would make constraints one request.

16. **Resolved in chargewire `80c903b`.** No driver change needed. **server-info key names differ:** the spec's example names `sql_transactions_supported`;
    the server returns `transactions_supported` (Appendix A.2 is still open).

Also in `188cd84`: deleting a statement means "done with it" (§6.8). A running query is
cancelled, but a running write finishes with its rows discarded, as when a client hangs up
mid-stream; `POST …/cancel` stops anything. The driver deletes on release (so
`INSERT … RETURNING` persists unread) and sends `POST …/cancel` whenever the application
asks to stop a statement.
