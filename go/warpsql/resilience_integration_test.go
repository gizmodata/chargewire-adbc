// Licensed under the Apache License, Version 2.0.

//go:build integration

// What the driver does when the network or the server lets it down: cut
// result streams, lost answers, sessions the server no longer has.

package warpsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

const bigQuery = "SELECT i, i * 2 AS j FROM range(1000000) t(i)"

// checkBig verifies the rows of bigQuery.
func checkBig(t *testing.T, table arrow.Table) {
	t.Helper()
	values := int64s(t, table, 0)
	if len(values) != 1_000_000 {
		t.Fatalf("got %d rows, want 1000000", len(values))
	}
	for i, v := range values {
		if v != int64(i) {
			t.Fatalf("row %d = %d: rows lost, repeated or reordered", i, v)
		}
	}
}

func viaProxy(t *testing.T, extra map[string]string) (*faultProxy, adbc.ConnectionWithContext) {
	t.Helper()
	proxy := newFaultProxy(t, shared)
	options := map[string]string{adbc.OptionKeyURI: proxy.uri()}
	for key, value := range extra {
		options[key] = value
	}
	return proxy, connect(t, openDB(t, shared, options))
}

func fromBatchRequests(proxy *faultProxy) []*http.Request {
	return proxy.requests(func(r *http.Request) bool {
		return isResultFetch(r) && r.URL.Query().Get("from_batch") != ""
	})
}

// A retained result whose streamed download breaks off resumes from the next
// batch (§6.5), and nothing is lost or repeated.
func TestIntegrationResumeStreamedRetainedResult(t *testing.T) {
	proxy, cnxn := viaProxy(t, map[string]string{OptionRetainResult: "true"})
	proxy.add(&faultRule{match: isPost("/statements"), cutAfter: 5_000_000, times: 1})
	checkBig(t, queryTable(t, cnxn, bigQuery))
	if len(fromBatchRequests(proxy)) == 0 {
		t.Fatal("the download was not resumed with from_batch")
	}
}

func TestIntegrationResumeAsyncResult(t *testing.T) {
	proxy, cnxn := viaProxy(t, map[string]string{OptionAsync: "true"})
	proxy.add(&faultRule{match: isResultFetch, cutAfter: 5_000_000, times: 2})
	checkBig(t, queryTable(t, cnxn, bigQuery))
	if got := len(fromBatchRequests(proxy)); got != 2 {
		t.Fatalf("%d resumed downloads, want 2", got)
	}
}

// An unretained result cannot be resumed; the error says what would help.
func TestIntegrationInterruptedUnretainedResultFails(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	proxy.add(&faultRule{match: isPost("/statements"), cutAfter: 300_000, times: 1})
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, bigQuery)
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err == nil {
		for reader.Next() {
		}
		err = reader.Err()
		reader.Release()
	}
	assertStatus(t, err, adbc.StatusIO, "")
	if !strings.Contains(err.Error(), "did not retain the result") || !strings.Contains(err.Error(), OptionRetainResult) {
		t.Errorf("the error does not explain the cause and the remedy: %v", err)
	}
}

// A submit whose answer is lost is sent again with the same Idempotency-Key,
// and the server does not run it twice (§6.7).
func TestIntegrationLostSubmitAnswerRunsOnce(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	execUpdate(t, cnxn, "CREATE TABLE once (v INTEGER)")
	proxy.add(&faultRule{match: isPost("/statements"), times: 1})
	if n := execUpdate(t, cnxn, "INSERT INTO once VALUES (1)"); n != 1 {
		t.Fatalf("rows affected %d", n)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM once"); got != 1 {
		t.Fatalf("the retried INSERT ran %d times", got)
	}
	submits := proxy.requests(isPost("/statements"))
	keys := map[string]int{}
	for _, r := range submits {
		keys[r.Header.Get("Idempotency-Key")]++
	}
	insertKey := submits[1].Header.Get("Idempotency-Key")
	if insertKey == "" || keys[insertKey] != 2 {
		t.Fatalf("the retry did not reuse the Idempotency-Key: %v", keys)
	}
}

// The same holds for a bulk upload kept in memory.
func TestIntegrationLostIngestAnswerRunsOnce(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	proxy.add(&faultRule{match: isPost("/ingest"), times: 1})
	rec := keyValueBatch([]int64{1, 2, 3}, []string{"a", "b", "c"})
	defer rec.Release()
	n, err := ingest(t, cnxn, "ing_retry", adbc.OptionValueIngestModeCreateAppend, rec, nil)
	if err != nil || n != 3 {
		t.Fatalf("ingest: %d rows, %v", n, err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ing_retry"); got != 3 {
		t.Fatalf("the retried upload wrote %d rows, want 3", got)
	}
	if got := len(proxy.requests(isPost("/ingest"))); got != 2 {
		t.Fatalf("%d ingest requests, want 2", got)
	}
}

// A failure while the result streams is the statement's error (§6.6).
func TestIntegrationFailureMidStream(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "SELECT CASE WHEN i < 300000 THEN i ELSE error('boom') END AS v FROM range(1000000) t(i)")
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err == nil {
		for reader.Next() {
		}
		err = reader.Err()
		reader.Release()
	}
	assertStatus(t, err, adbc.StatusInternal, "execution_error")
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("the error lost the server's message: %v", err)
	}
}

// killSession ends a connection's session behind the driver's back, as an
// idle eviction or a server restart would.
func killSession(t *testing.T, cnxn adbc.ConnectionWithContext) string {
	t.Helper()
	id, err := cnxn.(optionConn).GetOption(context.Background(), OptionSessionID)
	if err != nil {
		t.Fatal(err)
	}
	token := rawToken(t)
	req, _ := http.NewRequest(http.MethodDelete, "http://"+shared.host+"/warp/v1/sessions/"+id, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("killing session %s: %v %v", id, resp, err)
	}
	resp.Body.Close()
	return id
}

func rawToken(t *testing.T) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"grant_type": "password", "username": testUsername, "password": shared.password})
	resp, err := http.Post("http://"+shared.host+"/warp/v1/auth/token", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var answer struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil || answer.AccessToken == "" {
		t.Fatalf("token: %v", err)
	}
	return answer.AccessToken
}

func replacements(t *testing.T, cnxn adbc.ConnectionWithContext) string {
	t.Helper()
	n, err := cnxn.(optionConn).GetOption(context.Background(), OptionSessionReplacements)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A lost session is replaced, with the connection's options, and the
// statement runs in the replacement.
func TestIntegrationLostSessionIsReplaced(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "ATTACH IF NOT EXISTS ':memory:' AS replay_cat")
	if err := cnxn.(optionConn).SetOption(context.Background(), adbc.OptionKeyCurrentCatalog, "replay_cat"); err != nil {
		t.Fatal(err)
	}
	old := killSession(t, cnxn)
	if got := scalarInt(t, cnxn, "SELECT 5"); got != 5 {
		t.Fatalf("got %d", got)
	}
	if got := replacements(t, cnxn); got != "1" {
		t.Fatalf("replacements = %s, want 1", got)
	}
	if id, _ := cnxn.(optionConn).GetOption(context.Background(), OptionSessionID); id == old {
		t.Fatal("the session id did not change")
	}
	if got, _ := cnxn.(optionConn).GetOption(context.Background(), adbc.OptionKeyCurrentCatalog); got != "replay_cat" {
		t.Fatalf("the replacement session's catalog is %q: options were not replayed", got)
	}
}

// What did not survive is said, not hidden: a temporary table is gone, and
// the error says the session was replaced.
func TestIntegrationLostSessionTemporaryStateIsReported(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE TEMPORARY TABLE scratch AS SELECT 7 AS v")
	killSession(t, cnxn)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "SELECT v FROM scratch")
	_, _, err := stmt.ExecuteQuery(context.Background())
	assertStatus(t, err, adbc.StatusInvalidArgument, "sql_error")
	if !strings.Contains(err.Error(), "replacement session") {
		t.Errorf("the error does not say the session was replaced: %v", err)
	}
}

// Inside a transaction, nothing runs on a replacement session: that would
// commit half a transaction. The connection is usable afterwards.
func TestIntegrationLostSessionInTransaction(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE TABLE lost_tx (v INTEGER)")
	if err := cnxn.(optionConn).SetOption(context.Background(), adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
		t.Fatal(err)
	}
	execUpdate(t, cnxn, "INSERT INTO lost_tx VALUES (1)")
	killSession(t, cnxn)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "INSERT INTO lost_tx VALUES (2)")
	_, err := stmt.ExecuteUpdate(context.Background())
	assertStatus(t, err, adbc.StatusIO, "session_not_found")
	if !strings.Contains(err.Error(), "open transaction") {
		t.Errorf("the error does not say the transaction was lost: %v", err)
	}
	// The replacement runs with autocommit off, like the old session.
	execUpdate(t, cnxn, "INSERT INTO lost_tx VALUES (3)")
	if err := cnxn.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := scalarInt(t, openConn(t), "SELECT sum(v)::BIGINT FROM lost_tx"); got != 3 {
		t.Fatalf("sum = %d, want 3 (only the row inserted after the loss)", got)
	}
}

// Commit of a lost transaction fails rather than pretending.
func TestIntegrationCommitAfterLostSession(t *testing.T) {
	cnxn := openConn(t)
	if err := cnxn.(optionConn).SetOption(context.Background(), adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
		t.Fatal(err)
	}
	killSession(t, cnxn)
	assertStatus(t, cnxn.Commit(context.Background()), adbc.StatusIO, "session_not_found")
}

func TestIntegrationLostSessionWithoutReconnect(t *testing.T) {
	cnxn := connect(t, openDB(t, shared, map[string]string{OptionSessionReconnect: "false"}))
	killSession(t, cnxn)
	for range 2 {
		stmt := newStmt(t, cnxn)
		setQuery(t, stmt, "SELECT 1")
		_, _, err := stmt.ExecuteQuery(context.Background())
		assertStatus(t, err, adbc.StatusIO, "session_not_found")
		if !strings.Contains(err.Error(), "must be reopened") {
			t.Errorf("error %v", err)
		}
	}
}

// A prepared statement lost with its session is prepared again.
func TestIntegrationPreparedStatementSurvivesLostSession(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "SELECT ? + 1")
	for i := range 2 {
		bindInt32(t, stmt, 41)
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			t.Fatalf("execution %d: %v", i, err)
		}
		if got := int64s(t, readAll(t, reader), 0); got[0] != 42 {
			t.Fatalf("got %d", got[0])
		}
		if i == 0 {
			killSession(t, cnxn)
		}
	}
}

// A retained result lost with its session cannot be read; the error says so.
func TestIntegrationResultLostWithSession(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	_, partitions, _, err := executePartitions(t, stmt, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	killSession(t, cnxn)
	_, err = cnxn.ReadPartition(context.Background(), partitions.PartitionIDs[0])
	assertStatus(t, err, adbc.StatusIO, "session_not_found")
}

func executePartitions(t *testing.T, stmt *statementImpl, sql string) (*arrow.Schema, adbc.Partitions, int64, error) {
	t.Helper()
	setQuery(t, stmt, sql)
	return stmt.ExecutePartitions(context.Background())
}

// Partitions can be read on another connection of the same principal, more
// than once while the result is retained.
func TestIntegrationPartitions(t *testing.T) {
	db := openDB(t, shared, nil)
	first, second := connect(t, db), connect(t, db)
	schema, partitions, rows, err := executePartitions(t, newStmt(t, first), bigQuery)
	if err != nil {
		t.Fatal(err)
	}
	if partitions.NumPartitions != 1 || schema.NumFields() != 2 || rows != 1_000_000 {
		t.Fatalf("%d partitions, schema %s, %d rows", partitions.NumPartitions, schema, rows)
	}
	for range 2 {
		reader, err := second.ReadPartition(context.Background(), partitions.PartitionIDs[0])
		if err != nil {
			t.Fatal(err)
		}
		checkBig(t, readAll(t, reader))
	}
	// A statement without rows has no partitions, only a count.
	execUpdate(t, first, "CREATE TABLE part_rows (v INTEGER)")
	_, partitions, rows, err = executePartitions(t, newStmt(t, first), "INSERT INTO part_rows VALUES (1), (2)")
	if err != nil || partitions.NumPartitions != 0 || rows != 2 {
		t.Fatalf("%d partitions, %d rows, %v", partitions.NumPartitions, rows, err)
	}
	_, err = second.ReadPartition(context.Background(), []byte("not a descriptor"))
	assertStatus(t, err, adbc.StatusInvalidArgument, "")
}

// zstd buffer compression is asked for, used, and transparent (§8.2).
func TestIntegrationZstdResults(t *testing.T) {
	proxy, cnxn := viaProxy(t, map[string]string{OptionIPCCompression: "zstd", OptionAsync: "true"})
	checkBig(t, queryTable(t, cnxn, bigQuery))
	fetches := proxy.requests(isResultFetch)
	if len(fetches) == 0 || fetches[0].Header.Get("Warp-IPC-Compression") != "zstd" {
		t.Fatal("the driver did not ask for zstd")
	}
}

func TestIntegrationQueryTag(t *testing.T) {
	proxy, cnxn := viaProxy(t, map[string]string{OptionQueryTag: `{"job":"nightly"}`})
	scalarInt(t, cnxn, "SELECT 1")
	submits := proxy.requests(isPost("/statements"))
	if len(submits) == 0 || submits[0].Header.Get("Warp-Query-Tag") != `{"job":"nightly"}` {
		t.Fatal("the query tag was not sent")
	}
	stmt := newStmt(t, cnxn)
	assertStatus(t, stmt.SetOption(context.Background(), OptionQueryTag, "not json"), adbc.StatusInvalidArgument, "")
}

// A dropped answer to a retryable read is retried: the driver survives a
// flaky network on status polls too.
func TestIntegrationRetriedStatusPoll(t *testing.T) {
	proxy, cnxn := viaProxy(t, map[string]string{OptionAsync: "true"})
	proxy.add(&faultRule{match: func(r *http.Request) bool {
		return r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/statements/") && !strings.HasSuffix(r.URL.Path, "/result")
	}, times: 1})
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM range(300000000)"); got != 300000000 {
		t.Fatalf("got %d", got)
	}
}

// The same error type survives every layer: errors.As finds the server's
// problem document through the ADBC error.
func TestIntegrationErrorDetails(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "SELEC 1")
	_, _, err := stmt.ExecuteQuery(context.Background())
	assertStatus(t, err, adbc.StatusInvalidArgument, "sql_error")
	adbcErr, _ := errors.AsType[adbc.Error](err)
	var code, requestID string
	for _, detail := range adbcErr.Details {
		value, _ := detail.Serialize()
		switch detail.Key() {
		case ErrorDetailCode:
			code = string(value)
		case ErrorDetailRequestID:
			requestID = string(value)
		}
	}
	if code != "sql_error" || !strings.HasPrefix(requestID, "r_") {
		t.Errorf("details: code %q, request id %q", code, requestID)
	}
	if p, ok := errors.AsType[*protocol.Problem](err); !ok || p.Status != http.StatusBadRequest {
		t.Errorf("problem document not reachable: %v", err)
	}
	if !strings.Contains(adbcErr.Msg, "Parser Error") {
		t.Errorf("the server's message was lost: %s", adbcErr.Msg)
	}
}
