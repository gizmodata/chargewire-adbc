// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// fakeWarp is an in-process Warp SQL server with one query result: batch b
// of partition p holds the rows p*1000+b*10 … +9. It can cut streams, keep
// results or not, and put partitions elsewhere, for tests of the driver's
// download logic that need no real server.
type fakeWarp struct {
	t      *testing.T
	server *httptest.Server

	batches      int      // per partition
	partitions   int      // partitions per result
	order        string   // the document's partition_order when partitions > 1
	remote       string   // base URL serving partitions > 0, if set
	capabilities []string // listed in discovery
	retained     bool
	tokenTTL     int64
	// streamSplit, when set, are the split headers of a streamed answer
	// (Warp-Result-Partitions, Warp-Result-Order).
	streamSplit map[string]string

	cutFirstAfter atomic.Int64 // >0: cut the first stream after that many batches
	tokens        atomic.Int64
	fromBatch     []int64 // from_batch of every result request
	mu            sync.Mutex
	// sessionDelete, when set, answers DELETE of a session.
	sessionDelete atomic.Pointer[http.HandlerFunc]
	// submit, when set, answers a submit.
	submit atomic.Pointer[http.HandlerFunc]
	// submits are the headers of every submit; deleted the statements
	// deleted.
	submits []http.Header
	deleted []string
}

// setSubmit makes handler answer submits.
func (f *fakeWarp) setSubmit(handler http.HandlerFunc) { f.submit.Store(&handler) }

// deletedStatements waits up to a second for the background deletes and
// returns the statements deleted.
func (f *fakeWarp) deletedStatements(want int) []string {
	deadline := time.Now().Add(time.Second)
	for {
		f.mu.Lock()
		deleted := append([]string(nil), f.deleted...)
		f.mu.Unlock()
		if len(deleted) >= want || time.Now().After(deadline) {
			return deleted
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *fakeWarp) lastSubmit() http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.submits) == 0 {
		return nil
	}
	return f.submits[len(f.submits)-1]
}

// setSessionDelete makes handler answer DELETE of a session.
func (f *fakeWarp) setSessionDelete(handler http.HandlerFunc) { f.sessionDelete.Store(&handler) }

var fakeSchema = arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)

func newFakeWarp(t *testing.T) *fakeWarp {
	f := &fakeWarp{t: t, batches: 5, partitions: 1, retained: true, tokenTTL: 3600, capabilities: []string{"long_poll"}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /warp", func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusOK, map[string]any{"protocol": "warp-sql", "protocol_versions": []string{"1"},
			"server":       map[string]string{"name": "fake-warp", "version": "9.9"},
			"capabilities": f.capabilities, "auth": map[string]any{"grant_types": []string{"password"}}})
	})
	mux.HandleFunc("POST /warp/v1/auth/token", func(w http.ResponseWriter, r *http.Request) {
		n := f.tokens.Add(1)
		fakeJSON(w, http.StatusOK, map[string]any{"access_token": fmt.Sprintf("tok%d", n), "token_type": "Bearer", "expires_in": f.tokenTTL})
	})
	mux.HandleFunc("POST /warp/v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusCreated, map[string]any{"session_id": "s1", "options": map[string]any{}})
	})
	mux.HandleFunc("DELETE /warp/v1/sessions/{s}", func(w http.ResponseWriter, r *http.Request) {
		if handler := f.sessionDelete.Load(); handler != nil {
			(*handler)(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /warp/v1/sessions/{s}/statements/{q}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.deleted = append(f.deleted, r.PathValue("q"))
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /warp/v1/sessions/{s}/statements", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.submits = append(f.submits, r.Header.Clone())
		f.mu.Unlock()
		if handler := f.submit.Load(); handler != nil {
			(*handler)(w, r)
			return
		}
		if r.Header.Get("Accept") == "application/vnd.apache.arrow.stream" {
			w.Header().Set("Warp-Statement-Id", "q1")
			for key, value := range f.streamSplit {
				w.Header().Set(key, value)
			}
			f.stream(w, 0, 0)
			return
		}
		fakeJSON(w, http.StatusCreated, f.document())
	})
	mux.HandleFunc("GET /warp/v1/sessions/{s}/statements/{q}", func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusOK, f.document())
	})
	mux.HandleFunc("GET /warp/v1/sessions/{s}/statements/{q}/result", f.result)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeWarp) result(w http.ResponseWriter, r *http.Request) {
	from, _ := strconv.ParseInt(r.URL.Query().Get("from_batch"), 10, 64)
	partition, _ := strconv.Atoi(r.URL.Query().Get("partition"))
	f.mu.Lock()
	f.fromBatch = append(f.fromBatch, from)
	f.mu.Unlock()
	if !f.retained && from > 0 {
		fakeProblem(w, http.StatusConflict, "result_not_resumable")
		return
	}
	f.stream(w, partition, int(from))
}

func (f *fakeWarp) document() map[string]any {
	var partitions []map[string]any
	for p := range f.partitions {
		partition := map[string]any{"index": p, "location": nil, "batches": f.batches, "rows": f.batches * 10}
		if p > 0 && f.remote != "" {
			partition["location"] = f.remote
		}
		partitions = append(partitions, partition)
	}
	var schema bytesBuffer
	writer := ipc.NewWriter(&schema, ipc.WithSchema(fakeSchema))
	_ = writer.Close()
	result := map[string]any{
		"kind": "rows", "schema": base64.StdEncoding.EncodeToString(schema.data), "partitions": partitions, "retained": f.retained,
	}
	if f.partitions > 1 && f.order != "" {
		result["partition_order"] = f.order
	}
	return map[string]any{"statement_id": "q1", "state": "succeeded", "result": result}
}

type bytesBuffer struct{ data []byte }

func (b *bytesBuffer) Write(p []byte) (int, error) { b.data = append(b.data, p...); return len(p), nil }

// stream writes a partition's batches from batch from; the first stream may
// be cut, without its end-of-stream marker, by aborting the connection.
func (f *fakeWarp) stream(w http.ResponseWriter, partition, from int) {
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	writer := ipc.NewWriter(w, ipc.WithSchema(fakeSchema))
	cut := f.cutFirstAfter.Swap(0)
	for b := from; b < f.batches; b++ {
		if cut > 0 && int64(b-from) == cut {
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		builder := array.NewRecordBuilder(memory.DefaultAllocator, fakeSchema)
		for i := range 10 {
			builder.Field(0).(*array.Int64Builder).Append(int64(partition*1000 + b*10 + i))
		}
		rec := builder.NewRecordBatch()
		_ = writer.Write(rec)
		rec.Release()
		builder.Release()
	}
	_ = writer.Close()
}

func fakeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fakeProblem(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "code": code, "detail": "fake " + code})
}

func (f *fakeWarp) connect(t *testing.T, extra map[string]string) adbc.ConnectionWithContext {
	t.Helper()
	options := map[string]string{adbc.OptionKeyURI: f.server.URL, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"}
	for key, value := range extra {
		options[key] = value
	}
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cnxn.Close(context.Background()) })
	return cnxn
}

// query runs the fake's query and returns its values.
func (f *fakeWarp) query(t *testing.T, cnxn adbc.ConnectionWithContext) ([]int64, error) {
	t.Helper()
	stmt, _ := cnxn.NewStatement(context.Background())
	defer stmt.Close(context.Background())
	_ = stmt.SetSqlQuery(context.Background(), "SELECT v")
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	var values []int64
	for reader.Next() {
		values = append(values, reader.RecordBatch().Column(0).(*array.Int64).Int64Values()...)
	}
	return values, reader.Err()
}

func wantSequence(t *testing.T, values []int64, partitions, batches int) {
	t.Helper()
	var want []int64
	for p := range partitions {
		for i := range batches * 10 {
			want = append(want, int64(p*1000+i))
		}
	}
	if fmt.Sprint(values) != fmt.Sprint(want) {
		t.Fatalf("values\n%v\nwant\n%v", values, want)
	}
}

// A cut streamed download of a retained result resumes at the next batch.
func TestFakeResumeStreamedResult(t *testing.T) {
	f := newFakeWarp(t)
	f.cutFirstAfter.Store(2)
	values, err := f.query(t, f.connect(t, map[string]string{OptionRetainResult: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	wantSequence(t, values, 1, f.batches)
	if fmt.Sprint(f.fromBatch) != "[2]" {
		t.Fatalf("result requests from batches %v, want [2]", f.fromBatch)
	}
}

func TestFakeUnretainedCutFails(t *testing.T) {
	f := newFakeWarp(t)
	f.retained = false
	f.cutFirstAfter.Store(2)
	_, err := f.query(t, f.connect(t, nil))
	if adbcErr, ok := errors.AsType[adbc.Error](err); !ok || adbcErr.Code != adbc.StatusIO {
		t.Fatalf("a cut unretained stream gave %v", err)
	}
}

// A server with result_locations is told the driver follows locations, and
// partitions on other servers are read from there, all at once (§6.2, §6.10).
func TestFakePartitionsAcrossServers(t *testing.T) {
	remote := newFakeWarp(t)
	f := newFakeWarp(t)
	f.capabilities = append(f.capabilities, "result_locations")
	f.partitions = 3
	f.order = "any"
	f.remote = remote.server.URL + "/warp"
	values, err := f.query(t, f.connect(t, map[string]string{OptionAsync: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(values)
	wantSequence(t, values, 3, f.batches)
	if len(remote.fromBatch) != 2 || len(f.fromBatch) != 1 {
		t.Fatalf("this server served %d partitions and the remote %d; want 1 and 2", len(f.fromBatch), len(remote.fromBatch))
	}
	if got := f.lastSubmit().Get("Warp-Result-Locations"); got != "remote" {
		t.Fatalf("Warp-Result-Locations %q, want remote", got)
	}
}

// On the streamed path the answer is partition 0; a client that sent
// Warp-Result-Locations finds partitions 1 to k-1 in the statement document,
// here on another server (§6.2).
func TestFakeStreamedPartitionsAcrossServers(t *testing.T) {
	remote := newFakeWarp(t)
	f := newFakeWarp(t)
	f.capabilities = append(f.capabilities, "result_partitions", "result_locations")
	f.partitions = 3
	f.order = "any"
	f.remote = remote.server.URL + "/warp"
	f.streamSplit = map[string]string{"Warp-Result-Partitions": "3", "Warp-Result-Order": "any"}
	values, err := f.query(t, f.connect(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(values)
	wantSequence(t, values, 3, f.batches)
	if len(remote.fromBatch) != 2 || len(f.fromBatch) != 0 {
		t.Fatalf("the remote served %d partitions and this server %d result requests; want 2 and 0", len(remote.fromBatch), len(f.fromBatch))
	}
}

// Without result_locations the driver never sends Warp-Result-Locations, and
// a location it did not ask for is a protocol error: the access token is not
// sent to a server nobody named (§6.2).
func TestFakeRemoteLocationNotAskedFor(t *testing.T) {
	remote := newFakeWarp(t)
	f := newFakeWarp(t)
	f.partitions = 2
	f.order = "any"
	f.remote = remote.server.URL + "/warp"
	_, err := f.query(t, f.connect(t, map[string]string{OptionAsync: "true"}))
	assertFakeError(t, err, adbc.StatusInternal, "client_protocol_error")
	if got := f.lastSubmit().Values("Warp-Result-Locations"); len(got) != 0 {
		t.Fatalf("Warp-Result-Locations %v sent to a server without result_locations", got)
	}
	if len(remote.fromBatch) != 0 {
		t.Fatal("the unasked-for location was read")
	}
}

// A partition location must not drop the server's TLS (the access token goes
// along).
func TestPartitionLocationKeepsTLS(t *testing.T) {
	base, _ := url.Parse("https://db.example.com/warp")
	plain, secure := "http://worker.example.com/warp", "https://worker.example.com/warp"
	if _, err := newResultPart(1, &plain, base); !protocol.HasCode(err, protocol.CodeClientProtocolError) {
		t.Errorf("an http location of an https server gave %v", err)
	}
	if part, err := newResultPart(1, &secure, base); err != nil || part.location.String() != secure {
		t.Errorf("an https location: %+v, %v", part, err)
	}
}

// A streamed result split into partitions must say how in both headers: a
// missing or unknown Warp-Result-Order is a protocol error, never "any", and
// the statement is still freed (§6.10).
func TestFakeSplitStreamNeedsItsOrder(t *testing.T) {
	for _, order := range []string{"", "sideways"} {
		f := newFakeWarp(t)
		f.capabilities = append(f.capabilities, "result_partitions")
		f.streamSplit = map[string]string{"Warp-Result-Partitions": "3"}
		if order != "" {
			f.streamSplit["Warp-Result-Order"] = order
		}
		_, err := f.query(t, f.connect(t, nil))
		assertFakeError(t, err, adbc.StatusInternal, "client_protocol_error")
		if deleted := f.deletedStatements(1); len(deleted) != 1 || deleted[0] != "q1" {
			t.Errorf("order %q: statements deleted %v, want [q1]", order, deleted)
		}
	}
	// With the order the partitions are read: 0 from the answer, 1 and 2 at once.
	f := newFakeWarp(t)
	f.capabilities = append(f.capabilities, "result_partitions")
	f.partitions = 3
	f.streamSplit = map[string]string{"Warp-Result-Partitions": "3", "Warp-Result-Order": "any"}
	values, err := f.query(t, f.connect(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(values)
	wantSequence(t, values, 3, f.batches)
}

// So must a statement document's result of several partitions (§6.10).
func TestFakeSplitDocumentNeedsItsOrder(t *testing.T) {
	f := newFakeWarp(t)
	f.partitions = 2
	_, err := f.query(t, f.connect(t, map[string]string{OptionAsync: "true"}))
	assertFakeError(t, err, adbc.StatusInternal, "client_protocol_error")
}

// A statement cancelled by the time the server answers its submit is an
// error response with its cancel_reason and Warp-Statement-Id (§6.1), not a
// success status; the driver reports it as cancelled and frees the statement.
func TestFakeCancelledAtSubmit(t *testing.T) {
	f := newFakeWarp(t)
	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Warp-Statement-Id", "q9")
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 409, "code": "statement_cancelled",
			"title": "Statement cancelled", "detail": "cancelled by an administrator", "cancel_reason": "killed"})
	})
	cnxn := f.connect(t, nil)
	for _, async := range []string{"false", "true"} {
		stmt, _ := cnxn.NewStatement(context.Background())
		_ = stmt.SetOption(context.Background(), OptionAsync, async)
		_ = stmt.SetSqlQuery(context.Background(), "SELECT v")
		_, _, err := stmt.ExecuteQuery(context.Background())
		assertFakeError(t, err, adbc.StatusCancelled, "statement_cancelled")
		if detail := errorDetail(err, ErrorDetailCancelReason); detail != "killed" {
			t.Errorf("async=%s: cancel reason %q, want killed", async, detail)
		}
		_ = stmt.Close(context.Background())
	}
	if deleted := f.deletedStatements(2); len(deleted) != 2 || deleted[0] != "q9" {
		t.Errorf("statements deleted %v, want q9 twice", deleted)
	}
}

// A failed document is the statement's error whatever the status: an
// idempotent replay answers 200 with it (§6.2, §6.7). An execution error is
// 422, a 4xx, not a server fault (§11).
func TestFakeFailedDocumentAndExecutionError(t *testing.T) {
	f := newFakeWarp(t)
	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusOK, map[string]any{"statement_id": "q2", "state": "failed",
			"error": map[string]any{"code": "execution_error", "status": 422, "detail": "division by zero"}})
	})
	cnxn := f.connect(t, nil)
	stmt, _ := cnxn.NewStatement(context.Background())
	defer stmt.Close(context.Background())
	_ = stmt.SetSqlQuery(context.Background(), "UPDATE t SET v = 1/0")
	_, err := stmt.ExecuteUpdate(context.Background())
	assertFakeError(t, err, adbc.StatusInvalidArgument, "execution_error")

	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Warp-Statement-Id", "q3")
		fakeProblem(w, http.StatusUnprocessableEntity, "execution_error")
	})
	_, err = stmt.ExecuteUpdate(context.Background())
	assertFakeError(t, err, adbc.StatusInvalidArgument, "execution_error")
	if deleted := f.deletedStatements(2); !slices.Contains(deleted, "q3") {
		t.Errorf("statements deleted %v, want q3 among them", deleted)
	}
}

// The query tag goes out compact and in ASCII, and reads back so (§2.5).
func TestFakeQueryTagIsSentInASCII(t *testing.T) {
	f := newFakeWarp(t)
	cnxn := f.connect(t, map[string]string{OptionQueryTag: "{\"job\": \"Zo\u00eb\u2019s load\"}"})
	if _, err := f.query(t, cnxn); err != nil {
		t.Fatal(err)
	}
	want := `{"job":"Zo\u00eb\u2019s load"}`
	if got := f.lastSubmit().Get("Warp-Query-Tag"); got != want {
		t.Fatalf("Warp-Query-Tag %q, want %q", got, want)
	}
	if got, _ := cnxn.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), OptionQueryTag); got != want {
		t.Fatalf("option reads back %q", got)
	}
	for _, bad := range []string{"null", "[1]", `{"p":"` + strings.Repeat("x", protocol.MaxQueryTagBytes) + `"}`} {
		if err := cnxn.(adbc.GetSetOptionsWithContext).SetOption(context.Background(), OptionQueryTag, bad); err == nil {
			t.Errorf("query tag %.40q accepted", bad)
		}
	}
}

// adbc.warp.session.transaction is the server's view of the session's
// transaction, "unknown" for a value the driver doesn't know (§5.2, §12).
func TestFakeSessionTransaction(t *testing.T) {
	f := newFakeWarp(t)
	var transaction atomic.Value
	transaction.Store("open")
	f.server.Config.Handler.(*http.ServeMux).HandleFunc("GET /warp/v1/sessions/{s}", func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusOK, map[string]any{"session_id": "s1", "options": map[string]any{}, "transaction": transaction.Load()})
	})
	cnxn := f.connect(t, nil).(adbc.GetSetOptionsWithContext)
	for _, value := range []string{"open", "aborted", "none", "suspended"} {
		transaction.Store(value)
		want := value
		if value == "suspended" {
			want = "unknown"
		}
		if got, err := cnxn.GetOption(context.Background(), OptionSessionTransaction); err != nil || got != want {
			t.Errorf("transaction %q: option %q, %v; want %q", value, got, err, want)
		}
	}
	if err := cnxn.SetOption(context.Background(), OptionSessionTransaction, "none"); err == nil {
		t.Error("the transaction option is writable")
	}
}

// assertFakeError checks an error's ADBC status and Warp SQL code.
func assertFakeError(t *testing.T, err error, status adbc.Status, code string) {
	t.Helper()
	adbcErr, ok := errors.AsType[adbc.Error](err)
	if !ok || adbcErr.Code != status || ErrorCode(err) != code {
		t.Fatalf("error %v (status %v, code %q); want %v, %q", err, adbcErr.Code, ErrorCode(err), status, code)
	}
}

// errorDetail is a text detail of an ADBC error.
func errorDetail(err error, name string) string {
	adbcErr, _ := errors.AsType[adbc.Error](err)
	for _, detail := range adbcErr.Details {
		if text, ok := detail.(*adbc.TextErrorDetail); ok && text.Name == name {
			return text.Detail
		}
	}
	return ""
}

// A short-lived token is renewed before it is sent again.
func TestFakeTokenRenewal(t *testing.T) {
	f := newFakeWarp(t)
	f.tokenTTL = 1
	cnxn := f.connect(t, nil)
	time.Sleep(1100 * time.Millisecond)
	if _, err := f.query(t, cnxn); err != nil {
		t.Fatal(err)
	}
	if got := f.tokens.Load(); got < 2 {
		t.Fatalf("%d tokens issued, want a renewal", got)
	}
}
