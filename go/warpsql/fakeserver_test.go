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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// fakeWarp is an in-process Warp SQL server with one query result: batch b
// of partition p holds the rows p*1000+b*10 … +9. It can cut streams, keep
// results or not, and put partitions elsewhere, for tests of the driver's
// download logic that need no real server.
type fakeWarp struct {
	t      *testing.T
	server *httptest.Server

	batches    int    // per partition
	partitions int    // partitions per result
	remote     string // base URL serving partitions > 0, if set
	retained   bool
	tokenTTL   int64

	cutFirstAfter atomic.Int64 // >0: cut the first stream after that many batches
	tokens        atomic.Int64
	fromBatch     []int64 // from_batch of every result request
	mu            sync.Mutex
}

var fakeSchema = arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)

func newFakeWarp(t *testing.T) *fakeWarp {
	f := &fakeWarp{t: t, batches: 5, partitions: 1, retained: true, tokenTTL: 3600}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /warp", func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusOK, map[string]any{"protocol": "warp-sql", "protocol_versions": []string{"1"},
			"server":       map[string]string{"name": "fake-warp", "version": "9.9"},
			"capabilities": []string{"result_retention", "long_poll"}, "auth": map[string]any{"grant_types": []string{"password"}}})
	})
	mux.HandleFunc("POST /warp/v1/auth/token", func(w http.ResponseWriter, r *http.Request) {
		n := f.tokens.Add(1)
		fakeJSON(w, http.StatusOK, map[string]any{"access_token": fmt.Sprintf("tok%d", n), "token_type": "Bearer", "expires_in": f.tokenTTL})
	})
	mux.HandleFunc("POST /warp/v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusCreated, map[string]any{"session_id": "s1", "options": map[string]any{}})
	})
	mux.HandleFunc("DELETE /warp/v1/sessions/{s}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("DELETE /warp/v1/sessions/{s}/statements/{q}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /warp/v1/sessions/{s}/statements", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") == "application/vnd.apache.arrow.stream" {
			w.Header().Set("Warp-Statement-Id", "q1")
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
	return map[string]any{"statement_id": "q1", "state": "succeeded", "result": map[string]any{
		"kind": "rows", "schema": base64.StdEncoding.EncodeToString(schema.data), "partitions": partitions, "retained": f.retained,
	}}
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

// Partitions are read in order, each from its own server (§6.2).
func TestFakePartitionsAcrossServers(t *testing.T) {
	remote := newFakeWarp(t)
	f := newFakeWarp(t)
	f.partitions = 3
	f.remote = remote.server.URL + "/warp"
	values, err := f.query(t, f.connect(t, map[string]string{OptionAsync: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	wantSequence(t, values, 3, f.batches)
	if len(remote.fromBatch) != 2 || len(f.fromBatch) != 1 {
		t.Fatalf("this server served %d partitions and the remote %d; want 1 and 2", len(f.fromBatch), len(remote.fromBatch))
	}
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
