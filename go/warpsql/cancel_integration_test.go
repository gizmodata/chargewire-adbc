// Licensed under the Apache License, Version 2.0.

//go:build integration

// Cancellation, ported from gizmosql-adbc's cancel tests. The oracle is the
// session itself: a session runs one statement at a time, so a statement the
// server did not stop holds up the next one (see TestIntegrationCancelOracle).

package warpsql

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// longQuery runs until something stops it, and produces nothing until it
// ends; streamingQuery streams batches until something stops it.
const (
	longQuery      = "SELECT sum(a.range * b.range) FROM range(100000000) a, range(100000) b"
	streamingQuery = "SELECT i FROM range(100000000000) t(i)"
)

// runQueryInBackground starts ExecuteQuery and reads the whole result.
func runQueryInBackground(stmt *statementImpl) <-chan error {
	done := make(chan error, 1)
	go func() {
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err == nil {
			for reader.Next() {
			}
			err = reader.Err()
			reader.Release()
		}
		done <- err
	}()
	return done
}

func runUpdateInBackground(stmt *statementImpl) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := stmt.ExecuteUpdate(context.Background())
		done <- err
	}()
	return done
}

func waitErr(t *testing.T, done <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(within):
		t.Fatalf("the statement did not end within %v", within)
		return nil
	}
}

// waitRunning waits until the statement has an id on the server.
func waitRunning(t *testing.T, stmt *statementImpl) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		stmt.mu.Lock()
		exec := stmt.active
		stmt.mu.Unlock()
		if exec != nil && exec.sub != nil && exec.sub.Sent() {
			time.Sleep(time.Second) // let the server get going
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the statement never started")
}

func setQuery(t *testing.T, stmt *statementImpl, sql string) {
	t.Helper()
	if err := stmt.SetSqlQuery(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}

// The oracle works: while a statement runs, the session answers nothing else.
func TestIntegrationCancelOracle(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, longQuery)
	done := runQueryInBackground(stmt)
	waitRunning(t, stmt)

	blocked := make(chan struct{})
	go func() {
		scalarInt(t, cnxn, "SELECT 42")
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("SELECT 42 ran while the long query was running: the session-is-free oracle proves nothing")
	case <-time.After(2 * time.Second):
	}
	if err := stmt.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitErr(t, done, 30*time.Second)
	select {
	case <-blocked:
	case <-time.After(30 * time.Second):
		t.Fatal("SELECT 42 never ran after the cancel")
	}
}

// Cancel from another goroutine while the result streams: what the Python
// driver manager does on Ctrl+C, and cursor.adbc_cancel().
func TestIntegrationCancelQueryInterruptsRunningStatement(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, longQuery)
	start := time.Now()
	done := runQueryInBackground(stmt)
	waitRunning(t, stmt)
	if err := stmt.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	err := waitErr(t, done, 30*time.Second)
	assertStatus(t, err, adbc.StatusCancelled, "")
	t.Logf("interrupted after %v: %v", time.Since(start), err)
	assertSessionUsable(t, cnxn)
}

// Cancel reaches an ExecuteUpdate whose answer the server is still holding
// (Prefer: wait), whose id the driver does not know: alone on its connection,
// through the session cancel (§6.8).
func TestIntegrationCancelUpdateWithUnknownStatementID(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "CREATE OR REPLACE TABLE cancelled_update AS "+longQuery)
	done := runUpdateInBackground(stmt)
	waitRunning(t, stmt)
	if _, known := stmt.active.statementRef(); known {
		t.Fatal("the statement id is already known; this test needs a held answer")
	}
	if err := stmt.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	assertStatus(t, waitErr(t, done, 30*time.Second), adbc.StatusCancelled, "")
	sessionCancels := proxy.requests(func(r *http.Request) bool {
		return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel") && !strings.Contains(r.URL.Path, "/statements/")
	})
	if submits := proxy.requests(isPost("/statements")); len(sessionCancels) != 1 || len(submits) != 1 {
		t.Fatalf("%d session cancels and %d submits; want 1 and 1 (no resend)", len(sessionCancels), len(submits))
	}
	assertSessionUsable(t, cnxn)
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'cancelled_update'"); got != 0 {
		t.Fatalf("the cancelled CREATE TABLE AS left %d table(s) behind", got)
	}
}

// Beside another statement of the same connection, the session cancel would
// stop that one too: the driver names its own statement by sending its
// request again with its Idempotency-Key, and only it is cancelled.
func TestIntegrationCancelLeavesOtherStatementsAlone(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE cancel_bystander (v INTEGER)")
	long := newStmt(t, cnxn)
	setQuery(t, long, "CREATE OR REPLACE TABLE cancel_target AS "+longQuery)
	done := runUpdateInBackground(long)
	waitRunning(t, long)
	bystander := newStmt(t, cnxn)
	setQuery(t, bystander, "INSERT INTO cancel_bystander VALUES (1)")
	queued := runUpdateInBackground(bystander) // waits behind the long statement
	waitRunning(t, bystander)
	if err := long.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	assertStatus(t, waitErr(t, done, 30*time.Second), adbc.StatusCancelled, "")
	if err := waitErr(t, queued, 30*time.Second); err != nil {
		t.Fatalf("the other statement was cancelled too: %v", err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM cancel_bystander"); got != 1 {
		t.Fatalf("the other statement's row is missing: %d", got)
	}
}

func TestIntegrationCancelAsyncQuery(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetOption(context.Background(), OptionAsync, "true"); err != nil {
		t.Fatal(err)
	}
	setQuery(t, stmt, longQuery)
	done := runQueryInBackground(stmt)
	waitRunning(t, stmt)
	if err := stmt.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertStatus(t, waitErr(t, done, 30*time.Second), adbc.StatusCancelled, "")
	assertSessionUsable(t, cnxn)
}

// Closing the statement while its result streams stops it on the server.
func TestIntegrationCloseWhileExecutingCancelsOnServer(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, streamingQuery)
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	time.Sleep(time.Second)
	if err := stmt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for reader.Next() {
	}
	if reader.Err() == nil {
		t.Fatal("expected the abandoned query to fail")
	}
	reader.Release()
	assertSessionUsable(t, cnxn)
}

// Releasing a result before reading it (ADBC 1.1: equivalent to cancel) stops
// the query on the server.
func TestIntegrationAbandonedResultCancelsRunningQuery(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, streamingQuery)
	got := make(chan array.RecordReader, 1)
	go func() {
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			t.Error(err)
		}
		got <- reader
	}()
	var reader array.RecordReader
	select {
	case reader = <-got:
	case <-time.After(20 * time.Second):
		t.Fatal("ExecuteQuery blocked; the streamed path answers with the first batch, long before this query ends")
	}
	if reader == nil {
		t.FailNow()
	}
	time.Sleep(time.Second)
	reader.Release() // walk away without reading
	assertSessionUsable(t, cnxn)
}

// Closing a statement while ExecuteUpdate waits stops the update on the
// server and leaves no table behind.
func TestIntegrationCloseWhileUpdateExecutingCancelsOnServer(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "CREATE OR REPLACE TABLE cancelled_ctas AS "+longQuery)
	done := runUpdateInBackground(stmt)
	waitRunning(t, stmt)
	if err := stmt.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if waitErr(t, done, 10*time.Second) == nil {
		t.Fatal("expected the interrupted update to fail")
	}
	assertSessionUsable(t, cnxn)
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'cancelled_ctas'"); got != 0 {
		t.Fatalf("cancelled CTAS left %d table(s) behind", got)
	}
}

// A context that expires stops the statement on the server too.
func TestIntegrationContextDeadlineStopsStatement(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, longQuery)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := stmt.ExecuteUpdate(ctx)
	assertStatus(t, err, adbc.StatusTimeout, "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error does not wrap context.DeadlineExceeded: %v", err)
	}
	assertSessionUsable(t, cnxn)
}

// The server cancels a statement that outlives its timeout (§6.9).
func TestIntegrationStatementTimeout(t *testing.T) {
	cnxn := openConn(t)
	for _, async := range []string{"false", "true"} {
		stmt := newStmt(t, cnxn)
		for key, value := range map[string]string{OptionStatementTimeout: "0.5", OptionAsync: async} {
			if err := stmt.SetOption(context.Background(), key, value); err != nil {
				t.Fatal(err)
			}
		}
		setQuery(t, stmt, longQuery)
		_, err := stmt.ExecuteUpdate(context.Background())
		assertStatus(t, err, adbc.StatusTimeout, "statement_timeout")
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err == nil {
			for reader.Next() {
			}
			err = reader.Err()
			reader.Release()
		}
		assertStatus(t, err, adbc.StatusTimeout, "statement_timeout")
	}
	assertSessionUsable(t, cnxn)
}

func TestIntegrationCancelWithNothingRunning(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	assertStatus(t, stmt.Cancel(context.Background()), adbc.StatusInvalidState, "")
	// A result read to the end is over: nothing left to cancel.
	setQuery(t, stmt, "SELECT * FROM range(1000)")
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, reader)
	assertStatus(t, stmt.Cancel(context.Background()), adbc.StatusInvalidState, "")
}
