// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/goleak"
)

// openAgainst opens a database, a connection and two statements (one with a
// result being read) on the fake server.
func openAgainst(t *testing.T, f *fakeChargewire) (adbc.DatabaseWithContext, adbc.ConnectionWithContext, []adbc.StatementWithContext) {
	t.Helper()
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: f.server.URL, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"})
	if err != nil {
		t.Fatal(err)
	}
	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var stmts []adbc.StatementWithContext
	for range 2 {
		stmt, err := cnxn.NewStatement(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stmt.SetSqlQuery(ctx, "SELECT v"); err != nil {
			t.Fatal(err)
		}
		stmts = append(stmts, stmt)
	}
	reader, _, err := stmts[0].ExecuteQuery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader.Next()
	reader.Release()
	return db, cnxn, stmts
}

// closeAll closes the statements, the connection and the database, each of
// which must succeed, and returns how long it took.
func closeAll(t *testing.T, db adbc.DatabaseWithContext, cnxn adbc.ConnectionWithContext, stmts []adbc.StatementWithContext) time.Duration {
	t.Helper()
	ctx := context.Background()
	start := time.Now()
	for i, stmt := range stmts {
		if err := stmt.Close(ctx); err != nil {
			t.Errorf("statement %d: Close: %v", i, err)
		}
	}
	if err := cnxn.Close(ctx); err != nil {
		t.Errorf("connection: Close: %v", err)
	}
	if err := db.Close(ctx); err != nil {
		t.Errorf("database: Close: %v", err)
	}
	return time.Since(start)
}

// Closing after the server stopped succeeds at once (no error for a server
// that is gone) and leaves no goroutine behind.
func TestCloseAfterTheServerStopped(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	f := newFakeChargewire(t)
	db, cnxn, stmts := openAgainst(t, f)
	f.server.CloseClientConnections()
	f.server.Close()
	if elapsed := closeAll(t, db, cnxn, stmts); elapsed > 2*time.Second {
		t.Errorf("closing took %s against a stopped server", elapsed)
	}
	goleak.VerifyNone(t, ignore)
}

// A server that takes the request and never answers holds Close up for at
// most closeTimeout.
func TestCloseAgainstAServerThatNeverAnswers(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	f := newFakeChargewire(t)
	db, cnxn, stmts := openAgainst(t, f)
	hung := make(chan struct{})
	f.hangSessionDelete(hung)
	elapsed := closeAll(t, db, cnxn, stmts)
	close(hung)
	if elapsed < closeTimeout-time.Second || elapsed > closeTimeout+2*time.Second {
		t.Errorf("closing took %s, want about closeTimeout (%s)", elapsed, closeTimeout)
	}
	f.server.Close()
	goleak.VerifyNone(t, ignore)
}

// The twin: with the server up, Close ends the session on it.
func TestCloseEndsTheSessionOnARunningServer(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	f := newFakeChargewire(t)
	deleted := f.countSessionDeletes()
	db, cnxn, stmts := openAgainst(t, f)
	closeAll(t, db, cnxn, stmts)
	if got := deleted(); got != 1 {
		t.Errorf("%d session deletes, want 1", got)
	}
	f.server.Close()
	goleak.VerifyNone(t, ignore)
}

// hangSessionDelete makes DELETE of a session wait until release is closed or
// the client gives up.
func (f *fakeChargewire) hangSessionDelete(release <-chan struct{}) {
	f.setSessionDelete(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
}

// countSessionDeletes counts DELETEs of a session.
func (f *fakeChargewire) countSessionDeletes() func() int {
	count := make(chan int, 1)
	count <- 0
	f.setSessionDelete(func(w http.ResponseWriter, r *http.Request) {
		count <- <-count + 1
		w.WriteHeader(http.StatusNoContent)
	})
	return func() int { n := <-count; count <- n; return n }
}
