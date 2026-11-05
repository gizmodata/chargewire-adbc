// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
)

// resumingDetail is the message the fake's server_resuming answers carry.
const resumingDetail = "the server is resuming; retry after 1 s"

// writeResuming answers server_resuming with Retry-After: 1.
func writeResuming(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "urn:chargewire:error:server_resuming", "status": 503,
		"code": "server_resuming", "detail": resumingDetail})
}

// resumingFor makes a handler that answers server_resuming the first n calls
// (n < 0: always) and counts the calls.
func resumingFor(n int64) (func(http.ResponseWriter, *http.Request) bool, *atomic.Int64) {
	calls := &atomic.Int64{}
	return func(w http.ResponseWriter, _ *http.Request) bool {
		if call := calls.Add(1); n < 0 || call <= n {
			writeResuming(w)
			return true
		}
		return false
	}, calls
}

func openFakeDatabase(t *testing.T, f *fakeChargewire, extra map[string]string) adbc.DatabaseWithContext {
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
	return db
}

// Through the driver, with its defaults: the sign-in that wakes a stopped
// server and a statement on it are each sent again until the server has
// resumed. retry.max_attempts=1 shows the repeats are not the bounded retry.
func TestFakeResumingServerSignInAndStatement(t *testing.T) {
	f := newFakeChargewire(t)
	signIn, signIns := resumingFor(2)
	f.setToken(signIn)
	db := openFakeDatabase(t, f, map[string]string{OptionRetryMaxAttempts: "1"})
	if got, _ := db.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), OptionResumeTimeout); got != "600" {
		t.Fatalf("default %s %q, want 600", OptionResumeTimeout, got)
	}
	start := time.Now()
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Open through a resume: %v", err)
	}
	t.Cleanup(func() { _ = cnxn.Close(context.Background()) })
	if signIns.Load() != 3 || time.Since(start) < 2*time.Second {
		t.Fatalf("%d sign-ins in %v; want 3, a Retry-After (1 s) apart", signIns.Load(), time.Since(start))
	}

	statement, statements := resumingFor(2)
	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		if statement(w, r) {
			return
		}
		f.submit.Store(nil) // from now on the fake answers as usual
		w.Header().Set("Chargewire-Statement-Id", "q1")
		f.stream(w, 0, 0)
	})
	values, err := f.query(t, cnxn)
	if err != nil {
		t.Fatalf("query through a resume: %v", err)
	}
	if statements.Load() != 3 || len(values) != f.batches*10 {
		t.Fatalf("%d submits, %d values; want 3 submits and the whole result", statements.Load(), len(values))
	}
}

// When the server is still resuming at the deadline, Open fails with
// server_resuming (StatusIO) and the server's message.
func TestFakeResumeTimeoutReturnsTheServersError(t *testing.T) {
	f := newFakeChargewire(t)
	signIn, signIns := resumingFor(-1)
	f.setToken(signIn)
	db := openFakeDatabase(t, f, map[string]string{OptionResumeTimeout: "1.5"})
	start := time.Now()
	_, err := db.Open(context.Background())
	assertFakeError(t, err, adbc.StatusIO, protocol.CodeServerResuming)
	if !strings.Contains(err.Error(), resumingDetail) || !strings.Contains(err.Error(), "still resuming after 1.5s") {
		t.Fatalf("error %v: want the server's message and the timeout", err)
	}
	// Attempts at 0 and 1 s; the next, at 2 s, would be after 1.5 s.
	if signIns.Load() != 2 || time.Since(start) > 10*time.Second {
		t.Fatalf("%d sign-ins in %v, want 2", signIns.Load(), time.Since(start))
	}
}

// resume_timeout 0: the first server_resuming is the error, with no repeat.
func TestFakeResumeTimeoutZeroReturnsAtOnce(t *testing.T) {
	f := newFakeChargewire(t)
	signIn, signIns := resumingFor(-1)
	f.setToken(signIn)
	db := openFakeDatabase(t, f, map[string]string{OptionResumeTimeout: "0"})
	start := time.Now()
	_, err := db.Open(context.Background())
	assertFakeError(t, err, adbc.StatusIO, protocol.CodeServerResuming)
	if !strings.Contains(err.Error(), resumingDetail) {
		t.Fatalf("error %v: want the server's message", err)
	}
	if signIns.Load() != 1 || time.Since(start) > 900*time.Millisecond {
		t.Fatalf("%d sign-ins in %v, want 1 at once", signIns.Load(), time.Since(start))
	}
}

// A server (or a holder standing in for one) may hold the sign-in for up to
// 25 s before it answers: the driver's own HTTP client must not give up on
// it. Real time, through the driver's real transport.
func TestFakeHeldSignInSucceeds(t *testing.T) {
	f := newFakeChargewire(t)
	const hold = 25 * time.Second
	f.setToken(func(w http.ResponseWriter, r *http.Request) bool {
		select {
		case <-time.After(hold):
		case <-r.Context().Done():
		}
		return false
	})
	db := openFakeDatabase(t, f, nil)
	start := time.Now()
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Open with a sign-in held %v: %v (after %v)", hold, err, time.Since(start))
	}
	_ = cnxn.Close(context.Background())
	if elapsed := time.Since(start); elapsed < hold {
		t.Fatalf("Open took %v, under the hold", elapsed)
	}
}

func TestResumeTimeoutOption(t *testing.T) {
	ctx := context.Background()
	for _, bad := range []string{"-1", "soon", "NaN", "+Inf", "1e30", ""} {
		_, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx,
			map[string]string{OptionResumeTimeout: bad})
		if adbcErr, ok := err.(adbc.Error); !ok || adbcErr.Code != adbc.StatusInvalidArgument ||
			!strings.Contains(adbcErr.Msg, OptionResumeTimeout) {
			t.Errorf("%s=%q: error %v, want InvalidArgument naming the option", OptionResumeTimeout, bad, err)
		}
	}
	for value, want := range map[string]string{"0": "0", "90": "90", "2.5": "2.5"} {
		db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{OptionResumeTimeout: value})
		if err != nil {
			t.Fatalf("%s=%q: %v", OptionResumeTimeout, value, err)
		}
		if got, _ := db.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionResumeTimeout); got != want {
			t.Errorf("%s=%q reads back as %q, want %q", OptionResumeTimeout, value, got, want)
		}
	}
	// A connection string from before the rename spells it adbc.warp.*.
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{"adbc.warp.resume_timeout": "42"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := db.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionResumeTimeout); got != "42" {
		t.Errorf("adbc.warp.resume_timeout reads back as %q", got)
	}
	// A browser sign-in reads the option like a database.
	_, err = BrowserSignIn(ctx, map[string]string{adbc.OptionKeyURI: "chargewire://127.0.0.1:1", OptionResumeTimeout: "soon"},
		SystemBrowser(BrowserNone), time.Second)
	if adbcErr, ok := err.(adbc.Error); !ok || adbcErr.Code != adbc.StatusInvalidArgument {
		t.Errorf("BrowserSignIn with a bad %s: %v", OptionResumeTimeout, err)
	}
}
