// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// openFakeWithHook is openFake with a new-session hook on Open's context.
func openFakeWithHook(t *testing.T, f *fakeChargewire, extra map[string]string, hook NewSessionHook) (adbc.ConnectionWithContext, error) {
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
	return db.Open(WithNewSessionHook(context.Background(), hook))
}

// recordSQL makes f answer submits with a finished update and records their SQL.
func recordSQL(f *fakeChargewire) func() []string {
	var mu sync.Mutex
	var ran []string
	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			SQL string `json:"sql"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		ran = append(ran, body.SQL)
		mu.Unlock()
		fakeJSON(w, http.StatusCreated, map[string]any{"statement_id": "q1", "state": "succeeded",
			"result": map[string]any{"kind": "update", "rows_affected": 0}})
	})
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(ran)
	}
}

// The hook runs on the connection's first session inside Open, before the
// application can use it.
func TestFakeNewSessionHookRunsAtOpen(t *testing.T) {
	f := newFakeChargewire(t)
	ran := recordSQL(f)
	var seen []string
	cnxn, err := openFakeWithHook(t, f, nil, func(ctx context.Context, s NewSession) error {
		seen = append(seen, s.ID()+" replaces "+s.Replaces())
		return s.Exec(ctx, "SET a = 1")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cnxn.Close(context.Background()) }()
	if !slices.Equal(seen, []string{"s1 replaces "}) || !slices.Equal(ran(), []string{"SET a = 1"}) {
		t.Fatalf("the hook saw %v and ran %v", seen, ran())
	}
}

// A hook that fails fails Open, with the server's or the hook's error and its
// status; the session is ended. An adbc.Error from the hook keeps its message.
func TestFakeNewSessionHookFailureFailsOpen(t *testing.T) {
	f := newFakeChargewire(t)
	var deletes []string
	var mu sync.Mutex
	f.setSessionDelete(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		deletes = append(deletes, r.PathValue("s"))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusCreated, map[string]any{"statement_id": "q1", "state": "failed",
			"error": map[string]any{"code": "sql_error", "status": 400, "detail": "unknown setting"}})
	})
	_, err := openFakeWithHook(t, f, nil, func(ctx context.Context, s NewSession) error {
		return s.Exec(ctx, "SET nope = 1")
	})
	assertFakeError(t, err, adbc.StatusInvalidArgument, "sql_error")
	if !strings.Contains(err.Error(), "setting up new session s1 failed") || !strings.Contains(err.Error(), "unknown setting") {
		t.Fatalf("error %q", err)
	}
	mu.Lock()
	if !slices.Equal(deletes, []string{"s1"}) {
		t.Fatalf("sessions ended: %v, want s1", deletes)
	}
	mu.Unlock()

	_, err = openFakeWithHook(t, f, nil, func(ctx context.Context, s NewSession) error {
		return adbc.Error{Code: adbc.StatusInvalidState, Msg: "the hook's own error"}
	})
	var adbcErr adbc.Error
	if !errors.As(err, &adbcErr) || !strings.Contains(adbcErr.Msg, "setting up new session s1 failed") ||
		!strings.Contains(adbcErr.Msg, "the hook's own error") {
		t.Fatalf("error %v", err)
	}
}

// A session the driver attached to was set up by whoever created it: the hook
// does not run.
func TestFakeNewSessionHookSkipsAttachedSession(t *testing.T) {
	f := newFakeChargewire(t)
	recordSessions(f)
	called := false
	cnxn, err := openFakeWithHook(t, f, map[string]string{OptionSessionID: "s9"}, func(ctx context.Context, s NewSession) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cnxn.Close(context.Background()) }()
	if called {
		t.Fatal("the hook ran on an attached session")
	}
}
