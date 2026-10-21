// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// sessionRecorder wraps a fake server: it answers GET of session "s9" (and
// 404 session_not_found for any other id) and records the session requests
// the driver sends ("POST /warp/v1/sessions", "DELETE /warp/v1/sessions/s9").
type sessionRecorder struct {
	mu       sync.Mutex
	requests []string
}

func recordSessions(f *fakeWarp) *sessionRecorder {
	rec := &sessionRecorder{}
	mux := f.server.Config.Handler.(*http.ServeMux)
	mux.HandleFunc("GET /warp/v1/sessions/{s}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("s") != "s9" {
			fakeProblem(w, http.StatusNotFound, "session_not_found")
			return
		}
		fakeJSON(w, http.StatusOK, map[string]any{"session_id": "s9", "options": map[string]any{"autocommit": true},
			"transaction": "open"})
	})
	f.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasPrefix(path, "/warp/v1/sessions") && !strings.Contains(path, "/statements") {
			rec.mu.Lock()
			rec.requests = append(rec.requests, r.Method+" "+path)
			rec.mu.Unlock()
		}
		mux.ServeHTTP(w, r)
	})
	return rec
}

func (rec *sessionRecorder) take() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	requests := rec.requests
	rec.requests = nil
	return requests
}

func (rec *sessionRecorder) count(request string) int {
	n := 0
	for _, r := range rec.take() {
		if r == request {
			n++
		}
	}
	return n
}

// openFake opens a database and a connection on f with extra options, and
// returns the connection and Open's error.
func openFake(t *testing.T, f *fakeWarp, extra map[string]string) (adbc.ConnectionWithContext, error) {
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
	return db.Open(context.Background())
}

func optionOf(t *testing.T, cnxn adbc.ConnectionWithContext, key string) string {
	t.Helper()
	value, err := cnxn.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), key)
	if err != nil {
		t.Fatalf("GetOption(%s): %v", key, err)
	}
	return value
}

// adbc.warp.session.id on the database: the connection reads the session to
// check it, works in it, creates none, and leaves it alive when it closes.
func TestFakeAttachToASession(t *testing.T) {
	f := newFakeWarp(t)
	rec := recordSessions(f)
	cnxn, err := openFake(t, f, map[string]string{OptionSessionID: "s9"})
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.take(); len(got) != 1 || got[0] != "GET /warp/v1/sessions/s9" {
		t.Fatalf("attaching sent %v; want only GET /warp/v1/sessions/s9", got)
	}
	if got := optionOf(t, cnxn, OptionSessionID); got != "s9" {
		t.Fatalf("session id %q, want s9", got)
	}
	if got := optionOf(t, cnxn, OptionSessionCloseOnRelease); got != "false" {
		t.Fatalf("close_on_release of an attached session is %q, want false", got)
	}
	if got := optionOf(t, cnxn, OptionSessionTransaction); got != "open" {
		t.Fatalf("transaction %q, want open (the session's)", got)
	}
	values, err := f.query(t, cnxn)
	if err != nil || len(values) == 0 {
		t.Fatalf("query in the attached session: %v, %v", values, err)
	}
	if f.lastSubmit() == nil {
		t.Fatal("no statement was submitted")
	}
	rec.take()
	if err := cnxn.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("closing an attached connection sent %v; want nothing (the session stays)", got)
	}
}

// An id the server doesn't have (or another user's: the same 404, §9.2) is
// session_not_found, and no session is created instead.
func TestFakeAttachToAMissingSession(t *testing.T) {
	f := newFakeWarp(t)
	rec := recordSessions(f)
	_, err := openFake(t, f, map[string]string{OptionSessionID: "nope"})
	assertFakeError(t, err, adbc.StatusNotFound, "session_not_found")
	if !strings.Contains(err.Error(), "attaching to session nope (the server doesn't have it for this user") {
		t.Fatalf("error %q does not say which session or why", err)
	}
	if n := rec.count("POST /warp/v1/sessions"); n != 0 {
		t.Fatalf("%d sessions were created after a failed attach", n)
	}
}

// An attached session the server later loses is not replaced by a new one.
func TestFakeAttachedSessionIsNeverReplaced(t *testing.T) {
	f := newFakeWarp(t)
	rec := recordSessions(f)
	cnxn, err := openFake(t, f, map[string]string{OptionSessionID: "s9"})
	if err != nil {
		t.Fatal(err)
	}
	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		fakeProblem(w, http.StatusNotFound, "session_not_found")
	})
	_, err = f.query(t, cnxn)
	// A lost session the driver doesn't replace is IO, as with reconnect off.
	assertFakeError(t, err, adbc.StatusIO, "session_not_found")
	if !strings.Contains(err.Error(), "the connection must be reopened") {
		t.Fatalf("error %q", err)
	}
	if n := rec.count("POST /warp/v1/sessions"); n != 0 {
		t.Fatalf("the lost attached session was replaced (%d sessions created)", n)
	}
	if got := optionOf(t, cnxn, OptionSessionReplacements); got != "0" {
		t.Fatalf("replacements %q, want 0", got)
	}
}

// Who ends the session on Close: by default the connection that created it,
// never one that attached; an explicit close_on_release wins, on the
// database and on the connection.
func TestFakeCloseOnRelease(t *testing.T) {
	const deleteS1, deleteS9 = "DELETE /warp/v1/sessions/s1", "DELETE /warp/v1/sessions/s9"
	for _, tc := range []struct {
		name       string
		database   map[string]string
		connection string // set on the open connection, when not ""
		delete     string // the DELETE Close must send, or "" for none
	}{
		{"created", nil, "", deleteS1},
		{"created, false on the database", map[string]string{OptionSessionCloseOnRelease: "false"}, "", ""},
		{"created, false on the connection", nil, "false", ""},
		{"attached", map[string]string{OptionSessionID: "s9"}, "", ""},
		{"attached, true on the database", map[string]string{OptionSessionID: "s9", OptionSessionCloseOnRelease: "true"}, "", deleteS9},
		{"attached, true on the connection", map[string]string{OptionSessionID: "s9"}, "true", deleteS9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeWarp(t)
			rec := recordSessions(f)
			cnxn, err := openFake(t, f, tc.database)
			if err != nil {
				t.Fatal(err)
			}
			if tc.connection != "" {
				if err := cnxn.(adbc.GetSetOptionsWithContext).SetOption(context.Background(), OptionSessionCloseOnRelease, tc.connection); err != nil {
					t.Fatal(err)
				}
				if got := optionOf(t, cnxn, OptionSessionCloseOnRelease); got != tc.connection {
					t.Fatalf("close_on_release reads %q after setting %q", got, tc.connection)
				}
			}
			rec.take()
			if err := cnxn.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := rec.take()
			if tc.delete == "" && len(got) != 0 || tc.delete != "" && (len(got) != 1 || got[0] != tc.delete) {
				t.Fatalf("Close sent %v; want %q", got, tc.delete)
			}
		})
	}
}

func TestFakeSessionOptionsAreChecked(t *testing.T) {
	f := newFakeWarp(t)
	cnxn, err := openFake(t, f, nil)
	if err != nil {
		t.Fatal(err)
	}
	options := cnxn.(adbc.GetSetOptionsWithContext)
	err = options.SetOption(context.Background(), OptionSessionID, "s9")
	if err == nil || !strings.Contains(err.Error(), "set it on the database before its first connection opens") {
		t.Fatalf("setting the session id on a connection: %v", err)
	}
	if err := options.SetOption(context.Background(), OptionSessionCloseOnRelease, "maybe"); err == nil {
		t.Fatal("close_on_release took \"maybe\"")
	}
	if _, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(),
		map[string]string{adbc.OptionKeyURI: f.server.URL, OptionSessionCloseOnRelease: "maybe"}); err == nil {
		t.Fatal("close_on_release on a database took \"maybe\"")
	}
}
