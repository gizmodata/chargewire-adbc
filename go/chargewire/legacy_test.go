// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Connection strings saved before the protocol was renamed to Chargewire:
// the old URI scheme and option keys read as their current spellings.

func TestLegacyURIScheme(t *testing.T) {
	for raw, want := range map[string]string{
		"warp://h:1":                      "https://h:1/chargewire",
		"WARP://h:1":                      "https://h:1/chargewire",
		"warp://127.0.0.1:8080?tls=false": "http://127.0.0.1:8080/chargewire",
		"warp://h:1/custom/prefix":        "https://h:1/custom/prefix",
	} {
		got, err := parseURI(raw)
		if err != nil || got.String() != want {
			t.Errorf("parseURI(%q) = %v, %v; want %s", raw, got, err, want)
		}
	}
	for _, raw := range []string{"warpx://h:1", "chargewire+warp://h:1"} {
		if _, err := parseURI(raw); err == nil {
			t.Errorf("parseURI(%q) accepted", raw)
		}
	}
}

func TestLegacyOptionKeys(t *testing.T) {
	for key, want := range map[string]string{
		"adbc.warp.statement.timeout_seconds": OptionStatementTimeout,
		"adbc.warp.tls.skip_verify":           OptionTLSSkipVerify,
		"adbc.warp.session.id":                OptionSessionID,
		OptionQueryTag:                        OptionQueryTag,
		"adbc.warpx.statement.timeout":        "adbc.warpx.statement.timeout",
		"uri":                                 "uri",
	} {
		if got := currentOptionKey(key); got != want {
			t.Errorf("currentOptionKey(%q) = %q, want %q", key, got, want)
		}
	}
	// Both spellings of one key: the current one wins, whatever the map order.
	got := currentOptions(map[string]string{"adbc.warp.retry.max_attempts": "9", OptionRetryMaxAttempts: "2", "uri": "u"})
	if len(got) != 2 || got[OptionRetryMaxAttempts] != "2" || got["uri"] != "u" {
		t.Errorf("currentOptions = %v", got)
	}
}

// A database, connection and statement configured only with the old spellings
// behave as with the current ones, end to end against the fake server.
func TestLegacyConnectionString(t *testing.T) {
	f := newFakeChargewire(t)
	ctx := context.Background()
	uri := "warp://" + strings.TrimPrefix(f.server.URL, "http://") + "?tls=false"
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: uri, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p",
		"adbc.warp.statement.query_tag": `{"job": "legacy"}`,
		"adbc.warp.retry.max_attempts":  "7",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if got, _ := db.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionRetryMaxAttempts); got != "7" {
		t.Errorf("%s = %q, want 7", OptionRetryMaxAttempts, got)
	}
	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cnxn.Close(ctx)
	values, err := f.query(t, cnxn)
	if err != nil {
		t.Fatal(err)
	}
	wantSequence(t, values, 1, f.batches)
	if got := f.lastSubmit().Get("Chargewire-Query-Tag"); got != `{"job":"legacy"}` {
		t.Errorf("Chargewire-Query-Tag %q", got)
	}

	// Set and read on a connection and a statement under the old keys.
	options := cnxn.(adbc.GetSetOptionsWithContext)
	if err := options.SetOption(ctx, "adbc.warp.statement.query_tag", `{"job": "conn"}`); err != nil {
		t.Fatal(err)
	}
	if got, _ := options.GetOption(ctx, "adbc.warp.statement.query_tag"); got != `{"job":"conn"}` {
		t.Errorf("connection reads back %q", got)
	}
	if got, _ := options.GetOption(ctx, "adbc.warp.session.id"); got != "s1" {
		t.Errorf("session id %q", got)
	}
	stmt, err := cnxn.NewStatement(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	if err := stmt.SetOption(ctx, "adbc.warp.statement.query_tag", `{"job": "stmt"}`); err != nil {
		t.Fatal(err)
	}
	if got, _ := stmt.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionQueryTag); got != `{"job":"stmt"}` {
		t.Errorf("statement reads back %q", got)
	}
	_ = stmt.SetSqlQuery(ctx, "SELECT v")
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader.Release()
	if got := f.lastSubmit().Get("Chargewire-Query-Tag"); got != `{"job":"stmt"}` {
		t.Errorf("Chargewire-Query-Tag %q", got)
	}
}

// legacyFront is a server from before the rename in front of a current one:
// it answers /chargewire with 404 and serves the old spelling at /warp
// (paths, Warp-* headers both ways, the discovery name, problem types),
// translating to and from the current server behind it. It records every
// request path and the request headers it saw on the wire.
type legacyFront struct {
	server  *httptest.Server
	mu      sync.Mutex
	paths   []string
	headers []http.Header
	// protocol is the discovery name it answers with at /warp.
	protocol string
}

func newLegacyFront(t *testing.T, inner http.Handler) *legacyFront {
	front := &legacyFront{protocol: "warp-sql"}
	front.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		front.mu.Lock()
		front.paths = append(front.paths, r.URL.Path)
		front.headers = append(front.headers, r.Header.Clone())
		front.mu.Unlock()
		rest, ok := strings.CutPrefix(r.URL.Path, "/warp")
		if !ok || (rest != "" && !strings.HasPrefix(rest, "/")) {
			http.NotFound(w, r)
			return
		}
		r = r.Clone(r.Context())
		r.URL.Path, r.URL.RawPath = "/chargewire"+rest, ""
		for key, values := range r.Header {
			if name, ok := strings.CutPrefix(key, "Warp-"); ok {
				delete(r.Header, key)
				r.Header["Chargewire-"+name] = values
			}
		}
		out := &legacyWriter{ResponseWriter: w, protocol: front.protocol}
		inner.ServeHTTP(out, r)
		out.flush()
	}))
	t.Cleanup(front.server.Close)
	return front
}

func (f *legacyFront) seen() ([]string, []http.Header) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...), append([]http.Header(nil), f.headers...)
}

func (f *legacyFront) uri(path string) string {
	return "chargewire://" + strings.TrimPrefix(f.server.URL, "http://") + path + "?tls=false"
}

// legacyWriter renames Chargewire-* response headers to Warp-*, and rewrites
// JSON bodies to the old discovery name and problem types.
type legacyWriter struct {
	http.ResponseWriter
	protocol string
	status   int
	json     bool
	body     bytes.Buffer
}

func (w *legacyWriter) WriteHeader(status int) {
	header := w.ResponseWriter.Header()
	for key, values := range header {
		if name, ok := strings.CutPrefix(key, "Chargewire-"); ok {
			delete(header, key)
			header["Warp-"+name] = values
		}
	}
	w.json = strings.Contains(header.Get("Content-Type"), "json")
	if w.json {
		w.status = status
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *legacyWriter) Write(p []byte) (int, error) {
	if w.status == 0 && !w.json {
		w.WriteHeader(http.StatusOK)
	}
	if w.json {
		return w.body.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

func (w *legacyWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok && !w.json {
		flusher.Flush()
	}
}

func (w *legacyWriter) flush() {
	if !w.json {
		return
	}
	body := bytes.ReplaceAll(w.body.Bytes(), []byte(`"protocol":"chargewire"`), []byte(`"protocol":"`+w.protocol+`"`))
	body = bytes.ReplaceAll(body, []byte(`"urn:chargewire:error:`), []byte(`"urn:warp-sql:error:`))
	w.ResponseWriter.WriteHeader(w.status)
	_, _ = w.ResponseWriter.Write(body)
}

func legacyDatabase(t *testing.T, uri string) adbc.DatabaseWithContext {
	t.Helper()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
		adbc.OptionKeyURI: uri, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p",
		OptionQueryTag: `{"job":"old"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db
}

// At the default base path, a server that answers only the old spelling is
// spoken to in it: discovery once per database, every request under /warp
// with Warp-* headers, and Warp-* response headers read as the current ones.
func TestLegacyServerFallback(t *testing.T) {
	f := newFakeChargewire(t)
	front := newLegacyFront(t, f.server.Config.Handler)
	f.setSubmit(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Chargewire-Statement-Id", "q9")
		fakeProblem(w, http.StatusConflict, "statement_cancelled")
	})
	for _, path := range []string{"", "/chargewire", "/warp"} {
		db := legacyDatabase(t, front.uri(path))
		for range 2 {
			cnxn, err := db.Open(context.Background())
			if err != nil {
				t.Fatalf("path %q: %v", path, err)
			}
			stmt, _ := cnxn.NewStatement(context.Background())
			_ = stmt.SetSqlQuery(context.Background(), "SELECT v")
			_, _, err = stmt.ExecuteQuery(context.Background())
			assertFakeError(t, err, adbc.StatusCancelled, "statement_cancelled")
			_ = stmt.Close(context.Background())
			// Open until the test ends: the driver deletes the statement in
			// the background.
			t.Cleanup(func() { _ = cnxn.Close(context.Background()) })
		}
	}
	paths, headers := front.seen()
	discoveries := map[string]int{}
	for i, path := range paths {
		if path == "/chargewire" || path == "/warp" {
			discoveries[path]++
			continue
		}
		if !strings.HasPrefix(path, "/warp/v1/") {
			t.Errorf("request to %s", path)
		}
		for key := range headers[i] {
			if strings.HasPrefix(key, "Chargewire-") {
				t.Errorf("%s sent header %s", path, key)
			}
		}
	}
	// Three databases, two connections each: one discovery per database.
	if discoveries["/chargewire"] != 3 || discoveries["/warp"] != 3 {
		t.Errorf("discovery requests %v, want 3 at each root", discoveries)
	}
	// The query tag went out as Warp-Query-Tag and the statement id came back
	// in Warp-Statement-Id (the driver deleted q9).
	if got := f.lastSubmit().Get("Chargewire-Query-Tag"); got != `{"job":"old"}` {
		t.Errorf("query tag %q", got)
	}
	if deleted := f.deletedStatements(6); len(deleted) != 6 || deleted[0] != "q9" {
		t.Errorf("statements deleted %v, want q9 six times", deleted)
	}
}

// A current server is never asked for the old spelling.
func TestLegacyNoFallbackForCurrentServer(t *testing.T) {
	f := newFakeChargewire(t)
	var mu sync.Mutex
	var paths []string
	inner := f.server.Config.Handler
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		inner.ServeHTTP(w, r)
	}))
	defer server.Close()
	f.server = server
	cnxn := f.connect(t, nil)
	if _, err := f.query(t, cnxn); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, path := range paths {
		if strings.HasPrefix(path, "/warp") {
			t.Errorf("request to %s", path)
		}
	}
	if len(paths) < 4 || paths[0] != "/chargewire" {
		t.Errorf("requests %v", paths)
	}
}

// A custom base path never falls back, and when the old spelling isn't
// there either the first error stands, without the old name.
func TestLegacyFallbackRefused(t *testing.T) {
	f := newFakeChargewire(t)
	front := newLegacyFront(t, f.server.Config.Handler)
	db := legacyDatabase(t, front.uri("/custom"))
	_, err := db.Open(context.Background())
	if err == nil {
		t.Fatal("connected at a custom base path")
	}
	if paths, _ := front.seen(); len(paths) != 1 || paths[0] != "/custom" {
		t.Errorf("requests %v, want only /custom", paths)
	}

	other := newLegacyFront(t, f.server.Config.Handler)
	other.protocol = "something-else"
	_, err = legacyDatabase(t, other.uri("")).Open(context.Background())
	if err == nil {
		t.Fatal("connected to a server that speaks neither spelling")
	}
	if !strings.Contains(err.Error(), "/chargewire") || !strings.Contains(err.Error(), "404") ||
		strings.Contains(strings.ToLower(err.Error()), "warp") {
		t.Errorf("error %q, want the 404 from /chargewire", err)
	}
	var doc map[string]any
	if json.Unmarshal([]byte(err.Error()), &doc) == nil {
		t.Errorf("error is a JSON document: %v", doc)
	}
}
