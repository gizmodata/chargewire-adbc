// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fake is a scriptable Warp SQL server for protocol tests.
type fake struct {
	t      *testing.T
	server *httptest.Server
	mux    *http.ServeMux

	mu       sync.Mutex
	tokens   int // tokens issued
	valid    map[string]bool
	sessions map[string]map[string]any // id -> options
	nextID   int
	created  []map[string]any // options of every session created
	lifetime int64            // expires_in of issued tokens
	grants   []map[string]string
}

func newFake(t *testing.T) *fake {
	f := &fake{t: t, mux: http.NewServeMux(), valid: map[string]bool{}, sessions: map[string]map[string]any{}, lifetime: 3600}
	f.mux.HandleFunc("GET /warp", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"protocol": "warp-sql", "protocol_versions": []string{"1"},
			"server": map[string]string{"name": "fake", "version": "1"}, "capabilities": []string{"transactions"},
			"auth": map[string]any{"grant_types": []string{"password"}}})
	})
	f.mux.HandleFunc("POST /warp/v1/auth/token", func(w http.ResponseWriter, r *http.Request) {
		var grant map[string]string
		_ = json.NewDecoder(r.Body).Decode(&grant)
		f.mu.Lock()
		f.grants = append(f.grants, grant)
		f.mu.Unlock()
		if grant["password"] != "s3:cr@t" && grant["grant_type"] != GrantClientCertificate {
			writeProblem(w, http.StatusUnauthorized, CodeInvalidCredentials, "")
			return
		}
		f.mu.Lock()
		f.tokens++
		token := "t" + string(rune('0'+f.tokens))
		f.valid[token] = true
		lifetime := f.lifetime
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": lifetime})
	})
	f.mux.HandleFunc("POST /warp/v1/sessions", f.auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Options map[string]any `json:"options"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.nextID++
		id := "s" + string(rune('0'+f.nextID))
		f.sessions[id] = body.Options
		f.created = append(f.created, body.Options)
		f.mu.Unlock()
		writeJSON(w, http.StatusCreated, map[string]any{"session_id": id, "options": body.Options, "idle_timeout_s": 900})
	}))
	f.server = httptest.NewServer(f.mux)
	t.Cleanup(f.server.Close)
	return f
}

// auth checks the bearer token, as every authenticated endpoint does.
func (f *fake) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		ok := f.valid[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		f.mu.Unlock()
		if !ok {
			writeProblem(w, http.StatusUnauthorized, CodeInvalidToken, "")
			return
		}
		next(w, r)
	}
}

func (f *fake) client(credentials Credentials, attempts int) *Client {
	base, _ := url.Parse(f.server.URL + "/warp")
	return NewClient(Config{BaseURL: base, Credentials: credentials, MaxAttempts: attempts})
}

func (f *fake) revokeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.valid = map[string]bool{}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", MediaTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, code, reason string) {
	w.Header().Set("Content-Type", MediaTypeProblem)
	w.Header().Set("Warp-Request-Id", "r_test")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "urn:warp-sql:error:" + code, "status": status, "code": code,
		"detail": "fake " + code, "reason": reason})
}

var password = Credentials{Username: "joe", Password: "s3:cr@t"}

func TestDiscoverChecksTheProtocol(t *testing.T) {
	f := newFake(t)
	doc, err := f.client(password, 1).Discover(context.Background())
	if err != nil || doc.Server.Name != "fake" || !doc.Has(CapabilityTransactions) || doc.Has(CapabilityIngest) {
		t.Fatalf("Discover: %+v, %v", doc, err)
	}
	f.mux.HandleFunc("GET /other", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"protocol": "warp-sql", "protocol_versions": []string{"2"}})
	})
	base, _ := url.Parse(f.server.URL + "/other")
	if _, err := NewClient(Config{BaseURL: base}).Discover(context.Background()); err == nil || !strings.Contains(err.Error(), "version 1") {
		t.Fatalf("a version-2-only server was accepted: %v", err)
	}
}

func TestAuthenticateFailsWithTheServersCode(t *testing.T) {
	f := newFake(t)
	err := f.client(Credentials{Username: "joe", Password: "wrong"}, 1).Authenticate(context.Background())
	if !HasCode(err, CodeInvalidCredentials) {
		t.Fatalf("error %v", err)
	}
	p, _ := errors.AsType[*Problem](err)
	if p.Status != http.StatusUnauthorized || p.RequestID != "r_test" {
		t.Fatalf("problem %+v", p)
	}
}

// A token the server forgot is replaced and the request sent again, once.
func TestExpiredTokenIsRenewed(t *testing.T) {
	f := newFake(t)
	client := f.client(password, 1)
	session, err := client.OpenSession(context.Background(), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	f.mux.HandleFunc("GET /warp/v1/sessions/{id}", f.auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"session_id": r.PathValue("id"), "options": map[string]any{}})
	}))
	f.revokeAll()
	if _, err := session.Get(context.Background()); err != nil {
		t.Fatalf("Get after the token was revoked: %v", err)
	}
	if f.tokens != 2 {
		t.Fatalf("%d tokens issued, want 2", f.tokens)
	}
}

// A token close to its expiry is replaced before it is sent.
func TestTokenIsRenewedBeforeItExpires(t *testing.T) {
	f := newFake(t)
	f.lifetime = 1
	client := f.client(password, 1)
	if err := client.Authenticate(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if _, err := client.OpenSession(context.Background(), nil, true); err != nil {
		t.Fatal(err)
	}
	if f.tokens != 2 {
		t.Fatalf("%d tokens issued, want 2 (renewed before use)", f.tokens)
	}
}

// A token issued elsewhere cannot be renewed: the refusal is the error.
func TestAccessTokenIsUsedAsIs(t *testing.T) {
	f := newFake(t)
	_, err := f.client(Credentials{AccessToken: "issued-elsewhere"}, 1).OpenSession(context.Background(), nil, true)
	if !HasCode(err, CodeInvalidToken) || f.tokens != 0 {
		t.Fatalf("error %v after %d token requests", err, f.tokens)
	}
}

// hijackOnce answers a request by closing the connection the first n times.
func hijackOnce(n int, then http.HandlerFunc) (http.HandlerFunc, *atomic.Int32) {
	calls := &atomic.Int32{}
	return func(w http.ResponseWriter, r *http.Request) {
		if int(calls.Add(1)) <= n {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		then(w, r)
	}, calls
}

func TestIdempotentRequestsAreRetriedAfterTransportErrors(t *testing.T) {
	f := newFake(t)
	handler, calls := hijackOnce(2, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{})
	})
	f.mux.HandleFunc("GET /warp/v1/flaky", handler)
	f.mux.HandleFunc("POST /warp/v1/flaky", handler)
	client := f.client(Credentials{AccessToken: "x"}, 3)

	resp, err := client.Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/flaky", Idempotent: true, Anonymous: true})
	if err != nil {
		t.Fatalf("GET after two dropped connections: %v", err)
	}
	resp.Body.Close()
	if calls.Load() != 3 {
		t.Fatalf("%d calls, want 3", calls.Load())
	}

	calls.Store(0)
	_, err = client.Do(context.Background(), &Request{Method: http.MethodPost, Path: "/v1/flaky", Anonymous: true})
	if _, ok := errors.AsType[*TransportError](err); !ok || calls.Load() != 1 {
		t.Fatalf("a non-idempotent request was retried (%d calls): %v", calls.Load(), err)
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	f := newFake(t)
	calls := atomic.Int32{}
	f.mux.HandleFunc("GET /warp/v1/busy", func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			writeProblem(w, http.StatusServiceUnavailable, CodeServerDraining, "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	})
	start := time.Now()
	resp, err := f.client(Credentials{AccessToken: "x"}, 2).Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/busy", Idempotent: true, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("retried after %v, before Retry-After", elapsed)
	}
}

// A client error is not retried, and an unstructured error body still has a
// code: a client_ one, with the response's status (§11).
func TestErrorsWithoutProblemDocuments(t *testing.T) {
	f := newFake(t)
	calls := atomic.Int32{}
	f.mux.HandleFunc("GET /warp/v1/proxy", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "<html>request entity too large</html>", http.StatusRequestEntityTooLarge)
	})
	_, err := f.client(Credentials{AccessToken: "x"}, 4).Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/proxy", Idempotent: true, Anonymous: true})
	if !HasCode(err, CodeClientHTTPError) || !HasStatus(err, http.StatusRequestEntityTooLarge) || calls.Load() != 1 {
		t.Fatalf("%d calls: %v", calls.Load(), err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	for value, want := range map[string]time.Duration{"3": 3 * time.Second, " 0 ": 0, "": 0, "-1": 0, "Wed, 21 Oct 2026 07:28:00 GMT": 0} {
		if got := parseRetryAfter(value); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", value, got, want)
		}
	}
}

// The client_certificate grant sends nothing but its type (§4.4), and is sent
// again, unchanged, to renew the token: when the server forgets the token and
// before it expires.
func TestClientCertificateGrantSendsNoSecret(t *testing.T) {
	f := newFake(t)
	f.lifetime = 1
	client := f.client(Credentials{ClientCertificate: true}, 1)
	session, err := client.OpenSession(context.Background(), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	f.mux.HandleFunc("GET /warp/v1/sessions/{id}", f.auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"session_id": r.PathValue("id"), "options": map[string]any{}})
	}))
	f.revokeAll()
	if _, err := session.Get(context.Background()); err != nil {
		t.Fatalf("Get after the token was revoked: %v", err)
	}
	time.Sleep(time.Second)
	if _, err := session.Get(context.Background()); err != nil {
		t.Fatalf("Get after the token expired: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.grants) != 3 {
		t.Fatalf("%d grants sent, want 3 (sign-in, renewal after revocation, renewal before expiry)", len(f.grants))
	}
	for _, grant := range f.grants {
		if len(grant) != 1 || grant["grant_type"] != GrantClientCertificate {
			t.Errorf("grant %v, want only grant_type=client_certificate", grant)
		}
	}
}
