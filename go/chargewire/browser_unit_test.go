// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/chargewire-adbc/go/internal/protocol"
)

// fakeBrowserServer serves discovery, the browser sign-in endpoints (§4.3)
// and session creation. Polls answer from `answers` in order (the last one
// repeats); a poll with another secret gets 404 flow_not_found, as from a
// real server.
type fakeBrowserServer struct {
	t       *testing.T
	server  *httptest.Server
	offer   bool
	answers []map[string]any

	mu        sync.Mutex
	flows     int
	polls     int
	secrets   []string // poll secrets received
	sessions  []string // Authorization headers of session creations
	returnTos []string
}

const fakeSecret = "s3cr3t-poll-secret"

func newFakeBrowserServer(t *testing.T, offer bool, answers ...map[string]any) *fakeBrowserServer {
	f := &fakeBrowserServer{t: t, offer: offer, answers: answers}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /chargewire", func(w http.ResponseWriter, r *http.Request) {
		capabilities := []string{"long_poll"}
		auth := map[string]any{"grant_types": []string{"password"}}
		if f.offer {
			capabilities = append(capabilities, "oauth_browser")
			auth["oauth_browser"] = map[string]any{"provider": "Fake IdP"}
		}
		fakeJSON(w, http.StatusOK, map[string]any{"protocol": "chargewire", "protocol_versions": []string{"1"},
			"server": map[string]string{"name": "fake", "version": "1"}, "capabilities": capabilities, "auth": auth})
	})
	mux.HandleFunc("POST /chargewire/v1/auth/oauth/flows", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.flows++
		f.returnTos = append(f.returnTos, body["return_to"])
		f.mu.Unlock()
		fakeJSON(w, http.StatusCreated, map[string]any{"flow_id": "f_1", "poll_secret": fakeSecret,
			"authorize_url": "https://idp.example/authorize?state=abc", "poll_interval_s": 1,
			"expires_at": "2099-01-01T00:00:00Z"})
	})
	mux.HandleFunc("POST /chargewire/v1/auth/oauth/flows/{id}", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.secrets = append(f.secrets, body["poll_secret"])
		if r.PathValue("id") != "f_1" || body["poll_secret"] != fakeSecret {
			fakeDetailedProblem(w, http.StatusNotFound, "flow_not_found", "no such browser sign-in")
			return
		}
		answer := f.answers[min(f.polls, len(f.answers)-1)]
		f.polls++
		if answer == nil {
			fakeDetailedProblem(w, http.StatusNotFound, "flow_not_found", "no such browser sign-in")
			return
		}
		fakeJSON(w, http.StatusOK, answer)
	})
	mux.HandleFunc("POST /chargewire/v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sessions = append(f.sessions, r.Header.Get("Authorization"))
		f.mu.Unlock()
		fakeJSON(w, http.StatusCreated, map[string]any{"session_id": "s1", "options": map[string]any{}})
	})
	mux.HandleFunc("DELETE /chargewire/v1/sessions/{s}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func fakeDetailedProblem(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "urn:chargewire:error:" + code, "status": status,
		"code": code, "detail": detail})
}

var (
	pending  = map[string]any{"state": "pending"}
	complete = map[string]any{"state": "complete", "access_token": "browser-token", "token_type": "Bearer",
		"expires_in": 3600}
)

// fastBrowser makes polls quick and replaces the browser with a recorder.
func fastBrowser(t *testing.T) *[]string {
	t.Helper()
	opened := &[]string{}
	var mu sync.Mutex
	savedOpen, savedInterval := openBrowser, browserPollInterval
	openBrowser = func(command string) BrowserOpener {
		return func(_ context.Context, url string) error {
			mu.Lock()
			defer mu.Unlock()
			*opened = append(*opened, command+" "+url)
			return nil
		}
	}
	browserPollInterval = func(*protocol.BrowserFlow) time.Duration { return 10 * time.Millisecond }
	t.Cleanup(func() { openBrowser, browserPollInterval = savedOpen, savedInterval })
	return opened
}

func browserOptions(f *fakeBrowserServer, extra ...string) map[string]string {
	options := map[string]string{adbc.OptionKeyURI: f.server.URL, OptionAuthType: AuthTypeOAuthBrowser}
	for i := 0; i+1 < len(extra); i += 2 {
		options[extra[i]] = extra[i+1]
	}
	return options
}

func TestBrowserSignInUsesTheFlowsToken(t *testing.T) {
	opened := fastBrowser(t)
	f := newFakeBrowserServer(t, true, pending, pending, complete)
	db, _, err := openWith(t, browserOptions(f, OptionOAuthBrowser, "my-browser --new-tab"))
	if err != nil {
		t.Fatal(err)
	}
	if len(*opened) != 1 || (*opened)[0] != "my-browser --new-tab https://idp.example/authorize?state=abc" {
		t.Fatalf("opened %v", *opened)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.polls != 3 || f.flows != 1 || f.returnTos[0] != "" {
		t.Fatalf("polls %d, flows %d, return_to %q", f.polls, f.flows, f.returnTos)
	}
	for _, secret := range f.secrets {
		if secret != fakeSecret {
			t.Fatalf("polled with %q", secret)
		}
	}
	if len(f.sessions) != 1 || f.sessions[0] != "Bearer browser-token" {
		t.Fatalf("sessions opened with %v", f.sessions)
	}
	if got, _ := db.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), OptionAuthType); got != AuthTypeOAuthBrowser {
		t.Fatalf("auth type %q", got)
	}
}

func TestBrowserSignInNeedsTheCapability(t *testing.T) {
	fastBrowser(t)
	f := newFakeBrowserServer(t, false, complete)
	_, _, err := openWith(t, browserOptions(f))
	if adbcStatus(err) != adbc.StatusUnauthenticated || !strings.Contains(err.Error(), "does not offer browser sign-in") {
		t.Fatalf("got %v", err)
	}
	if f.flows != 0 {
		t.Fatal("a flow was created on a server without the capability")
	}
}

func TestBrowserSignInFailuresSayWhy(t *testing.T) {
	for _, c := range []struct {
		answer map[string]any
		code   string
		detail string
	}{
		{map[string]any{"state": "failed", "error": map[string]any{"code": "access_denied", "detail": "the user said no"}},
			"access_denied", "the user said no"},
		{map[string]any{"state": "failed", "error": map[string]any{"code": "invalid_credentials",
			"detail": "User 'mallory' is not authorized"}}, "invalid_credentials", "User 'mallory' is not authorized"},
		{nil, "flow_not_found", "expired or was already collected"},
	} {
		fastBrowser(t)
		f := newFakeBrowserServer(t, true, pending, c.answer)
		_, _, err := openWith(t, browserOptions(f))
		if adbcStatus(err) != adbc.StatusUnauthenticated || ErrorCode(err) != c.code || !strings.Contains(err.Error(), c.detail) {
			t.Fatalf("%s: got %v (code %q)", c.code, err, ErrorCode(err))
		}
		if len(f.sessions) != 0 {
			t.Fatalf("%s: a session was opened", c.code)
		}
	}
}

// Spec 1.0 (§4.3.2, §12): a failed flow's code arrives in a 200 poll answer,
// so the error has no HTTP status; an unknown code is a generic sign-in
// failure (still StatusUnauthenticated); a failed flow without an error gets
// a client_ code; an unknown flow state is not terminal, so polling goes on.
func TestBrowserSignInV1Rules(t *testing.T) {
	for _, c := range []struct {
		answer map[string]any
		code   string
	}{
		{map[string]any{"state": "failed", "error": map[string]any{"code": "a_later_failure", "detail": "x"}}, "a_later_failure"},
		{map[string]any{"state": "failed", "error": map[string]any{"code": "identity_provider_error", "detail": "x"}}, "identity_provider_error"},
		{map[string]any{"state": "failed"}, protocol.CodeClientSignInFailed},
	} {
		fastBrowser(t)
		f := newFakeBrowserServer(t, true, pending, c.answer)
		_, _, err := openWith(t, browserOptions(f))
		if adbcStatus(err) != adbc.StatusUnauthenticated || ErrorCode(err) != c.code {
			t.Fatalf("%s: got %v (code %q)", c.code, err, ErrorCode(err))
		}
		if p, ok := errors.AsType[*protocol.Problem](err); !ok || p.Status != 0 {
			t.Fatalf("%s: problem %+v, want no HTTP status", c.code, p)
		}
	}
	fastBrowser(t)
	f := newFakeBrowserServer(t, true, map[string]any{"state": "a_state_from_a_later_version"},
		map[string]any{"state": "awaiting_consent"}, complete)
	if _, _, err := openWith(t, browserOptions(f)); err != nil {
		t.Fatalf("an unknown flow state stopped the sign-in: %v", err)
	}
	if f.polls != 3 || len(f.sessions) != 1 {
		t.Fatalf("%d polls, %d sessions; want 3 and 1", f.polls, len(f.sessions))
	}
}

func TestBrowserSignInTimesOutAndCancels(t *testing.T) {
	fastBrowser(t)
	f := newFakeBrowserServer(t, true, pending)
	start := time.Now()
	_, _, err := openWith(t, browserOptions(f, OptionOAuthTimeoutSeconds, "0.3"))
	if adbcStatus(err) != adbc.StatusTimeout || time.Since(start) > 5*time.Second {
		t.Fatalf("got %v after %s", err, time.Since(start))
	}

	db, err := NewDriver(nil).NewDatabaseWithContext(context.Background(), browserOptions(f))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(context.Background()) }()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	if _, err := db.Open(ctx); adbcStatus(err) != adbc.StatusCancelled {
		t.Fatalf("cancelled: got %v", err)
	}
}

func TestBrowserSignInTakesNoOtherCredentials(t *testing.T) {
	f := newFakeBrowserServer(t, true, complete)
	for _, key := range []string{adbc.OptionKeyUsername, adbc.OptionKeyPassword, OptionSubjectToken, OptionAccessToken} {
		_, _, err := openWith(t, browserOptions(f, key, "x"))
		if adbcStatus(err) != adbc.StatusInvalidArgument || !strings.Contains(err.Error(), "signs in through the browser") {
			t.Fatalf("%s: got %v", key, err)
		}
	}
	for _, value := range []string{"0", "-1", "soon"} {
		_, _, err := openWith(t, browserOptions(f, OptionOAuthTimeoutSeconds, value))
		if adbcStatus(err) != adbc.StatusInvalidArgument {
			t.Fatalf("timeout %q: got %v", value, err)
		}
	}
}

// The exported API, for drivers built on this one: the token and the
// provider's name, and the error kinds as for a database.
func TestBrowserSignInFunction(t *testing.T) {
	fastBrowser(t)
	f := newFakeBrowserServer(t, true, pending, complete)
	var shown string
	result, err := BrowserSignIn(context.Background(), map[string]string{adbc.OptionKeyURI: f.server.URL},
		func(_ context.Context, url string) error { shown = url; return nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.AccessToken != "browser-token" || result.ExpiresIn != time.Hour || result.Provider != "Fake IdP" ||
		shown != "https://idp.example/authorize?state=abc" {
		t.Fatalf("got %+v, shown %q", result, shown)
	}
	refused := errors.New("no browser here")
	_, err = BrowserSignIn(context.Background(), map[string]string{adbc.OptionKeyURI: f.server.URL},
		func(context.Context, string) error { return refused }, 0)
	if !errors.Is(err, refused) {
		t.Fatalf("an opener's error: got %v", err)
	}
	if _, err := BrowserSignIn(context.Background(), map[string]string{adbc.OptionKeyURI: "chargewire://"}, nil, 0); adbcStatus(err) != adbc.StatusInvalidArgument {
		t.Fatalf("bad uri: got %v", err)
	}
}

// An open database's access token can be replaced (a wrapper that renews or
// shares tokens); one that signs in with a password can't take one.
func TestAccessTokenCanBeReplacedOnAnOpenDatabase(t *testing.T) {
	fastBrowser(t)
	f := newFakeBrowserServer(t, true, complete)
	db, _, err := openWith(t, browserOptions(f))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	setter := db.(adbc.GetSetOptionsWithContext)
	if err := setter.SetOption(ctx, OptionAccessToken, "replacement"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setter.SetOption(ctx, OptionAccessToken, ""); adbcStatus(err) != adbc.StatusInvalidArgument {
		t.Fatalf("an empty token: %v", err)
	}
	f.mu.Lock()
	sessions := append([]string(nil), f.sessions...)
	f.mu.Unlock()
	if len(sessions) != 2 || sessions[1] != "Bearer replacement" {
		t.Fatalf("sessions opened with %v", sessions)
	}

	password := newFakeChargewire(t)
	pdb, err := NewDriver(nil).NewDatabaseWithContext(ctx, map[string]string{adbc.OptionKeyURI: password.server.URL,
		adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pdb.Close(ctx) }()
	if c, err := pdb.Open(ctx); err == nil {
		_ = c.Close(ctx)
	} else {
		t.Fatal(err)
	}
	err = pdb.(adbc.GetSetOptionsWithContext).SetOption(ctx, OptionAccessToken, "x")
	if adbcStatus(err) != adbc.StatusInvalidState || !strings.Contains(err.Error(), "signs in another way") {
		t.Fatalf("a password database: %v", err)
	}
}
