// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// invalidTokenOnce makes path answer invalid_token with reason to the first
// request and 200 afterwards, and counts the requests.
func (f *fake) invalidTokenOnce(path, reason string) *atomic.Int32 {
	calls := &atomic.Int32{}
	f.mux.HandleFunc("GET "+path, f.auth(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			writeProblem(w, http.StatusUnauthorized, CodeInvalidToken, reason)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	}))
	return calls
}

// After invalid_token the client signs in again, once, only for a reason
// that allows it: none, token_expired, or one it doesn't know (§4.2, §12).
// After token_revoked, credentials_changed or principal_dropped the refusal
// and its reason reach the caller, and no sign-in is attempted.
func TestInvalidTokenRenewalFollowsTheReason(t *testing.T) {
	for reason, renews := range map[string]bool{
		"": true, ReasonTokenExpired: true, "a_reason_from_a_later_version": true,
		ReasonTokenRevoked: false, ReasonCredentialsChanged: false, ReasonPrincipalDropped: false,
	} {
		f := newFake(t)
		client := f.client(password, 1)
		if err := client.Authenticate(context.Background()); err != nil {
			t.Fatal(err)
		}
		calls := f.invalidTokenOnce("/warp/v1/probe", reason)
		resp, err := client.Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/probe", Idempotent: true})
		f.mu.Lock()
		tokens := f.tokens
		f.mu.Unlock()
		if renews {
			if err != nil || tokens != 2 || calls.Load() != 2 {
				t.Errorf("reason %q: err %v, %d tokens, %d calls; want a renewal and a second call", reason, err, tokens, calls.Load())
			} else {
				resp.Body.Close()
			}
			continue
		}
		p, ok := errors.AsType[*Problem](err)
		if !ok || p.Code != CodeInvalidToken || p.Reason != reason || tokens != 1 || calls.Load() != 1 {
			t.Errorf("reason %q: err %v, %d tokens, %d calls; want the refusal with its reason and no sign-in", reason, err, tokens, calls.Load())
		}
		if ok && !strings.Contains(p.Error(), reason) {
			t.Errorf("reason %q is not in the message %q", reason, p.Error())
		}
	}
}

// A token refused again after a renewal is not renewed a second time.
func TestInvalidTokenRenewsOnce(t *testing.T) {
	f := newFake(t)
	client := f.client(password, 1)
	calls := atomic.Int32{}
	f.mux.HandleFunc("GET /warp/v1/always", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeProblem(w, http.StatusUnauthorized, CodeInvalidToken, ReasonTokenExpired)
	})
	_, err := client.Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/always", Idempotent: true})
	if !HasCode(err, CodeInvalidToken) || calls.Load() != 2 || f.tokens != 2 {
		t.Fatalf("err %v after %d calls and %d tokens; want 2 and 2", err, calls.Load(), f.tokens)
	}
}

// After session_ended the driver replaces the session only for
// token_expired, and only with a new token (§5.6); every other reason,
// including one it doesn't know, stays an error with the reason.
func TestSessionEndedReasonsV1(t *testing.T) {
	for _, reason := range []string{ReasonCredentialsChanged, ReasonPrincipalDropped, ReasonTokenRevoked, "a_later_reason"} {
		f := newFake(t).withSessions()
		session := openTestSession(t, f, true)
		f.mux.HandleFunc("GET /warp/v1/sessions/{id}/metadata/server-info", f.auth(func(w http.ResponseWriter, r *http.Request) {
			writeProblem(w, http.StatusUnauthorized, CodeSessionEnded, reason)
		}))
		_, err := session.ServerInfo(context.Background())
		p, ok := errors.AsType[*Problem](err)
		if !ok || p.Code != CodeSessionEnded || p.Reason != reason || session.Replacements() != 0 || f.tokens != 1 {
			t.Errorf("%s: err %v, replacements %d, tokens %d", reason, err, session.Replacements(), f.tokens)
		}
	}
}

// A session ended because its token expired needs a new token first; a
// token issued elsewhere can't be renewed, so no new session is made.
func TestSessionEndedTokenExpiredWithoutRenewableCredentials(t *testing.T) {
	f := newFake(t).withSessions()
	f.valid["outside"] = true
	session, err := f.client(Credentials{AccessToken: "outside"}, 1).OpenSession(context.Background(), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	f.mux.HandleFunc("GET /warp/v1/sessions/{id}/metadata/server-info", f.auth(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, http.StatusUnauthorized, CodeSessionEnded, ReasonTokenExpired)
	}))
	_, err = session.ServerInfo(context.Background())
	lost, ok := errors.AsType[*SessionLostError](err)
	if !ok || lost.NewID != "" || len(f.created) != 1 || f.tokens != 0 || !strings.Contains(err.Error(), "cannot renew") {
		t.Fatalf("err %v, %d sessions created, %d tokens", err, len(f.created), f.tokens)
	}
}

// An error status without a problem document gets a client_ code and keeps
// its status, which decides how it is handled (§11, §12).
func TestErrorWithoutProblemDocumentHasAClientCode(t *testing.T) {
	f := newFake(t)
	f.mux.HandleFunc("GET /warp/v1/gateway", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "<html>bad gateway</html>", http.StatusBadGateway)
	})
	_, err := f.client(Credentials{AccessToken: "x"}, 1).Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/gateway", Idempotent: true, Anonymous: true})
	p, ok := errors.AsType[*Problem](err)
	if !ok || p.Code != CodeClientHTTPError || p.Status != http.StatusBadGateway || !HasStatus(err, http.StatusBadGateway) {
		t.Fatalf("problem %+v", p)
	}
}

// A statement's error response carries its cancel_reason, and names the
// statement in Warp-Statement-Id (§6.1).
func TestStatementErrorResponse(t *testing.T) {
	f := newFake(t).withSessions()
	f.mux.HandleFunc("POST /warp/v1/sessions/{id}/prepared/{p}/execute", f.auth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Warp-Statement-Id", "q_cancelled")
		w.Header().Set("Content-Type", MediaTypeProblem)
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": CodeStatementCancelled, "status": 409,
			"title": "Statement cancelled", "detail": "an administrator cancelled it", "cancel_reason": CancelReasonKilled})
	}))
	session := openTestSession(t, f, true)
	body, _ := SubmitDocument("SELECT 1", nil)
	_, err := session.Submit(context.Background(), &Submission{Body: body, Prepared: &PreparedRef{SessionID: session.ID(), PreparedID: "p1"}})
	p, ok := errors.AsType[*Problem](err)
	if !ok || p.Code != CodeStatementCancelled || p.CancelReason != CancelReasonKilled || p.StatementID != "q_cancelled" || p.Status != 409 {
		t.Fatalf("problem %+v", p)
	}
}

// A state this driver doesn't know is not terminal: WaitDone keeps polling
// (§12).
func TestUnknownStatementStateIsNotTerminal(t *testing.T) {
	f := newFake(t).withSessions()
	polls := atomic.Int32{}
	f.mux.HandleFunc("GET /warp/v1/sessions/{id}/statements/{q}", f.auth(func(w http.ResponseWriter, r *http.Request) {
		state := "a_state_from_a_later_version"
		if polls.Add(1) >= 3 {
			state = string(StateSucceeded)
		}
		writeJSON(w, http.StatusOK, map[string]any{"statement_id": "q1", "state": state,
			"result": map[string]any{"kind": "update"}})
	}))
	session := openTestSession(t, f, true)
	doc, err := session.WaitDone(context.Background(), StatementRef{SessionID: session.ID(), StatementID: "q1"},
		&StatementDocument{StatementID: "q1", State: "a_state_from_a_later_version"}, time.Second)
	if err != nil || doc.State != StateSucceeded || polls.Load() != 3 {
		t.Fatalf("doc %+v, err %v, %d polls", doc, err, polls.Load())
	}
	for _, state := range []StatementState{"a_state_from_a_later_version", StateQueued, StateRunning} {
		if state.Terminal() {
			t.Errorf("%s is terminal", state)
		}
	}
}

// The session's transaction is read from the server; a value this driver
// doesn't know, or none, is reported as unknown (§5.2, §12).
func TestSessionTransaction(t *testing.T) {
	f := newFake(t)
	var transaction atomic.Value
	f.mux.HandleFunc("GET /warp/v1/sessions/{id}", f.auth(func(w http.ResponseWriter, r *http.Request) {
		doc := map[string]any{"session_id": r.PathValue("id"), "options": map[string]any{}}
		if value := transaction.Load().(string); value != "" {
			doc["transaction"] = value
		}
		writeJSON(w, http.StatusOK, doc)
	}))
	session := openTestSession(t, f, true)
	for value, want := range map[string]string{
		TransactionNone: TransactionNone, TransactionOpen: TransactionOpen, TransactionAborted: TransactionAborted,
		"suspended": TransactionUnknown, "": TransactionUnknown,
	} {
		transaction.Store(value)
		got, err := session.Transaction(context.Background())
		if err != nil || got != want {
			t.Errorf("transaction %q: got %q, %v; want %q", value, got, err, want)
		}
	}
}

// Warp-Query-Tag is a JSON object sent in ASCII, at most 4,096 bytes (§2.5).
func TestNormalizeQueryTag(t *testing.T) {
	for tag, want := range map[string]string{
		`{"job": "nightly-load", "run": 1}`: `{"job":"nightly-load","run":1}`,
		`{"who":"Zoë"}`:                     `{"who":"Zo\u00eb"}`,
		`{"mood":"🚀"}`:                      `{"mood":"\ud83d\ude80"}`,
		"{\"del\":\"a\x7fb\"}":              `{"del":"a\u007fb"}`,
		`{}`:                                `{}`,
	} {
		got, err := NormalizeQueryTag(tag)
		if err != nil || got != want {
			t.Errorf("NormalizeQueryTag(%q) = %q, %v; want %q", tag, got, err, want)
			continue
		}
		for _, c := range []byte(got) {
			if c >= 0x7f || c < 0x20 {
				t.Errorf("%q has byte %#x", got, c)
			}
		}
		var before, after any
		_ = json.Unmarshal([]byte(tag), &before)
		if err := json.Unmarshal([]byte(got), &after); err != nil || !jsonEqual(before, after) {
			t.Errorf("%q does not mean what %q means: %v", got, tag, err)
		}
	}
	for _, tag := range []string{`null`, `[]`, `"text"`, `1`, `{"a":`, `{"a":1} x`, "{\"a\":\"\xff\"}"} {
		if got, err := NormalizeQueryTag(tag); err == nil {
			t.Errorf("NormalizeQueryTag(%q) = %q, want an error", tag, got)
		}
	}
	// The limit counts the bytes as sent, escapes included.
	padding := func(n int) string { return `{"p":"` + strings.Repeat("x", n-len(`{"p":""}`)) + `"}` }
	if got, err := NormalizeQueryTag(padding(MaxQueryTagBytes)); err != nil || len(got) != MaxQueryTagBytes {
		t.Errorf("a tag of exactly %d bytes: %d, %v", MaxQueryTagBytes, len(got), err)
	}
	if _, err := NormalizeQueryTag(padding(MaxQueryTagBytes + 1)); err == nil {
		t.Errorf("a tag of %d bytes was accepted", MaxQueryTagBytes+1)
	}
	short := `{"p":"` + strings.Repeat("é", 800) + `"}` // 1,608 bytes of UTF-8, 4,808 escaped
	if _, err := NormalizeQueryTag(short); err == nil {
		t.Error("a tag over the limit once escaped was accepted")
	}
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// The query tag is sent on submits and ingests as normalized.
func TestQueryTagHeader(t *testing.T) {
	f := newFake(t).withSessions()
	var seen atomic.Value
	f.mux.HandleFunc("POST /warp/v1/sessions/{id}/ingest", f.auth(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Warp-Query-Tag"))
		writeJSON(w, http.StatusOK, map[string]any{"rows_ingested": 0})
	}))
	session := openTestSession(t, f, true)
	tag, _ := NormalizeQueryTag(`{"who":"Zoë"}`)
	if _, err := session.Ingest(context.Background(), IngestRequest{Table: "t", Mode: "append", QueryTag: tag,
		Body: BytesBody(MediaTypeArrowStream, nil)}); err != nil {
		t.Fatal(err)
	}
	if got := seen.Load(); got != `{"who":"Zo\u00eb"}` {
		t.Fatalf("Warp-Query-Tag %q", got)
	}
}

// A partitioned ingest commits with {"part_count": n} (§8.3), and an open
// without an ingest_id is a protocol error with a client_ code.
func TestPartitionedIngestWire(t *testing.T) {
	f := newFake(t).withSessions()
	var commit atomic.Value
	openAnswer := atomic.Value{}
	openAnswer.Store(map[string]any{"ingest_id": "i1", "state": IngestOpen, "parts": []any{}})
	f.mux.HandleFunc("POST /warp/v1/sessions/{id}/ingests", f.auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusCreated, openAnswer.Load())
	}))
	f.mux.HandleFunc("POST /warp/v1/sessions/{id}/ingests/{i}/commit", f.auth(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		commit.Store(body)
		writeJSON(w, http.StatusOK, map[string]any{"rows_ingested": 7})
	}))
	session := openTestSession(t, f, true)
	handle, err := session.OpenIngest(context.Background(), IngestRequest{Table: "t", Mode: "append"})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := session.CommitIngest(context.Background(), handle, 3)
	body, _ := commit.Load().(map[string]any)
	if err != nil || rows != 7 || len(body) != 1 || body["part_count"] != float64(3) {
		t.Fatalf("commit body %v, rows %d, err %v", body, rows, err)
	}
	openAnswer.Store(map[string]any{"state": IngestOpen})
	_, err = session.OpenIngest(context.Background(), IngestRequest{Table: "t", Mode: "append"})
	if !HasCode(err, CodeClientProtocolError) {
		t.Fatalf("an open without an ingest_id gave %v", err)
	}
}

// Only a client that asks gets remote locations, and the header is
// Warp-Result-Locations: remote (§2.5, §6.2).
func TestRemoteLocationsHeader(t *testing.T) {
	for _, remote := range []bool{false, true} {
		sub := &Submission{RemoteLocations: remote}
		got := sub.request(false, true).Header.Get("Warp-Result-Locations")
		if (remote && got != "remote") || (!remote && got != "") {
			t.Errorf("RemoteLocations %v: header %q", remote, got)
		}
	}
}

// A TLS alert certificate_required at the handshake is reported as the
// refusal it stands for, not as an unreachable server (§4.4).
func TestTLSAlertCertificateRequired(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{})
	}))
	server.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL + "/warp")
	client := NewClient(Config{BaseURL: base, HTTPClient: server.Client(), MaxAttempts: 3})
	_, err := client.Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/x", Idempotent: true, Anonymous: true})
	if p, ok := errors.AsType[*Problem](err); !ok || p.Code != CodeCertificateRequired || p.Status != http.StatusUnauthorized {
		t.Fatalf("error %v (%T)", err, err)
	}
}
