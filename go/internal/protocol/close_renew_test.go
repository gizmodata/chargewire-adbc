// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"
)

// withCloses adds the close and cancel endpoints of a session's handles, each
// checking the token like every authenticated endpoint.
func (f *fake) withCloses() *fake {
	noContent := f.auth(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}", noContent)
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}/statements/{q}", noContent)
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}/prepared/{p}", noContent)
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}/ingests/{i}", noContent)
	f.mux.HandleFunc("POST /chargewire/v1/sessions/{id}/statements/{q}/cancel", f.auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]any{"statement_id": r.PathValue("q"), "state": "cancelled"})
	}))
	f.mux.HandleFunc("POST /chargewire/v1/sessions/{id}/cancel", f.auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]any{"cancelled": []string{}})
	}))
	return f
}

// pastRenewPoint makes the client's token one it would replace before
// sending it for a request that does work.
func pastRenewPoint(client *Client) {
	client.tokenMu.Lock()
	defer client.tokenMu.Unlock()
	client.renewAt = time.Now().Add(-time.Second)
}

func (f *fake) signIns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.grants)
}

// cancelsAndCloses sends every cancel and close of session's handles, then
// closes the session, and returns what each returned.
func cancelsAndCloses(t *testing.T, session *Session) map[string]error {
	t.Helper()
	ctx := context.Background()
	id := session.ID()
	body, _ := SubmitDocument("SELECT 1", nil)
	sub := &Submission{Body: body, SQL: "SELECT 1", key: NewIdempotencyKey(), sessionID: id}
	errs := map[string]error{}
	_, errs["recall"] = session.Recall(ctx, sub)
	errs["cancel"] = session.Cancel(ctx, StatementRef{SessionID: id, StatementID: "q1"})
	_, errs["cancel all"] = session.CancelAll(ctx)
	errs["delete statement"] = session.DeleteStatement(ctx, StatementRef{SessionID: id, StatementID: "q1"})
	errs["close prepared"] = session.ClosePrepared(ctx, PreparedRef{SessionID: id, PreparedID: "p1"})
	errs["abort ingest"] = session.AbortIngest(ctx, IngestHandle{SessionID: id, ID: "i1"})
	errs["close session"] = session.Close(ctx)
	return errs
}

// A close or cancel never signs in, not even with a token past the point
// where a request that does work would replace it first: a sign-in wakes a
// stopped server that resumes on sign-in, for nobody (or undoes a person's
// Stop). The token the driver has is sent as it is.
func TestClosesAndCancelsDoNotRenewATokenNearItsExpiry(t *testing.T) {
	f := newFake(t).withSessions().withCloses()
	session := openTestSession(t, f, true)
	pastRenewPoint(session.client)
	for what, err := range cancelsAndCloses(t, session) {
		if err != nil {
			t.Errorf("%s: %v", what, err)
		}
	}
	if n := f.signIns(); n != 1 {
		t.Fatalf("%d sign-ins, want 1 (the first): a close or cancel signed in", n)
	}
}

// A close the server refuses for its token (invalid_token, renewable reason
// or not) is done, without a sign-in: the session is gone or idles out, and
// with it whatever the close was to end. A cancel reports the refusal, still
// without a sign-in. Nothing is replaced either: a lost session is not
// reopened for a close.
func TestCloseRefusedForItsTokenIsDoneWithoutASignIn(t *testing.T) {
	f := newFake(t).withSessions().withCloses()
	session := openTestSession(t, f, true)
	f.revokeAll()
	errs := cancelsAndCloses(t, session)
	for _, what := range []string{"delete statement", "close prepared", "abort ingest", "close session"} {
		if errs[what] != nil {
			t.Errorf("%s: %v, want done", what, errs[what])
		}
	}
	for _, what := range []string{"recall", "cancel", "cancel all"} {
		if !HasCode(errs[what], CodeInvalidToken) {
			t.Errorf("%s: %v, want the invalid_token refusal", what, errs[what])
		}
	}
	if n := f.signIns(); n != 1 {
		t.Fatalf("%d sign-ins, want 1 (the first): a close or cancel signed in", n)
	}
	if len(f.created) != 1 {
		t.Fatalf("%d sessions created, want 1", len(f.created))
	}
}

// Aborting an ingest of a session the server lost (a stopped server answers
// session_not_found) is done, and opens no replacement session, which would
// wake the server.
func TestAbortIngestOfALostSessionOpensNoSession(t *testing.T) {
	f := newFake(t).withSessions()
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}/ingests/{i}", func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, http.StatusNotFound, CodeSessionNotFound, "")
	})
	session := openTestSession(t, f, true)
	pastRenewPoint(session.client)
	if err := session.AbortIngest(context.Background(), IngestHandle{SessionID: session.ID(), ID: "i1"}); err != nil {
		t.Fatalf("AbortIngest: %v", err)
	}
	if len(f.created) != 1 || f.signIns() != 1 {
		t.Fatalf("%d sessions created and %d sign-ins, want 1 and 1", len(f.created), f.signIns())
	}
}

// The positive twin: a statement with a token past its renew point still
// gets a new token first, and one refused for an expired token still signs
// in again once.
func TestStatementsStillRenewTheToken(t *testing.T) {
	f := newFake(t).withSessions().withCloses()
	session := openTestSession(t, f, true)
	pastRenewPoint(session.client)
	if err := submitUpdate(t, session); err != nil {
		t.Fatal(err)
	}
	if n := f.signIns(); n != 2 {
		t.Fatalf("%d sign-ins, want 2 (renewed before the statement)", n)
	}
	f.revokeAll()
	if err := submitUpdate(t, session); err != nil {
		t.Fatal(err)
	}
	if n := f.signIns(); n != 3 {
		t.Fatalf("%d sign-ins, want 3 (renewed after invalid_token)", n)
	}
}

// Closes and cancels are not sent again while a server resumes: a resuming
// server holds nothing they would end, and a close must not wait minutes.
func TestClosesAndCancelsDoNotWaitForAResume(t *testing.T) {
	f := newFake(t).withSessions()
	var mu sync.Mutex
	calls := map[string]int{}
	resuming := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.Method+" "+r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Retry-After", "1")
		writeProblem(w, http.StatusServiceUnavailable, CodeServerResuming, "")
	}
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}", resuming)
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}/statements/{q}", resuming)
	f.mux.HandleFunc("POST /chargewire/v1/sessions/{id}/statements/{q}/cancel", resuming)
	base, _ := url.Parse(f.server.URL + "/chargewire")
	client := NewClient(Config{BaseURL: base, Credentials: password, MaxAttempts: 3, ResumeTimeout: DefaultResumeTimeout})
	clock := installFakeClock(client)
	session, err := client.OpenSession(context.Background(), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ref := StatementRef{SessionID: session.ID(), StatementID: "q1"}
	if err := session.DeleteStatement(ctx, ref); !HasCode(err, CodeServerResuming) {
		t.Errorf("DeleteStatement: %v, want server_resuming", err)
	}
	if err := session.Cancel(ctx, ref); !HasCode(err, CodeServerResuming) {
		t.Errorf("Cancel: %v, want server_resuming", err)
	}
	if err := session.Close(ctx); !HasCode(err, CodeServerResuming) {
		t.Errorf("Close: %v, want server_resuming", err)
	}
	if waits := clock.recorded(); len(waits) != 0 {
		t.Errorf("waited %d times (%s in all) for a resume", len(waits), sum(waits))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 {
		t.Errorf("requests %v, want three", calls)
	}
	for route, n := range calls {
		if n != 1 {
			t.Errorf("%s sent %d times, want once", route, n)
		}
	}
	if n := f.signIns(); n != 1 {
		t.Errorf("%d sign-ins, want 1 (the first)", n)
	}
}

func sum(waits []time.Duration) time.Duration {
	var total time.Duration
	for _, wait := range waits {
		total += wait
	}
	return total
}
