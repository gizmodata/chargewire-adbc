// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
)

// withSessions adds the session-scoped endpoints the tests use. A session not
// in f.sessions is unknown, as after an idle eviction.
func (f *fake) withSessions() *fake {
	known := func(next http.HandlerFunc) http.HandlerFunc {
		return f.auth(func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			_, ok := f.sessions[r.PathValue("id")]
			f.mu.Unlock()
			if !ok {
				writeProblem(w, http.StatusNotFound, CodeSessionNotFound, "")
				return
			}
			next(w, r)
		})
	}
	f.mux.HandleFunc("GET /warp/v1/sessions/{id}", known(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		options := f.sessions[r.PathValue("id")]
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"session_id": r.PathValue("id"), "options": options})
	}))
	f.mux.HandleFunc("PATCH /warp/v1/sessions/{id}", known(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Options map[string]any `json:"options"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		refused := map[string]any{}
		if _, ok := body.Options["bogus"]; ok {
			refused["bogus"] = map[string]any{"code": CodeUnknownOption, "detail": "no such option"}
		}
		writeJSON(w, http.StatusOK, map[string]any{"options": body.Options, "errors": refused})
	}))
	f.mux.HandleFunc("POST /warp/v1/sessions/{id}/statements", known(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusCreated, map[string]any{"statement_id": "q1", "state": "succeeded",
			"result": map[string]any{"kind": "update", "rows_affected": 1}})
	}))
	f.mux.HandleFunc("POST /warp/v1/sessions/{id}/commit", known(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	return f
}

func (f *fake) evict() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions = map[string]map[string]any{}
}

func openTestSession(t *testing.T, f *fake, reconnect bool) *Session {
	t.Helper()
	session, err := f.client(password, 1).OpenSession(context.Background(), map[string]any{"catalog": "lake"}, reconnect)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func submitUpdate(t *testing.T, session *Session) error {
	t.Helper()
	body, _ := SubmitDocument("INSERT INTO t VALUES (1)", nil)
	resp, err := session.Submit(context.Background(), &Submission{Body: body})
	if err == nil {
		resp.Body.Close()
	}
	return err
}

// A lost session is replaced with the options the driver set, and the
// statement runs in the replacement.
func TestLostSessionIsReplacedWithItsOptions(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	if err := session.SetOptions(context.Background(), map[string]any{"schema": "raw"}); err != nil {
		t.Fatal(err)
	}
	f.evict()
	if err := submitUpdate(t, session); err != nil {
		t.Fatalf("Submit after eviction: %v", err)
	}
	if session.Replacements() != 1 || session.ID() == "s1" {
		t.Fatalf("replacements %d, id %s", session.Replacements(), session.ID())
	}
	replayed := f.created[len(f.created)-1]
	if replayed["catalog"] != "lake" || replayed["schema"] != "raw" {
		t.Fatalf("the replacement got options %v", replayed)
	}
}

func TestLostSessionInsideTransaction(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	session.options["autocommit"] = false
	f.evict()
	err := submitUpdate(t, session)
	lost, ok := errors.AsType[*SessionLostError](err)
	if !ok || lost.NewID == "" || lost.Lost != "open transaction" || !HasCode(err, CodeSessionNotFound) {
		t.Fatalf("error %v", err)
	}
	if err := session.Commit(context.Background()); err != nil {
		t.Fatalf("the replacement session is unusable: %v", err)
	}
}

func TestCommitIsNeverReplayed(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	f.evict()
	err := session.Commit(context.Background())
	if lost, ok := errors.AsType[*SessionLostError](err); !ok || lost.NewID == "" {
		t.Fatalf("error %v", err)
	}
}

func TestLostSessionWithoutReconnect(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, false)
	f.evict()
	err := submitUpdate(t, session)
	if lost, ok := errors.AsType[*SessionLostError](err); !ok || lost.NewID != "" || len(f.created) != 1 {
		t.Fatalf("error %v, %d sessions created", err, len(f.created))
	}
}

// Concurrent requests that all find the session gone replace it once.
func TestConcurrentLossReplacesOnce(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	f.evict()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := session.Get(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if session.Replacements() != 1 {
		t.Fatalf("%d replacements, want 1", session.Replacements())
	}
}

// A session an administrator ended stays ended; one ended because its token
// expired is replaced after a new token.
func TestSessionEndedReasons(t *testing.T) {
	for reason, replaced := range map[string]bool{ReasonKilled: false, ReasonTokenExpired: true} {
		f := newFake(t).withSessions()
		session := openTestSession(t, f, true)
		f.mux.HandleFunc("GET /warp/v1/sessions/{id}/metadata/server-info", f.auth(func(w http.ResponseWriter, r *http.Request) {
			if r.PathValue("id") == "s1" {
				writeProblem(w, http.StatusUnauthorized, CodeSessionEnded, reason)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"server_name": "fake"})
		}))
		_, err := session.ServerInfo(context.Background())
		if replaced {
			if err != nil || session.Replacements() != 1 || f.tokens != 2 {
				t.Errorf("%s: err %v, replacements %d, tokens %d", reason, err, session.Replacements(), f.tokens)
			}
		} else if !HasCode(err, CodeSessionEnded) || session.Replacements() != 0 {
			t.Errorf("%s: err %v, replacements %d", reason, err, session.Replacements())
		}
	}
}

func TestRefusedOptionIsAnError(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	err := session.SetOptions(context.Background(), map[string]any{"bogus": 1})
	optionErr, ok := errors.AsType[*OptionError](err)
	if !ok || optionErr.Key != "bogus" || !HasCode(err, CodeUnknownOption) {
		t.Fatalf("error %v", err)
	}
	if _, kept := session.options["bogus"]; kept {
		t.Fatal("a refused option would be replayed")
	}
}

// Recall needs the request to have been sent and its body in memory.
func TestRecallNeedsASentReplayableRequest(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	sub := &Submission{Body: StreamBody(MediaTypeArrowStream, nil)}
	if _, err := session.Recall(context.Background(), sub); !errors.Is(err, ErrNotReplayable) {
		t.Fatalf("error %v", err)
	}
	body, _ := SubmitDocument("SELECT 1", []any{1})
	sub = &Submission{Body: body}
	resp, err := session.Submit(context.Background(), sub)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !sub.Sent() {
		t.Fatal("a submission the server answered is not marked sent")
	}
	ref, err := session.Recall(context.Background(), sub)
	if err != nil || ref.StatementID != "q1" || ref.SessionID != session.ID() {
		t.Fatalf("Recall: %+v, %v", ref, err)
	}
}
