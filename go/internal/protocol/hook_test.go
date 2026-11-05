// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
)

// hookRecorder is a NewSessionHook that runs "SET x = 1" and records the
// sessions it set up, as "<id> replaces <old id>".
type hookRecorder struct {
	mu    sync.Mutex
	calls []string
	// fail, when set, decides whether the hook fails on a session.
	fail func(n *NewSession) error
}

func (h *hookRecorder) hook(ctx context.Context, n *NewSession) error {
	h.mu.Lock()
	h.calls = append(h.calls, n.ID()+" replaces "+n.Replaces())
	fail := h.fail
	h.mu.Unlock()
	if fail != nil {
		if err := fail(n); err != nil {
			return err
		}
	}
	return n.Exec(ctx, "SET x = 1")
}

func (h *hookRecorder) seen() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.calls)
}

// withSessionDelete records the sessions the driver ends.
func (f *fake) withSessionDelete(deleted *[]string) *fake {
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		*deleted = append(*deleted, r.PathValue("id"))
		delete(f.sessions, r.PathValue("id"))
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	return f
}

func (f *fake) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ran)
}

func openHookedSession(t *testing.T, f *fake, h *hookRecorder) *Session {
	t.Helper()
	session, err := f.client(password, 1).OpenSessionWithHook(context.Background(), map[string]any{"catalog": "lake"}, true, h.hook)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

// The hook sets up the first session and every replacement, for each way a
// session is replaced, before the request that found the loss runs there.
func TestNewSessionHookRunsOnEverySession(t *testing.T) {
	for _, how := range []string{"evicted", ReasonTokenExpired, ReasonIdleInTransaction} {
		t.Run(how, func(t *testing.T) {
			f := newFake(t).withSessions()
			h := &hookRecorder{}
			session := openHookedSession(t, f, h)
			if got := f.statements(); !slices.Equal(got, []string{"s1: SET x = 1"}) {
				t.Fatalf("after open the server ran %v, want the hook's statement in s1", got)
			}
			if how == "evicted" {
				f.evict()
			} else {
				f.end("s1", how)
			}
			err := submitSQL(t, session, "INSERT INTO t VALUES (1)")
			if how == ReasonIdleInTransaction {
				// The statement is refused (a transaction was lost), but
				// the replacement is set up for the next one.
				if lost, ok := errors.AsType[*SessionLostError](err); !ok || !lost.Transaction || lost.NewID != "s2" {
					t.Fatalf("error %v, want a lost transaction and replacement s2", err)
				}
				if got := f.statements(); !slices.Equal(got, []string{"s1: SET x = 1", "s2: SET x = 1"}) {
					t.Fatalf("the server ran %v: want the hook's statement in s2, before the refusal", got)
				}
				if err := submitSQL(t, session, "INSERT INTO t VALUES (1)"); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			want := []string{"s1: SET x = 1", "s2: SET x = 1", "s2: INSERT INTO t VALUES (1)"}
			if got := f.statements(); !slices.Equal(got, want) {
				t.Fatalf("the server ran %v, want %v", got, want)
			}
			if got := h.seen(); !slices.Equal(got, []string{"s1 replaces ", "s2 replaces s1"}) {
				t.Fatalf("the hook saw %v", got)
			}
		})
	}
}

// After SQL BEGIN, the statement that finds the session lost is refused, and
// the replacement is set up before the refusal returns.
func TestNewSessionHookRunsBeforeTransactionLossRefusal(t *testing.T) {
	f := newFake(t).withSessions()
	h := &hookRecorder{}
	session := openHookedSession(t, f, h)
	if err := submitSQL(t, session, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	f.evict()
	err := submitSQL(t, session, "INSERT INTO t VALUES (1)")
	lost, ok := errors.AsType[*SessionLostError](err)
	if !ok || !lost.Transaction || lost.NewID != "s2" {
		t.Fatalf("error %v, want a lost transaction and replacement s2", err)
	}
	if got := f.statements(); !slices.Contains(got, "s2: SET x = 1") || slices.Contains(got, "s2: INSERT INTO t VALUES (1)") {
		t.Fatalf("the server ran %v: want the hook's statement in s2 and not the refused one", got)
	}
}

// A hook that fails on a replacement ends it and fails the request, saying the
// next request tries again; the lost transaction is reported once. The next
// request opens another session, sets it up and runs there.
func TestNewSessionHookFailsOnReplacement(t *testing.T) {
	var deleted []string
	f := newFake(t).withSessions().withSessionDelete(&deleted)
	h := &hookRecorder{}
	session := openHookedSession(t, f, h)
	if err := submitSQL(t, session, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("setting refused")
	h.fail = func(n *NewSession) error {
		if n.ID() == "s2" {
			return refusal
		}
		return nil
	}
	f.evict()
	err := submitSQL(t, session, "INSERT INTO t VALUES (1)")
	lost, ok := errors.AsType[*SessionLostError](err)
	setup, isSetup := errors.AsType[*SessionSetupError](err)
	if !ok || !isSetup || lost.NewID != "" || !lost.Transaction || !errors.Is(err, refusal) ||
		setup.SessionID != "s2" || setup.Replaces != "s1" {
		t.Fatalf("error %v, want a lost transaction whose replacement s2 failed to set up", err)
	}
	if !strings.Contains(err.Error(), "next request opens and sets up another session") ||
		strings.Contains(err.Error(), "must be reopened") {
		t.Errorf("the error does not say the next request tries again: %v", err)
	}
	if !slices.Equal(deleted, []string{"s2"}) || session.ID() != "s1" || session.Replacements() != 0 {
		t.Fatalf("deleted %v, current %s, %d replacements: want s2 ended and s1 still current", deleted,
			session.ID(), session.Replacements())
	}
	if err := submitSQL(t, session, "INSERT INTO t VALUES (2)"); err != nil {
		t.Fatalf("the next request: %v", err)
	}
	want := []string{"s1: SET x = 1", "s1: BEGIN", "s3: SET x = 1", "s3: INSERT INTO t VALUES (2)"}
	if got := f.statements(); !slices.Equal(got, want) || session.ID() != "s3" || session.Replacements() != 1 {
		t.Fatalf("the server ran %v in session %s, want %v in s3", got, session.ID(), want)
	}
}

// A hook that fails on the first session fails the open and ends the session.
func TestNewSessionHookFailsOnOpen(t *testing.T) {
	var deleted []string
	f := newFake(t).withSessions().withSessionDelete(&deleted)
	refusal := errors.New("setting refused")
	h := &hookRecorder{fail: func(*NewSession) error { return refusal }}
	_, err := f.client(password, 1).OpenSessionWithHook(context.Background(), nil, true, h.hook)
	setup, ok := errors.AsType[*SessionSetupError](err)
	if !ok || setup.SessionID != "s1" || setup.Replaces != "" || !errors.Is(err, refusal) {
		t.Fatalf("error %v", err)
	}
	if !slices.Equal(deleted, []string{"s1"}) {
		t.Fatalf("deleted %v, want s1", deleted)
	}
}

// Exec goes to the new session only: when that session is gone too, the hook
// fails (rather than replace the session from inside its own setup).
func TestNewSessionExecDoesNotReplace(t *testing.T) {
	var deleted []string
	f := newFake(t).withSessions().withSessionDelete(&deleted)
	h := &hookRecorder{}
	session := openHookedSession(t, f, h)
	h.fail = func(n *NewSession) error {
		if n.ID() == "s2" {
			f.evict() // s2 is lost before the hook's statement
		}
		return nil
	}
	f.evict()
	err := submitUpdate(t, session)
	if !HasCode(err, CodeSessionNotFound) || !errors.As(err, new(*SessionSetupError)) {
		t.Fatalf("error %v", err)
	}
	if got := h.seen(); len(got) != 2 {
		t.Fatalf("the hook ran on %v", got)
	}
}

// The hook can't run statements once it has returned.
func TestNewSessionExecAfterHookReturns(t *testing.T) {
	f := newFake(t).withSessions()
	var kept *NewSession
	if _, err := f.client(password, 1).OpenSessionWithHook(context.Background(), nil, true,
		func(ctx context.Context, n *NewSession) error { kept = n; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := kept.Exec(context.Background(), "SET x = 1"); err == nil {
		t.Fatal("Exec after the hook returned ran")
	}
}

// Concurrent requests that find the session gone set up one replacement, and
// none of them runs before it is set up.
func TestNewSessionHookConcurrentLoss(t *testing.T) {
	f := newFake(t).withSessions()
	h := &hookRecorder{}
	session := openHookedSession(t, f, h)
	f.evict()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := submitUpdate(t, session); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	got := f.statements()
	if len(h.seen()) != 2 || len(got) != 10 || got[1] != "s2: SET x = 1" {
		t.Fatalf("hook calls %v, statements %v", h.seen(), got)
	}
}

// A hook statement that may begin a transaction leaves the driver unsure
// whether one is open, so a request is not run again on a loss.
func TestNewSessionHookTransactionWord(t *testing.T) {
	f := newFake(t).withSessions()
	session, err := f.client(password, 1).OpenSessionWithHook(context.Background(), nil, true,
		func(ctx context.Context, n *NewSession) error { return n.Exec(ctx, "BEGIN") })
	if err != nil {
		t.Fatal(err)
	}
	if !session.MayHaveTransaction() {
		t.Fatal("after a hook ran BEGIN, the driver is sure no transaction is open")
	}
}

// A hook's BEGIN leaves the transaction possibly open even when the session
// document, read right after, says "none" (a stale read, Chargewire up to
// 2.2.0). A hook whose last transaction statement ended it (BEGIN, COMMIT) can
// be confirmed by a read that says "none".
func TestNewSessionHookTransactionWordStaleRead(t *testing.T) {
	for _, statements := range [][]string{{"BEGIN"}, {"BEGIN", "COMMIT"}} {
		ended := len(statements) == 2
		f := newFake(t).withSessions()
		f.staleTxn = !ended
		session, err := f.client(password, 1).OpenSessionWithHook(context.Background(), nil, true,
			func(ctx context.Context, n *NewSession) error {
				for _, sql := range statements {
					if err := n.Exec(ctx, sql); err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		session.RefreshTransaction(context.Background())
		if session.MayHaveTransaction() == ended {
			t.Errorf("hook %v: may have a transaction %v after a \"none\" read", statements, session.MayHaveTransaction())
		}
	}
}
