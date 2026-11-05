// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
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
			reason, ended := f.ended[r.PathValue("id")]
			f.mu.Unlock()
			if ended {
				writeProblem(w, http.StatusUnauthorized, CodeSessionEnded, reason)
				return
			}
			if !ok {
				writeProblem(w, http.StatusNotFound, CodeSessionNotFound, "")
				return
			}
			next(w, r)
		})
	}
	f.mux.HandleFunc("GET /chargewire/v1/sessions/{id}", known(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		options := f.sessions[r.PathValue("id")]
		transaction, known := f.txn[r.PathValue("id")]
		if f.staleTxn {
			transaction, known = f.prevTxn[r.PathValue("id")], true
			if transaction == "" {
				transaction = TransactionNone
			}
		}
		f.mu.Unlock()
		doc := map[string]any{"session_id": r.PathValue("id"), "options": options}
		if known {
			doc["transaction"] = transaction
		}
		writeJSON(w, http.StatusOK, doc)
	}))
	f.mux.HandleFunc("PATCH /chargewire/v1/sessions/{id}", known(func(w http.ResponseWriter, r *http.Request) {
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
	f.mux.HandleFunc("POST /chargewire/v1/sessions/{id}/statements", known(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			SQL string `json:"sql"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		id := r.PathValue("id")
		f.mu.Lock()
		f.ran = append(f.ran, id+": "+body.SQL)
		if f.txn == nil {
			f.txn = map[string]string{}
			f.prevTxn = map[string]string{}
		}
		f.prevTxn[id] = f.txn[id]
		switch word, _, _ := strings.Cut(strings.ToUpper(body.SQL), " "); word {
		case "BEGIN":
			f.txn[id] = TransactionOpen
		case "COMMIT", "ROLLBACK":
			f.txn[id] = TransactionNone
		}
		f.mu.Unlock()
		writeJSON(w, http.StatusCreated, map[string]any{"statement_id": "q1", "state": "succeeded",
			"result": map[string]any{"kind": "update", "rows_affected": 1}})
	}))
	f.mux.HandleFunc("POST /chargewire/v1/sessions/{id}/commit", known(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	return f
}

func (f *fake) evict() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions = map[string]map[string]any{}
}

// end makes the server end session id for reason (session_ended, §5.6).
func (f *fake) end(id, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ended == nil {
		f.ended = map[string]string{}
	}
	f.ended[id] = reason
}

// statementsRun is how many statements the server ran in session id.
func (f *fake) statementsRun(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(slices.DeleteFunc(slices.Clone(f.ran), func(ran string) bool { return !strings.HasPrefix(ran, id+": ") }))
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
	return submitSQL(t, session, "INSERT INTO t VALUES (1)")
}

func submitSQL(t *testing.T, session *Session, sql string) error {
	t.Helper()
	body, _ := SubmitDocument(sql, nil)
	resp, err := session.Submit(context.Background(), &Submission{Body: body, SQL: sql})
	if err == nil {
		resp.Body.Close()
	}
	return err
}

// assertTransactionLost checks that err reports a session lost with a
// transaction in it, replaced by a new session (or not, without reconnect),
// and that nothing ran in the replacement.
func assertTransactionLost(t *testing.T, f *fake, session *Session, err error, code string, replaced bool) {
	t.Helper()
	lost, ok := errors.AsType[*SessionLostError](err)
	if !ok || !lost.Transaction || lost.Lost != "open transaction" || !HasCode(err, code) {
		t.Fatalf("error %v, want a lost transaction (%s)", err, code)
	}
	if replaced != (lost.NewID != "") || replaced != (session.Replacements() == 1) {
		t.Fatalf("replaced: new id %q, %d replacements; want replaced=%v", lost.NewID, session.Replacements(), replaced)
	}
	if replaced {
		if n := f.statementsRun(lost.NewID); n != 0 {
			t.Fatalf("%d statements ran in the replacement session: the lost transaction's statement ran again in autocommit", n)
		}
		if !strings.Contains(err.Error(), "did not run again") {
			t.Errorf("the error does not say the request did not run again: %v", err)
		}
	}
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
		f.mux.HandleFunc("GET /chargewire/v1/sessions/{id}/metadata/server-info", f.auth(func(w http.ResponseWriter, r *http.Request) {
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

// A session the driver closed is never replaced, even when a request still
// uses it.
func TestClosedSessionIsNotReplaced(t *testing.T) {
	f := newFake(t).withSessions()
	f.mux.HandleFunc("DELETE /chargewire/v1/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	session := openTestSession(t, f, true)
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.evict()
	err := submitUpdate(t, session)
	if !errors.Is(err, errSessionClosed) || len(f.created) != 1 {
		t.Fatalf("error %v, %d sessions created", err, len(f.created))
	}
}

func TestCancelAll(t *testing.T) {
	f := newFake(t).withSessions()
	f.mux.HandleFunc("POST /chargewire/v1/sessions/{id}/cancel", f.auth(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]any{"cancelled": []string{"q1", "q2"}})
	}))
	session := openTestSession(t, f, true)
	cancelled, err := session.CancelAll(context.Background())
	if err != nil || len(cancelled) != 2 || cancelled[0] != "q1" {
		t.Fatalf("CancelAll: %v, %v", cancelled, err)
	}
}

// A session the server ended for idling in a transaction (§5.6, §5.7) is
// replaced with the same token, and the statement that found it ended fails
// rather than run again in autocommit: the transaction was begun in SQL,
// which the driver did not track, and its earlier statements were rolled
// back. The next statement runs in the replacement. Without reconnect,
// nothing is replaced.
func TestSessionEndedIdleInTransaction(t *testing.T) {
	for _, reconnect := range []bool{true, false} {
		f := newFake(t).withSessions()
		session := openTestSession(t, f, reconnect)
		for _, sql := range []string{"BEGIN", "INSERT INTO t VALUES (1)"} {
			if err := submitSQL(t, session, sql); err != nil {
				t.Fatal(err)
			}
		}
		f.end("s1", ReasonIdleInTransaction)
		err := submitSQL(t, session, "INSERT INTO t VALUES (2)")
		assertTransactionLost(t, f, session, err, CodeSessionEnded, reconnect)
		if f.tokens != 1 {
			t.Fatalf("%d tokens: idle_in_transaction needs no new token (§5.6)", f.tokens)
		}
		if !reconnect {
			continue
		}
		if err := submitSQL(t, session, "INSERT INTO t VALUES (3)"); err != nil {
			t.Fatalf("the replacement session is unusable: %v", err)
		}
		if f.statementsRun(session.ID()) != 1 || session.ID() == "s1" {
			t.Fatalf("the next statement did not run in the replacement: %v", f.ran)
		}
	}
}

// Even when the driver believes no transaction is open (autocommit on, no
// BEGIN seen), session_ended with idle_in_transaction says one was.
func TestSessionEndedIdleInTransactionAlwaysHadOne(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	f.end("s1", ReasonIdleInTransaction)
	assertTransactionLost(t, f, session, submitUpdate(t, session), CodeSessionEnded, true)
}

// A session lost (session_not_found: evicted, or the server restarted) after
// SQL BEGIN had a transaction open, which the server rolled back: the next
// statement must not run again in the replacement, in autocommit.
func TestLostSessionAfterSQLBegin(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	for _, sql := range []string{"begin transaction", "INSERT INTO t VALUES (1)"} {
		if err := submitSQL(t, session, sql); err != nil {
			t.Fatal(err)
		}
	}
	f.evict()
	assertTransactionLost(t, f, session, submitSQL(t, session, "INSERT INTO t VALUES (2)"), CodeSessionNotFound, true)
	if err := submitUpdate(t, session); err != nil {
		t.Fatalf("the replacement session is unusable: %v", err)
	}
}

// A session-wide request (metadata, options, preparing) that finds the
// session lost with a transaction in it fails too: run again quietly, it would
// hide the loss, and the caller's next statements would run outside the
// transaction it thinks it is in.
func TestSessionRequestReportsLostTransaction(t *testing.T) {
	for _, how := range []string{"sql", "autocommit"} {
		f := newFake(t).withSessions()
		session := openTestSession(t, f, true)
		if how == "sql" {
			if err := submitSQL(t, session, "BEGIN"); err != nil {
				t.Fatal(err)
			}
		} else {
			session.options["autocommit"] = false
		}
		f.evict()
		_, err := session.Get(context.Background())
		assertTransactionLost(t, f, session, err, CodeSessionNotFound, true)
	}
}

// After COMMIT, once the session document says no transaction is open, a lost
// session is replaced and the statement runs again, as outside any
// transaction. Until the driver has read it, it does not run again.
func TestLostSessionAfterCommitRunsAgainOnceConfirmed(t *testing.T) {
	for _, confirmed := range []bool{true, false} {
		f := newFake(t).withSessions()
		session := openTestSession(t, f, true)
		for _, sql := range []string{"BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"} {
			if err := submitSQL(t, session, sql); err != nil {
				t.Fatal(err)
			}
		}
		if !session.TransactionUnconfirmed() {
			t.Fatal("after COMMIT the driver claims to know the transaction without asking")
		}
		if confirmed {
			session.RefreshTransaction(context.Background())
			if session.MayHaveTransaction() {
				t.Fatal("the session document said none, but the driver still thinks a transaction may be open")
			}
		}
		f.evict()
		err := submitSQL(t, session, "INSERT INTO t VALUES (2)")
		if confirmed {
			if err != nil || f.statementsRun(session.ID()) != 1 {
				t.Fatalf("confirmed no transaction: err %v, ran %v", err, f.ran)
			}
			continue
		}
		assertTransactionLost(t, f, session, err, CodeSessionNotFound, true)
	}
}

// RefreshTransaction never replaces a lost session: the next request finds
// the loss and reports it.
func TestRefreshTransactionDoesNotReplace(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	if err := submitSQL(t, session, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	f.evict()
	session.RefreshTransaction(context.Background())
	if session.Replacements() != 0 || !session.MayHaveTransaction() {
		t.Fatalf("replacements %d, may have transaction %v", session.Replacements(), session.MayHaveTransaction())
	}
	assertTransactionLost(t, f, session, submitUpdate(t, session), CodeSessionNotFound, true)
}

// Statements without a transaction word leave a session without a
// transaction as it was; any of them makes the driver unsure. In an open
// transaction, only the words that can end it do.
func TestTransactionWords(t *testing.T) {
	for sql, begins := range map[string]bool{
		"BEGIN": true, "begin transaction": true, "START TRANSACTION": true, "SET autocommit = 0": true,
		"EXECUTE p": true, "SELECT 1; BEGIN": true, "SELECT 'BEGIN'": true, "/* begin */ SELECT 1": true,
		"SELECT 1": false, "INSERT INTO t SELECT start_date, ended FROM s": false,
		"SELECT CASE WHEN x THEN 1 END": false, "COMMIT": false, "CREATE TABLE beginnings (v INT)": false,
	} {
		if got := mayBeginTransaction(sql); got != begins {
			t.Errorf("mayBeginTransaction(%q) = %v, want %v", sql, got, begins)
		}
	}
	for sql, ends := range map[string]bool{
		"COMMIT": true, "rollback": true, "END": true, "ABORT": true, "SELECT CASE WHEN x THEN 1 END": true,
		"INSERT INTO t VALUES (1)": false, "SELECT committed FROM t": false,
	} {
		if got := mayEndTransaction(sql); got != ends {
			t.Errorf("mayEndTransaction(%q) = %v, want %v", sql, got, ends)
		}
	}
}

// A Chargewire server up to 2.2.0 answers a statement without rows before it
// records the session's transaction, so the session document read right after
// BEGIN can still say "none". That read must not let the next statement run
// again, in autocommit, on a replacement session: the transaction it belonged
// to was rolled back with the lost session. Each text here can open a
// transaction, alone or with other statements (one ending in COMMIT
// included: CASE ... END makes the last word no proof that it ended).
func TestStaleNoneAfterBeginDoesNotLetAStatementRunAgain(t *testing.T) {
	for _, opening := range []string{"BEGIN", "start transaction", "BEGIN; INSERT INTO t VALUES (1)",
		"BEGIN; INSERT INTO t VALUES (1); COMMIT", "SET autocommit = false", "EXECUTE begin_it"} {
		for _, read := range []string{"refresh", "get"} {
			f := newFake(t).withSessions()
			f.staleTxn = true
			session := openTestSession(t, f, true)
			if err := submitSQL(t, session, opening); err != nil {
				t.Fatal(err)
			}
			if read == "refresh" {
				session.RefreshTransaction(context.Background())
			} else if state, err := session.Transaction(context.Background()); err != nil || state != TransactionNone {
				t.Fatalf("%q: the stale session document says %q (%v), want %q", opening, state, err, TransactionNone)
			}
			if !session.MayHaveTransaction() {
				t.Errorf("%q, %s: a stale \"none\" read made the driver sure no transaction is open", opening, read)
			}
			if err := submitSQL(t, session, "INSERT INTO t VALUES (1)"); err != nil {
				t.Fatal(err)
			}
			f.evict()
			assertTransactionLost(t, f, session, submitSQL(t, session, "INSERT INTO t VALUES (2)"), CodeSessionNotFound, true)
		}
	}
}

// After COMMIT a stale read says "open": the driver refuses to run a
// statement again (the safe side) until a later ending statement and a read
// after it say "none".
func TestStaleOpenAfterCommitRefusesUntilConfirmed(t *testing.T) {
	f := newFake(t).withSessions()
	f.staleTxn = true
	session := openTestSession(t, f, true)
	for _, sql := range []string{"BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"} {
		if err := submitSQL(t, session, sql); err != nil {
			t.Fatal(err)
		}
		session.RefreshTransaction(context.Background())
	}
	if !session.MayHaveTransaction() {
		t.Fatal("a stale \"open\" read after COMMIT made the driver sure no transaction is open")
	}
	// ROLLBACK outside a transaction; the document now says "none" (as of
	// after COMMIT), which an ending statement may trust.
	if err := submitSQL(t, session, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	session.RefreshTransaction(context.Background())
	if session.MayHaveTransaction() {
		t.Fatal("\"none\" read after an ending statement was not trusted")
	}
	f.evict()
	if err := submitSQL(t, session, "INSERT INTO t VALUES (2)"); err != nil || f.statementsRun(session.ID()) != 1 {
		t.Fatalf("confirmed no transaction: err %v, ran %v", err, f.ran)
	}
}

// assertStateLost checks that err reports a session lost after SQL changed
// its state (USE, SET, ...), replaced, and that the request did not run in the
// replacement, where it would have run without that state.
func assertStateLost(t *testing.T, f *fake, session *Session, err error, sql string) {
	t.Helper()
	lost, ok := errors.AsType[*SessionLostError](err)
	// EXECUTE is a transaction word too, so Transaction may be set as well.
	if !ok || !lost.State || lost.Transaction != mayBeginTransaction(sql) || lost.NewID == "" || !HasCode(err, CodeSessionNotFound) {
		t.Fatalf("after %q: error %v, want a session lost with its SQL state", sql, err)
	}
	if n := f.statementsRun(lost.NewID); n != 0 {
		t.Fatalf("after %q: %d statements ran in the replacement session without the old session's state", sql, n)
	}
	for _, want := range []string{"settings and USE", "did not run again"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("after %q: the error does not say %q: %v", sql, want, err)
		}
	}
}

// A statement that changes the session's state in SQL (USE, SET, RESET,
// PRAGMA, by its first word) makes a lost session's requests fail rather than
// run again: in the replacement, an INSERT INTO t would land in another
// schema's table, or run with other settings, without an error. The next
// request runs in the replacement. The twins below change no session state
// (SET inside UPDATE, a word in a literal) and run again.
func TestLostSessionAfterSQLStateIsNotRunAgain(t *testing.T) {
	for _, sql := range []string{"USE other.s2", "SET threads = 4", "reset threads", "PRAGMA enable_profiling",
		"/* why */ use s2", "-- note\n  SET search_path = 'x'", "SELECT 1; SET x = 1", "SET VARIABLE v = 1",
		"EXECUTE set_it"} {
		f := newFake(t).withSessions()
		session := openTestSession(t, f, true)
		if err := submitSQL(t, session, sql); err != nil {
			t.Fatal(err)
		}
		f.evict()
		assertStateLost(t, f, session, submitSQL(t, session, "INSERT INTO t VALUES (2)"), sql)
		if err := submitSQL(t, session, "INSERT INTO t VALUES (3)"); err != nil || f.statementsRun(session.ID()) != 1 {
			t.Fatalf("after %q: the next statement did not run in the replacement: %v, %v", sql, err, f.ran)
		}
	}
	for _, sql := range []string{"UPDATE t SET a = 1", "SELECT 'USE x'", "INSERT INTO settings VALUES (1)",
		"CREATE TABLE pragma_log (v INT)"} {
		f := newFake(t).withSessions()
		session := openTestSession(t, f, true)
		if err := submitSQL(t, session, sql); err != nil {
			t.Fatal(err)
		}
		f.evict()
		if err := submitSQL(t, session, "INSERT INTO t VALUES (2)"); err != nil || f.statementsRun(session.ID()) != 1 {
			t.Fatalf("after %q: err %v, ran %v; want the statement run again in the replacement", sql, err, f.ran)
		}
	}
}

// A session-wide request (metadata, options) is not run again either.
func TestLostSessionAfterSQLStateRefusesSessionRequests(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	if err := submitSQL(t, session, "USE other"); err != nil {
		t.Fatal(err)
	}
	f.evict()
	_, err := session.Get(context.Background())
	if lost, ok := errors.AsType[*SessionLostError](err); !ok || !lost.State || lost.NewID == "" {
		t.Fatalf("error %v, want a session lost with its SQL state", err)
	}
}

// A statement whose state the caller restores in every session (through the
// new-session hook) doesn't stop a request from running again.
func TestLostSessionAfterRestoredSQLStateRunsAgain(t *testing.T) {
	f := newFake(t).withSessions()
	session := openTestSession(t, f, true)
	body, _ := SubmitDocument("SET SESSION hardcharger.query_tag = '{}'", nil)
	resp, err := session.Submit(context.Background(), &Submission{Body: body, SQL: "SET SESSION hardcharger.query_tag = '{}'",
		StateRestored: true})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	f.evict()
	if err := submitSQL(t, session, "INSERT INTO t VALUES (2)"); err != nil || f.statementsRun(session.ID()) != 1 {
		t.Fatalf("err %v, ran %v; want the statement run again in the replacement", err, f.ran)
	}
}
