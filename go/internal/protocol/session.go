// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Scope says what a request may do when the server no longer has its session
// (§5.6): the server never recreates a session, so the client decides.
type Scope int

const (
	// ScopeSession requests act on the session as a whole (options,
	// metadata, preparing). They run again on a replacement session, unless
	// a transaction may have been open in the lost one: then they fail, so
	// that the caller learns its transaction is gone before running the rest
	// of it in the replacement.
	ScopeSession Scope = iota
	// ScopeStatement requests run SQL. They run again on a replacement
	// session only when the driver knows no transaction was open in the lost
	// one: inside one, the work before them is gone (rolled back), and running
	// the rest would commit half a transaction. Neither kind runs again when
	// SQL changed the lost session's state (USE, SET, RESET, PRAGMA): the
	// replacement doesn't have it (see changesSessionState).
	ScopeStatement
	// ScopeHandle requests name something that lived in the lost session: a
	// statement, a prepared statement, the open transaction. They fail.
	ScopeHandle
)

// Session is a server session (§5) and the options the driver set on it. When
// the server reports the session gone, Session opens a replacement with the
// same options (when allowed to) and says so, rather than hiding the loss.
type Session struct {
	client    *Client
	reconnect bool
	// hook sets up every session this Session creates, before any other
	// request uses it (OpenSessionWithHook).
	hook NewSessionHook

	mu           sync.Mutex
	id           string
	options      map[string]any // the options set through this Session, replayed on a replacement
	replacements int
	closed       bool // Close or Detach was called: never replace it
	attached     bool // AttachSession: another connection created it
	// pending: the session is not open on the server yet (NewLazySession);
	// the first request that does work opens it (open).
	pending bool
	// tx is what the driver knows of the current session's transaction,
	// which SQL (BEGIN) changes without the driver's say: it decides whether
	// a request may run again on a replacement session.
	tx txKnowledge
	// lostTx says, for each session id that was replaced, whether a
	// transaction may have been open in it, for requests that find the
	// session gone after another request replaced it.
	lostTx map[string]bool
	// sqlState says SQL changed the current session's state (USE, SET,
	// RESET, PRAGMA) in a way the driver doesn't restore in a replacement:
	// a request that finds the session gone then fails rather than run again
	// without that state. lostState is the same for each replaced session id.
	sqlState  bool
	lostState map[string]bool
}

// txKnowledge is what the driver knows of a session's transaction (§5.5).
// The server tells it only in the session document (§5.1, §5.2), so the
// driver reads that document after a statement that may have ended a
// transaction, and until it has, assumes one may be open.
//
// That read can be stale. A Chargewire server up to 2.2.0 answers a
// statement without rows on the streamed path as soon as its outcome is
// known, and records the session's transaction in the document slightly
// after, so a read right after BEGIN can say "none" and one right after
// COMMIT can say "open". The rule (nextTx) therefore never lets a read clear
// what a statement that can open a transaction implies:
//
//   - A statement whose text can open a transaction, or change how one
//     opens (BEGIN, START, TRANSACTION, AUTOCOMMIT, EXECUTE of a prepared
//     statement), leaves one possibly open (txOpen), and the driver doesn't
//     read the document after it: a "none" there may be stale.
//   - A statement whose text can end one (COMMIT, ROLLBACK, END, ABORT) and
//     has no opening word leaves the driver unsure (txMaybe), and a read
//     after it is trusted: a stale read there can only say "open", which
//     errs on the safe side (a request is not run again).
//   - A text with both kinds (BEGIN; ...; COMMIT) counts as opening, whatever
//     their order: END closes CASE expressions too, so an ending word last
//     doesn't prove the transaction ended. The next statement that ends one,
//     and a read after it that says "none", clear it.
//
// So a transaction is assumed open for longer than it is, never shorter; the
// cost is that a request is refused (SessionLostError.Transaction) rather
// than run again when a session is lost after such a statement.
type txKnowledge int

const (
	// txNone: no transaction is open; the server said so after a statement
	// that could end one (or of a new session), and no statement that could
	// open one has run since.
	txNone txKnowledge = iota
	// txMaybe: a statement that could have ended a transaction ran since the
	// server last said; RefreshTransaction asks.
	txMaybe
	// txOpen: a transaction is, or may be, open: a statement that could have
	// opened one ran, or the server said one is open (or aborted, or a value
	// this driver doesn't know). Only a statement that can end it, and a read
	// after that, clear this.
	txOpen
)

// nextTx is what the driver knows of the transaction after statement sql
// ran, given what it knew before (see txKnowledge for the rule).
func nextTx(tx txKnowledge, sql string) txKnowledge {
	switch {
	case mayBeginTransaction(sql):
		return txOpen
	case tx == txOpen && mayEndTransaction(sql):
		return txMaybe
	}
	return tx
}

// afterHook is what the driver knows of a new session's transaction: what
// its document said when it was created, unless the new-session hook's
// statements left one possibly open, or left the driver unsure.
func afterHook(created, hook txKnowledge) txKnowledge {
	return max(created, hook)
}

// txFromDocument is what a session document says of the transaction. fresh
// is a session just created, in which no SQL has run: one that doesn't say
// (a server from before the field) has none, unless autocommit is off, which
// inTransaction covers.
func txFromDocument(doc *SessionDocument, fresh bool) txKnowledge {
	switch doc.TransactionState() {
	case TransactionNone:
		return txNone
	case TransactionOpen, TransactionAborted:
		return txOpen
	}
	if fresh && doc.Transaction == "" {
		return txNone
	}
	return txMaybe
}

// OpenSession creates a session (§5.1). With reconnect, a session the server
// later loses is replaced by a new one with the same options.
func (c *Client) OpenSession(ctx context.Context, options map[string]any, reconnect bool) (*Session, error) {
	return c.OpenSessionWithHook(ctx, options, reconnect, nil)
}

// OpenSessionWithHook is OpenSession with a hook that sets up the session,
// and every session that replaces it, before any other request uses it. When
// the hook fails on the first session, the session is ended and the error is
// a *SessionSetupError.
func (c *Client) OpenSessionWithHook(ctx context.Context, options map[string]any, reconnect bool, hook NewSessionHook) (*Session, error) {
	s := &Session{client: c, reconnect: reconnect, options: maps.Clone(options), hook: hook}
	if s.options == nil {
		s.options = map[string]any{}
	}
	doc, err := s.create(ctx)
	if err != nil {
		return nil, err
	}
	hookTx, err := c.setUp(ctx, hook, doc.SessionID, "")
	if err != nil {
		return nil, err
	}
	s.id = doc.SessionID
	s.tx = afterHook(txFromDocument(doc, true), hookTx)
	return s, nil
}

// NewLazySession is OpenSessionWithHook without a request: the session opens
// on the server, signing in first when the client has no token, at the first
// request that does work (a statement, preparing, metadata, an ingest, a
// commit or rollback, reading the session). Until then nothing is sent:
// options set on it are kept and sent with the request that opens it,
// closing or detaching it sends nothing, and there is nothing to cancel. For
// a server whose discovery said it is stopped (Discovery.Stopped): a
// connection opened for nothing, such as a pool's refill, then doesn't start
// it (§3.1). A failure to open (wrong credentials, a server that can't be
// reached) is the error of that first request, and the next one tries again.
func (c *Client) NewLazySession(options map[string]any, reconnect bool, hook NewSessionHook) *Session {
	s := &Session{client: c, reconnect: reconnect, options: maps.Clone(options), hook: hook, pending: true}
	if s.options == nil {
		s.options = map[string]any{}
	}
	return s
}

// Open opens a session NewLazySession made, unless it is open already.
func (s *Session) Open(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.pending {
		return nil
	}
	if s.closed {
		return errSessionClosed
	}
	doc, err := s.create(ctx)
	if err != nil {
		return err
	}
	hookTx, err := s.client.setUp(ctx, s.hook, doc.SessionID, "")
	if err != nil {
		return err
	}
	s.id = doc.SessionID
	s.tx = afterHook(txFromDocument(doc, true), hookTx)
	s.pending = false
	return nil
}

// Pending reports a session NewLazySession made that is not open yet.
func (s *Session) Pending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending
}

// AttachSession uses the existing session id instead of creating one: it reads
// the session (§5.2) to check that the server has it for this principal. A
// session of another principal is session_not_found too (§9.2). An attached
// session is never replaced: the driver doesn't know what its creator set on
// it, and replacing it would hide that the session the caller named is gone.
func (c *Client) AttachSession(ctx context.Context, id string) (*Session, error) {
	if id == "" {
		return nil, errors.New("no session id to attach to")
	}
	doc := &SessionDocument{}
	req := &Request{Method: http.MethodGet, Path: sessionPath(id), Idempotent: true}
	if _, err := c.doJSON(ctx, req, doc); err != nil {
		return nil, err
	}
	if doc.SessionID != "" && doc.SessionID != id {
		return nil, fmt.Errorf("the server answered for session %s with session %s", id, doc.SessionID)
	}
	options := maps.Clone(doc.Options)
	if options == nil {
		options = map[string]any{}
	}
	return &Session{client: c, id: id, options: options, attached: true, tx: txFromDocument(doc, false)}, nil
}

// Attached reports whether the session came from AttachSession.
func (s *Session) Attached() bool {
	return s.attached
}

func (s *Session) create(ctx context.Context) (*SessionDocument, error) {
	body, err := JSONBody(map[string]any{"options": s.options})
	if err != nil {
		return nil, err
	}
	// Creating a session is not idempotent, but a retry after a lost answer
	// only leaves an unused session behind, which the server evicts when idle.
	doc := &SessionDocument{}
	req := &Request{Method: http.MethodPost, Path: "/v1/sessions", Body: body, Idempotent: true}
	if _, err := s.client.doJSON(ctx, req, doc); err != nil {
		return nil, err
	}
	if doc.SessionID == "" {
		return nil, errors.New("the server created a session without an id")
	}
	return doc, nil
}

// ID is the current session id, "" while the session is pending (Open).
func (s *Session) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

// Replacements is how many times the session was lost and replaced.
func (s *Session) Replacements() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replacements
}

// InTransaction reports whether the driver turned autocommit off.
// Transaction asks the server, which sees SQL BEGIN too; MayHaveTransaction
// is what decides whether a request runs again on a replacement session.
func (s *Session) InTransaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inTransaction()
}

func (s *Session) inTransaction() bool {
	autocommit, ok := s.options["autocommit"].(bool)
	return ok && !autocommit
}

// MayHaveTransaction reports whether a transaction may be open in the
// current session: autocommit is off, the server said one is open, a
// statement that could have begun one (SQL BEGIN) ran and no statement that
// could end it has since, or one that could end it ran and the server hasn't
// said none is open since (see txKnowledge). A request that finds the session
// gone runs again on a replacement only when this is false.
func (s *Session) MayHaveTransaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mayHaveTransaction()
}

func (s *Session) mayHaveTransaction() bool {
	return s.tx != txNone || s.inTransaction()
}

// TransactionUnconfirmed reports whether a statement that could have ended a
// transaction ran since the server last said what the session's transaction
// is; RefreshTransaction asks. After one that could have begun a transaction
// the driver doesn't ask: the answer may be stale (see txKnowledge).
func (s *Session) TransactionUnconfirmed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tx == txMaybe
}

// noteStatement records that SQL ran in session id (the server took the
// request): a statement that could begin a transaction leaves one possibly
// open, and one that could end it leaves the driver unsure until it reads the
// session document (nextTx).
//
// A statement that changes the session's state in SQL (changesSessionState)
// marks it, unless restored says the caller sets that state up again in every
// new session (with its new-session hook).
func (s *Session) noteStatement(id, sql string, restored bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id != id {
		return
	}
	s.tx = nextTx(s.tx, sql)
	if !restored && changesSessionState(sql) {
		s.sqlState = true
	}
}

// stateWords are the first words of SQL statements that change the session's
// state, which a replacement session doesn't have: USE (the current catalog
// and schema), SET and RESET (settings, variables), PRAGMA, and EXECUTE (of a
// statement prepared in SQL, which may be one of them). The driver doesn't
// restore what they set; the session options it replays (catalog, schema,
// autocommit) are set through the protocol, not SQL.
var stateWords = []string{"USE", "SET", "RESET", "PRAGMA", "EXECUTE"}

// changesSessionState reports whether a statement of sql (split at every
// semicolon, literals and comments included, which can only find more) starts
// with one of stateWords, after any comments. The first word only: UPDATE t
// SET a = 1 changes no session state.
func changesSessionState(sql string) bool {
	for _, part := range strings.Split(sql, ";") {
		word := firstWord(part)
		for _, w := range stateWords {
			if strings.EqualFold(word, w) {
				return true
			}
		}
	}
	return false
}

// firstWord is the first word of a statement, after white space, comments
// (-- to the end of the line, /* ... */) and opening parentheses.
func firstWord(sql string) string {
	for {
		sql = strings.TrimLeft(sql, " \t\r\n\f(")
		switch {
		case strings.HasPrefix(sql, "--"):
			_, rest, found := strings.Cut(sql, "\n")
			if !found {
				return ""
			}
			sql = rest
		case strings.HasPrefix(sql, "/*"):
			_, rest, found := strings.Cut(sql[2:], "*/")
			if !found {
				return ""
			}
			sql = rest
		default:
			end := strings.IndexFunc(sql, func(r rune) bool {
				return !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z')
			})
			if end < 0 {
				return sql
			}
			return sql[:end]
		}
	}
}

// observe records the transaction a session document of session id says,
// except that it never clears a transaction a statement that could open one
// left possibly open (txOpen): the document may not show that statement's
// effect yet (see txKnowledge). Only after a statement that could end the
// transaction (txMaybe) does a "none" count.
func (s *Session) observe(id string, doc *SessionDocument) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id == id && s.tx != txOpen {
		s.tx = txFromDocument(doc, false)
	}
}

// RefreshTransaction reads the session document (§5.2) when a statement that
// could have ended a transaction ran since the driver last did, so
// that it knows whether one is open before the session can be lost. It never
// replaces a lost session: the next request finds the loss and reports it. It
// is best effort; on any failure the driver stays unsure, which only means a
// request is not run again on a replacement session.
func (s *Session) RefreshTransaction(ctx context.Context) {
	s.mu.Lock()
	id, unsure := s.id, s.tx == txMaybe
	s.mu.Unlock()
	if !unsure {
		return
	}
	doc := &SessionDocument{}
	req := &Request{Method: http.MethodGet, Path: sessionPath(id), Idempotent: true, MaxAttempts: 1}
	if _, err := s.client.doJSON(ctx, req, doc); err != nil {
		return
	}
	s.observe(id, doc)
}

// Words of SQL statements that can begin a transaction or change how one
// begins (BEGIN, START TRANSACTION, an engine's SET autocommit or EXECUTE of a
// prepared BEGIN), and those that can end one (COMMIT, ROLLBACK, END, ABORT).
// A statement that has none of them as a word anywhere in its text, comments
// and literals included, leaves the session's transaction as it was, except
// that a failure may abort an open one, which is still a transaction the
// driver treats as open. Matching an opening word where none was meant (a
// column named start) keeps a transaction assumed open until a statement that
// can end one and a read after it; matching an ending word costs one session
// read.
var (
	beginWords = []string{"BEGIN", "START", "TRANSACTION", "AUTOCOMMIT", "EXECUTE"}
	endWords   = []string{"COMMIT", "ROLLBACK", "END", "ABORT"}
)

func mayBeginTransaction(sql string) bool { return hasWord(sql, beginWords) }
func mayEndTransaction(sql string) bool   { return hasWord(sql, endWords) }

// hasWord reports whether sql has one of words as a whole word (letters,
// digits and underscores), in any case.
func hasWord(sql string, words []string) bool {
	for _, word := range strings.FieldsFunc(sql, func(r rune) bool {
		return !(r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z')
	}) {
		for _, w := range words {
			if strings.EqualFold(word, w) {
				return true
			}
		}
	}
	return false
}

func sessionPath(id string) string {
	return "/v1/sessions/" + url.PathEscape(id)
}

// SessionLostError is an operation that could not run because the server no
// longer has the session it needed (§5.6).
type SessionLostError struct {
	OldID string
	// NewID is the replacement session, or empty when none was opened.
	NewID string
	// What was lost with the session: "transaction", "statement", ...
	Lost string
	// Transaction says a transaction was, or may have been, open in the lost
	// session: its work was rolled back, so the request was not run again
	// on the replacement session, where it would have committed on its own.
	Transaction bool
	// State says statements in the lost session changed its state in SQL
	// (USE, SET, RESET, PRAGMA), which the replacement session doesn't have,
	// so the request was not run again there.
	State bool
	Cause error
}

func (e *SessionLostError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "the server no longer has session %s (%v)", e.OldID, e.Cause)
	if e.Lost != "" {
		fmt.Fprintf(&b, "; the %s that lived in it is gone", e.Lost)
	}
	if e.Transaction {
		if e.Lost != "open transaction" {
			b.WriteString("; so is the open transaction")
		}
		b.WriteString(" (rolled back)")
		if e.NewID != "" {
			b.WriteString(", so this request did not run again in a new session, where it would have committed on its own")
		}
	}
	if e.State {
		b.WriteString("; settings and USE made in SQL in it (USE, SET, RESET, PRAGMA) are gone too")
		if e.NewID != "" && !e.Transaction {
			b.WriteString(", so this request did not run again in a new session, where it would have run without them")
		}
	}
	switch {
	case e.NewID != "":
		fmt.Fprintf(&b, "; the driver opened session %s with the connection's options, so the connection can be used again", e.NewID)
	case setupFailed(e.Cause):
		b.WriteString("; the connection's next request opens and sets up another session")
	default:
		b.WriteString("; the connection must be reopened")
	}
	return b.String()
}

func (e *SessionLostError) Unwrap() error { return e.Cause }

// SessionReplacedError is an error from a request that ran on a replacement
// session. It says so, because the error may be a consequence: the old
// session's temporary tables, prepared statements and the settings its SQL
// made did not carry over (only the options, and what the connection's
// new-session hook sets up).
type SessionReplacedError struct {
	OldID, NewID string
	Err          error
}

func (e *SessionReplacedError) Error() string {
	return fmt.Sprintf("%v (note: the server lost session %s and this ran in replacement session %s, "+
		"which has the connection's options and setup but not the old session's temporary tables, "+
		"prepared statements or settings made in SQL)", e.Err, e.OldID, e.NewID)
}

func (e *SessionReplacedError) Unwrap() error { return e.Err }

// lost reports whether err says the session is gone in a way a new session
// cures: the server doesn't have it, it ended because its token expired,
// which a new token cures, or it ended because it sat idle in a transaction,
// which the server rolled back (§5.6). A session ended for any other reason
// stays an error: killed by an administrator, or a reason this driver
// doesn't know (§5.6 allows a new session then, but the driver leaves that to
// the application), and credentials changed, principal dropped or token
// revoked (signing in again on its own is not the driver's call, §4.2).
func lost(err error) bool {
	p, ok := errors.AsType[*Problem](err)
	if !ok {
		return false
	}
	return p.Code == CodeSessionNotFound ||
		(p.Code == CodeSessionEnded && (p.Reason == ReasonTokenExpired || p.Reason == ReasonIdleInTransaction))
}

// lostTransaction reports whether err says the server ended the session with
// a transaction open in it (§5.6): whatever the driver knew, there was one.
func lostTransaction(err error) bool {
	p, ok := errors.AsType[*Problem](err)
	return ok && p.Code == CodeSessionEnded && p.Reason == ReasonIdleInTransaction
}

// replace opens a replacement for the session failedID, unless that already
// happened, and sets it up with the hook before any other request can use it
// (s.mu is held throughout). It returns the current session id, and whether a
// transaction may have been open in the lost session, which it also reports
// with a failed setup: the replacement is then ended, failedID stays the
// current session, and the next request that finds it gone tries again.
func (s *Session) replace(ctx context.Context, failedID string, cause error) (string, lostState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", lostState{}, errSessionClosed
	}
	if s.id != failedID {
		return s.id, lostState{tx: s.lostTx[failedID] || lostTransaction(cause), sql: s.lostState[failedID]}, nil
	}
	if p, ok := errors.AsType[*Problem](cause); ok && p.Code == CodeSessionEnded && p.Reason == ReasonTokenExpired {
		// §5.6: get a new access token before creating a new session. A
		// token issued elsewhere can't be replaced, so no session is made.
		// After idle_in_transaction the token still stands.
		if err := s.client.renewToken(ctx, s.client.currentToken()); err != nil {
			return "", lostState{}, err
		}
	}
	doc, err := s.create(ctx)
	if err != nil {
		return "", lostState{}, err
	}
	txLost := s.mayHaveTransaction() || lostTransaction(cause)
	lost := lostState{tx: txLost, sql: s.sqlState}
	hookTx, err := s.client.setUp(ctx, s.hook, doc.SessionID, failedID)
	if err != nil {
		// This failure reports the lost transaction and state; the next
		// attempt does not report them again.
		s.tx = txNone
		s.sqlState = false
		s.client.logger.WarnContext(ctx, "the server lost the session, and setting up its replacement failed; "+
			"the next request tries again", "old_session", failedID, "new_session", doc.SessionID,
			"cause", cause.Error(), "error", err.Error(), "transaction_lost", txLost, "sql_state_lost", lost.sql)
		return "", lost, err
	}
	if s.lostTx == nil {
		s.lostTx = map[string]bool{}
		s.lostState = map[string]bool{}
	}
	s.lostTx[failedID] = txLost
	s.lostState[failedID] = lost.sql
	s.sqlState = false
	s.id = doc.SessionID
	s.tx = afterHook(txFromDocument(doc, true), hookTx)
	s.replacements++
	s.client.logger.WarnContext(ctx, "the server lost the session; opened a replacement with the same options "+
		"and setup (temporary tables, prepared statements and settings made in SQL of the old session are gone)",
		"old_session", failedID, "new_session", doc.SessionID, "cause", cause.Error(), "transaction_lost", txLost,
		"sql_state_lost", lost.sql)
	return doc.SessionID, lost, nil
}

// lostState is what was lost with a replaced session that decides whether a
// request may run again on its replacement: a transaction that was, or may
// have been, open, and state set in SQL (USE, SET, ...).
type lostState struct {
	tx  bool
	sql bool
}

// lostError is the error for a request that found session id gone and was not
// run again: what was lost, and whether a transaction may have been.
func lostError(id, newID, lostWhat string, txLost, stateLost bool, cause error) *SessionLostError {
	if txLost && lostWhat == "" {
		lostWhat = "open transaction"
	}
	return &SessionLostError{OldID: id, NewID: newID, Lost: lostWhat, Transaction: txLost, State: stateLost, Cause: cause}
}

// sqlStateOf reports whether SQL changed the state of session id, the current
// one or one already replaced.
func (s *Session) sqlStateOf(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lostState[id] || id == s.id && s.sqlState
}

// do sends a request built for the current session, and handles the session
// being gone as scope allows.
func (s *Session) do(ctx context.Context, scope Scope, lostWhat string, build func(sessionID string) (*Request, error)) (*http.Response, error) {
	if err := s.Open(ctx); err != nil {
		return nil, err
	}
	id := s.ID()
	req, err := build(id)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(ctx, req)
	if !lost(err) {
		return resp, err
	}
	if !s.reconnect {
		return nil, lostError(id, "", lostWhat, s.MayHaveTransaction() || lostTransaction(err), s.sqlStateOf(id), err)
	}
	newID, lost, replaceErr := s.replace(ctx, id, err)
	if replaceErr != nil {
		return nil, lostError(id, "", lostWhat, lost.tx || s.MayHaveTransaction() || lostTransaction(err),
			lost.sql || s.sqlStateOf(id), errors.Join(err, replaceErr))
	}
	switch {
	case scope == ScopeHandle:
		return nil, lostError(id, newID, lostWhat, lost.tx, lost.sql, err)
	case lost.tx:
		// The transaction's earlier work was rolled back with the session:
		// running this request in the replacement, in autocommit, would
		// commit it on its own, and a session-wide request that succeeded
		// would hide the loss from the caller, whose next statements would
		// run outside the transaction they think they are in.
		return nil, lostError(id, newID, "open transaction", true, lost.sql, err)
	case lost.sql:
		// USE, SET, ... of the lost session are gone: in the replacement,
		// INSERT INTO t would land in another schema's table, or a statement
		// run with other settings, without an error.
		return nil, lostError(id, newID, lostWhat, false, true, err)
	case !req.Body.Replayable():
		return nil, &SessionLostError{OldID: id, NewID: newID, Lost: "upload (it streamed, so it cannot be sent again)", Cause: err}
	}
	if req, err = build(newID); err != nil {
		return nil, err
	}
	resp, err = s.client.Do(ctx, req)
	if err != nil {
		return nil, &SessionReplacedError{OldID: id, NewID: newID, Err: err}
	}
	return resp, nil
}

// doHandle sends a request about a handle that lives in session sessionID,
// which may no longer be the current one.
func (s *Session) doHandle(ctx context.Context, sessionID, what string, req *Request) (*http.Response, error) {
	resp, err := s.client.Do(ctx, req)
	if !lost(err) {
		return resp, err
	}
	if sessionID != s.ID() || !s.reconnect {
		s.mu.Lock()
		txLost := s.lostTx[sessionID] || sessionID == s.id && s.mayHaveTransaction() || lostTransaction(err)
		s.mu.Unlock()
		return nil, lostError(sessionID, "", what, txLost, s.sqlStateOf(sessionID), err)
	}
	newID, lost, replaceErr := s.replace(ctx, sessionID, err)
	if replaceErr != nil {
		return nil, lostError(sessionID, "", what, lost.tx || s.MayHaveTransaction() || lostTransaction(err),
			lost.sql || s.sqlStateOf(sessionID), errors.Join(err, replaceErr))
	}
	return nil, lostError(sessionID, newID, what, lost.tx, lost.sql, err)
}

// Get reads the session document (§5.2).
func (s *Session) Get(ctx context.Context) (*SessionDocument, error) {
	var sentTo string
	resp, err := s.do(ctx, ScopeSession, "", func(id string) (*Request, error) {
		sentTo = id
		return &Request{Method: http.MethodGet, Path: sessionPath(id), Idempotent: true}, nil
	})
	if err != nil {
		return nil, err
	}
	doc := &SessionDocument{}
	if err := decodeJSON(resp, doc); err != nil {
		return nil, err
	}
	s.observe(sentTo, doc)
	return doc, nil
}

// Transaction reads the session's transaction from the server (§5.2):
// TransactionNone, Open or Aborted, or TransactionUnknown for a value this
// driver doesn't know. The server sees SQL BEGIN and COMMIT too, so this is
// what the session's state is, not what the driver asked for.
func (s *Session) Transaction(ctx context.Context) (string, error) {
	doc, err := s.Get(ctx)
	if err != nil {
		return "", err
	}
	return doc.TransactionState(), nil
}

// OptionError is an option the server refused (§5.3).
type OptionError struct {
	Key     string
	Problem *Problem
}

func (e *OptionError) Error() string {
	return fmt.Sprintf("session option %q: %v", e.Key, e.Problem)
}

func (e *OptionError) Unwrap() error { return e.Problem }

// SetOptions sets session options (§5.3) and remembers the ones the server
// took, to replay them on a replacement session. On a pending session
// (NewLazySession) they are kept and sent with the request that opens it,
// which fails if the server refuses one.
func (s *Session) SetOptions(ctx context.Context, options map[string]any) error {
	s.mu.Lock()
	if s.pending && !s.closed {
		maps.Copy(s.options, options)
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	body, err := JSONBody(map[string]any{"options": options})
	if err != nil {
		return err
	}
	resp, err := s.do(ctx, ScopeSession, "", func(id string) (*Request, error) {
		return &Request{Method: http.MethodPatch, Path: sessionPath(id), Body: body, Idempotent: true}, nil
	})
	if err != nil {
		return err
	}
	result := &optionsResult{}
	if err := decodeJSON(resp, result); err != nil {
		return err
	}
	s.mu.Lock()
	for key, value := range options {
		if _, refused := result.Errors[key]; !refused {
			s.options[key] = value
			if key == "autocommit" && s.tx == txNone {
				// Turning autocommit on commits what was open; off opens a
				// transaction (inTransaction says so). Ask rather than guess.
				s.tx = txMaybe
			}
		}
	}
	s.mu.Unlock()
	if len(result.Errors) == 0 {
		return nil
	}
	keys := make([]string, 0, len(result.Errors))
	for key := range result.Errors {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	problem := result.Errors[keys[0]]
	if problem.Status == 0 {
		problem.Status = http.StatusBadRequest
	}
	return &OptionError{Key: keys[0], Problem: &problem}
}

// errSessionClosed is a request on a session the driver already closed.
var errSessionClosed = errors.New("the connection is closed")

// Close ends the session (§5.4). A session the server no longer has counts as
// closed, and so does one whose close the server refused for its token: the
// driver never signs in to close (Request.NoRenew), and a session left on
// the server idles out. A closed session is never replaced.
func (s *Session) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	id, pending := s.id, s.pending
	s.mu.Unlock()
	if pending {
		return nil // never opened: nothing to end
	}
	// Two attempts at most (the second for a pooled connection the server had
	// already closed): a stopped server must not hold up a close, and a
	// session whose end the server never heard idles out there.
	_, err := s.client.doJSON(ctx, &Request{Method: http.MethodDelete, Path: sessionPath(id), Idempotent: true,
		MaxAttempts: 2, NoRenew: true}, nil)
	if nothingToClose(err) {
		return nil
	}
	return err
}

// nothingToClose reports an answer to a close (of a session, statement,
// prepared statement or ingest) after which nothing is left to close: the
// server no longer has it (session_not_found or one of codes), its session
// ended (session_ended), or the server refused the token (invalid_token),
// which a close does not replace (Request.NoRenew). Then the session is gone
// or idles out, and whatever it held goes with it.
func nothingToClose(err error, codes ...string) bool {
	p, ok := errors.AsType[*Problem](err)
	if !ok {
		return false
	}
	switch p.Code {
	case CodeSessionNotFound, CodeSessionEnded, CodeInvalidToken:
		return true
	}
	return slices.Contains(codes, p.Code)
}

// Detach stops using the session without ending it on the server: its
// transaction, temporary tables and settings stay for whoever attaches to it
// next, until it is closed or idles out. A detached session is never replaced.
func (s *Session) Detach() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// Commit ends the current transaction (§5.5). It is never retried: a commit
// whose answer was lost may have happened.
func (s *Session) Commit(ctx context.Context) error {
	return s.endTransaction(ctx, "commit")
}

// Rollback abandons the current transaction (§5.5).
func (s *Session) Rollback(ctx context.Context) error {
	return s.endTransaction(ctx, "rollback")
}

func (s *Session) endTransaction(ctx context.Context, action string) error {
	if err := s.Open(ctx); err != nil {
		return err
	}
	id := s.ID()
	resp, err := s.doHandle(ctx, id, "open transaction", &Request{Method: http.MethodPost, Path: sessionPath(id) + "/" + action})
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// NewIdempotencyKey returns a fresh key for one logical request (§6.7).
func NewIdempotencyKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "k_" + hex.EncodeToString(b[:])
}

func decodeJSON(resp *http.Response, out any) error {
	defer resp.Body.Close()
	return decodeInto(resp, out)
}
