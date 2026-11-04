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
	// the rest would commit half a transaction.
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

	mu           sync.Mutex
	id           string
	options      map[string]any // the options set through this Session, replayed on a replacement
	replacements int
	closed       bool // Close or Detach was called: never replace it
	attached     bool // AttachSession: another connection created it
	// tx is what the driver knows of the current session's transaction,
	// which SQL (BEGIN) changes without the driver's say: it decides whether
	// a request may run again on a replacement session.
	tx txKnowledge
	// lostTx says, for each session id that was replaced, whether a
	// transaction may have been open in it, for requests that find the
	// session gone after another request replaced it.
	lostTx map[string]bool
}

// txKnowledge is what the driver knows of a session's transaction (§5.5).
// The server tells it only in the session document (§5.1, §5.2), so the
// driver reads that document after a statement that may have begun or ended
// a transaction, and until it has, assumes one may be open.
type txKnowledge int

const (
	// txNone: no transaction is open; the server said so, and no statement
	// that could open one has run since.
	txNone txKnowledge = iota
	// txMaybe: a statement that could have begun or ended a transaction ran
	// since the server last said; RefreshTransaction asks.
	txMaybe
	// txOpen: the server said a transaction is open (or aborted, or a value
	// this driver doesn't know).
	txOpen
)

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
	s := &Session{client: c, reconnect: reconnect, options: maps.Clone(options)}
	if s.options == nil {
		s.options = map[string]any{}
	}
	doc, err := s.create(ctx)
	if err != nil {
		return nil, err
	}
	s.id = doc.SessionID
	s.tx = txFromDocument(doc, true)
	return s, nil
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

// ID is the current session id.
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
// current session: autocommit is off, the server said one is open, or a
// statement that could have begun one (SQL BEGIN) ran since the server last
// said none was. A request that finds the session gone runs again on a
// replacement only when this is false.
func (s *Session) MayHaveTransaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mayHaveTransaction()
}

func (s *Session) mayHaveTransaction() bool {
	return s.tx != txNone || s.inTransaction()
}

// TransactionUnconfirmed reports whether a statement that could have begun or
// ended a transaction ran since the server last said what the session's
// transaction is; RefreshTransaction asks.
func (s *Session) TransactionUnconfirmed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tx == txMaybe
}

// noteStatement records that SQL ran in session id (the server took the
// request): a statement that could begin or end a transaction leaves the
// driver unsure of the transaction until it reads the session document.
func (s *Session) noteStatement(id, sql string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id != id {
		return
	}
	if s.tx == txNone && mayBeginTransaction(sql) || s.tx == txOpen && mayEndTransaction(sql) {
		s.tx = txMaybe
	}
}

// observe records the transaction a session document of session id says.
func (s *Session) observe(id string, doc *SessionDocument) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id == id {
		s.tx = txFromDocument(doc, false)
	}
}

// RefreshTransaction reads the session document (§5.2) when a statement that
// could have begun or ended a transaction ran since the driver last did, so
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

// Words of SQL statements that can begin a transaction (BEGIN, START
// TRANSACTION, an engine's SET autocommit or EXECUTE of a prepared BEGIN), and
// those that can end one (COMMIT, ROLLBACK, END, ABORT). A statement that has
// none of them as a word anywhere in its text, comments and literals
// included, leaves the session's transaction as it was, except that a
// failure may abort an open one, which is still a transaction the driver
// treats as open. Matching more than needed costs one session read.
var (
	beginWords = []string{"BEGIN", "START", "TRANSACTION", "AUTOCOMMIT", "EXECUTE"}
	endWords   = []string{"COMMIT", "ROLLBACK", "END", "ABORT", "BEGIN", "START", "TRANSACTION", "AUTOCOMMIT", "EXECUTE"}
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
	Cause       error
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
	if e.NewID != "" {
		fmt.Fprintf(&b, "; the driver opened session %s with the connection's options, so the connection can be used again", e.NewID)
	} else {
		b.WriteString("; the connection must be reopened")
	}
	return b.String()
}

func (e *SessionLostError) Unwrap() error { return e.Cause }

// SessionReplacedError is an error from a request that ran on a replacement
// session. It says so, because the error may be a consequence: the old
// session's temporary tables and SQL-level settings did not carry over.
type SessionReplacedError struct {
	OldID, NewID string
	Err          error
}

func (e *SessionReplacedError) Error() string {
	return fmt.Sprintf("%v (note: the server lost session %s and this ran in replacement session %s, "+
		"which has the connection's options but not the old session's temporary tables, "+
		"SQL-level settings or prepared statements)", e.Err, e.OldID, e.NewID)
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
// happened. It returns the current session id, and whether a transaction may
// have been open in the lost session.
func (s *Session) replace(ctx context.Context, failedID string, cause error) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", false, errSessionClosed
	}
	if s.id != failedID {
		return s.id, s.lostTx[failedID] || lostTransaction(cause), nil
	}
	if p, ok := errors.AsType[*Problem](cause); ok && p.Code == CodeSessionEnded && p.Reason == ReasonTokenExpired {
		// §5.6: get a new access token before creating a new session. A
		// token issued elsewhere can't be replaced, so no session is made.
		// After idle_in_transaction the token still stands.
		if err := s.client.renewToken(ctx, s.client.currentToken()); err != nil {
			return "", false, err
		}
	}
	doc, err := s.create(ctx)
	if err != nil {
		return "", false, err
	}
	txLost := s.mayHaveTransaction() || lostTransaction(cause)
	if s.lostTx == nil {
		s.lostTx = map[string]bool{}
	}
	s.lostTx[failedID] = txLost
	s.id = doc.SessionID
	s.tx = txFromDocument(doc, true)
	s.replacements++
	s.client.logger.WarnContext(ctx, "the server lost the session; opened a replacement with the same options "+
		"(temporary tables, SQL-level settings and prepared statements of the old session are gone)",
		"old_session", failedID, "new_session", doc.SessionID, "cause", cause.Error(), "transaction_lost", txLost)
	return doc.SessionID, txLost, nil
}

// lostError is the error for a request that found session id gone and was not
// run again: what was lost, and whether a transaction may have been.
func lostError(id, newID, lostWhat string, txLost bool, cause error) *SessionLostError {
	if txLost && lostWhat == "" {
		lostWhat = "open transaction"
	}
	return &SessionLostError{OldID: id, NewID: newID, Lost: lostWhat, Transaction: txLost, Cause: cause}
}

// do sends a request built for the current session, and handles the session
// being gone as scope allows.
func (s *Session) do(ctx context.Context, scope Scope, lostWhat string, build func(sessionID string) (*Request, error)) (*http.Response, error) {
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
		return nil, lostError(id, "", lostWhat, s.MayHaveTransaction() || lostTransaction(err), err)
	}
	newID, txLost, replaceErr := s.replace(ctx, id, err)
	if replaceErr != nil {
		return nil, lostError(id, "", lostWhat, s.MayHaveTransaction() || lostTransaction(err), errors.Join(err, replaceErr))
	}
	switch {
	case scope == ScopeHandle:
		return nil, lostError(id, newID, lostWhat, txLost, err)
	case txLost:
		// The transaction's earlier work was rolled back with the session:
		// running this request in the replacement, in autocommit, would
		// commit it on its own, and a session-wide request that succeeded
		// would hide the loss from the caller, whose next statements would
		// run outside the transaction they think they are in.
		return nil, lostError(id, newID, "open transaction", true, err)
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
		return nil, lostError(sessionID, "", what, txLost, err)
	}
	newID, txLost, replaceErr := s.replace(ctx, sessionID, err)
	if replaceErr != nil {
		return nil, lostError(sessionID, "", what, s.MayHaveTransaction() || lostTransaction(err), errors.Join(err, replaceErr))
	}
	return nil, lostError(sessionID, newID, what, txLost, err)
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
// took, to replay them on a replacement session.
func (s *Session) SetOptions(ctx context.Context, options map[string]any) error {
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
// closed. A closed session is never replaced.
func (s *Session) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	id := s.id
	s.mu.Unlock()
	// Two attempts at most (the second for a pooled connection the server had
	// already closed): a stopped server must not hold up a close, and a
	// session whose end the server never heard idles out there.
	_, err := s.client.doJSON(ctx, &Request{Method: http.MethodDelete, Path: sessionPath(id), Idempotent: true,
		MaxAttempts: 2}, nil)
	if HasCode(err, CodeSessionNotFound) {
		return nil
	}
	return err
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
