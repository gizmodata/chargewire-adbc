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
	// metadata, preparing). They run again on a replacement session.
	ScopeSession Scope = iota
	// ScopeStatement requests run SQL. They run again on a replacement
	// session only outside a transaction: inside one, the work before them
	// is gone, and running the rest would commit half a transaction.
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
	closed       bool // Close was called: never replace it
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
	return s, nil
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
func (s *Session) InTransaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inTransaction()
}

func (s *Session) inTransaction() bool {
	autocommit, ok := s.options["autocommit"].(bool)
	return ok && !autocommit
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
	Lost  string
	Cause error
}

func (e *SessionLostError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "the server no longer has session %s (%v)", e.OldID, e.Cause)
	if e.Lost != "" {
		fmt.Fprintf(&b, "; the %s that lived in it is gone", e.Lost)
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
// cures. A session ended for any other reason (killed by an administrator,
// credentials changed, principal dropped) stays an error.
func lost(err error) bool {
	p, ok := errors.AsType[*Problem](err)
	if !ok {
		return false
	}
	return p.Code == CodeSessionNotFound || (p.Code == CodeSessionEnded && p.Reason == ReasonTokenExpired)
}

// replace opens a replacement for the session failedID, unless that already
// happened. It returns the current session id.
func (s *Session) replace(ctx context.Context, failedID string, cause error) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", errSessionClosed
	}
	if s.id != failedID {
		return s.id, nil
	}
	if p, ok := errors.AsType[*Problem](cause); ok && p.Code == CodeSessionEnded {
		// §5.6: get a new access token before creating a new session.
		if err := s.client.renewToken(ctx, s.client.currentToken()); err != nil {
			return "", err
		}
	}
	doc, err := s.create(ctx)
	if err != nil {
		return "", err
	}
	s.id = doc.SessionID
	s.replacements++
	s.client.logger.WarnContext(ctx, "the server lost the session; opened a replacement with the same options "+
		"(temporary tables, SQL-level settings and prepared statements of the old session are gone)",
		"old_session", failedID, "new_session", doc.SessionID, "cause", cause.Error())
	return doc.SessionID, nil
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
		return nil, &SessionLostError{OldID: id, Lost: lostWhat, Cause: err}
	}
	newID, replaceErr := s.replace(ctx, id, err)
	if replaceErr != nil {
		return nil, &SessionLostError{OldID: id, Lost: lostWhat, Cause: errors.Join(err, replaceErr)}
	}
	switch {
	case scope == ScopeHandle:
		return nil, &SessionLostError{OldID: id, NewID: newID, Lost: lostWhat, Cause: err}
	case scope == ScopeStatement && s.InTransaction():
		return nil, &SessionLostError{OldID: id, NewID: newID, Lost: "open transaction", Cause: err}
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
		return nil, &SessionLostError{OldID: sessionID, Lost: what, Cause: err}
	}
	newID, replaceErr := s.replace(ctx, sessionID, err)
	if replaceErr != nil {
		return nil, &SessionLostError{OldID: sessionID, Lost: what, Cause: errors.Join(err, replaceErr)}
	}
	return nil, &SessionLostError{OldID: sessionID, NewID: newID, Lost: what, Cause: err}
}

// Get reads the session document (§5.2).
func (s *Session) Get(ctx context.Context) (*SessionDocument, error) {
	resp, err := s.do(ctx, ScopeSession, "", func(id string) (*Request, error) {
		return &Request{Method: http.MethodGet, Path: sessionPath(id), Idempotent: true}, nil
	})
	if err != nil {
		return nil, err
	}
	doc := &SessionDocument{}
	return doc, decodeJSON(resp, doc)
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
	_, err := s.client.doJSON(ctx, &Request{Method: http.MethodDelete, Path: sessionPath(id), Idempotent: true}, nil)
	if HasCode(err, CodeSessionNotFound) {
		return nil
	}
	return err
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
