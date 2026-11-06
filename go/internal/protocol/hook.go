// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// NewSessionHook sets up a session the driver just created (§5.1): the first
// session of a connection, and every session that replaces a lost one (§5.6).
// It runs before any other request uses the session, so that state the
// session options can't carry (a server's SQL-level settings) is the same in
// every session of the connection. When it fails, the driver ends the session
// rather than run requests in it.
type NewSessionHook func(ctx context.Context, session *NewSession) error

// NewSession is a session being set up by a NewSessionHook.
type NewSession struct {
	client   *Client
	id       string
	replaces string

	mu   sync.Mutex
	done bool        // the hook returned: Exec refuses
	tx   txKnowledge // what the hook's statements leave of the transaction (nextTx)
}

// ID is the new session's id.
func (n *NewSession) ID() string { return n.id }

// Replaces is the id of the lost session this one replaces, or "" for a
// connection's first session.
func (n *NewSession) Replaces() string { return n.replaces }

// hookWait is how long the server may hold the answer to a hook's statement
// (§6.1) before the driver polls its status (§6.3).
const hookWait = 15 * time.Second

// Exec runs one SQL statement in the new session and waits for it to finish.
// Its rows, if any, are discarded. It sends the statement to this session
// only: a session that is lost while the hook runs is not replaced, and the
// hook fails. The error is the server's (a *Problem) or the transport's.
func (n *NewSession) Exec(ctx context.Context, sql string) error {
	n.mu.Lock()
	if n.done {
		n.mu.Unlock()
		return fmt.Errorf("session %s: Exec after its new-session hook returned", n.id)
	}
	n.tx = nextTx(n.tx, sql)
	n.mu.Unlock()
	body, err := SubmitDocument(sql, nil)
	if err != nil {
		return err
	}
	sub := &Submission{Body: body, SQL: sql, Wait: hookWait, key: NewIdempotencyKey(), sessionID: n.id}
	resp, err := n.client.Do(ctx, sub.request(false, false))
	if err != nil {
		return err
	}
	doc, err := DecodeStatementDocument(resp)
	if err != nil {
		return err
	}
	ref := StatementRef{SessionID: n.id, StatementID: doc.StatementID}
	defer func() {
		// Free the statement (§6.8); best effort, the session's end frees it too.
		freeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = n.client.doJSON(freeCtx, &Request{Method: http.MethodDelete, Path: ref.path(), Idempotent: true,
			MaxAttempts: 1, NoRenew: true}, nil)
	}()
	for !doc.State.Terminal() {
		header := http.Header{"Prefer": {"wait=" + strconv.Itoa(int(hookWait/time.Second))}}
		doc = &StatementDocument{}
		if _, err := n.client.doJSON(ctx, &Request{Method: http.MethodGet, Path: ref.path(), Header: header,
			Idempotent: true}, doc); err != nil {
			return err
		}
	}
	if doc.State == StateSucceeded {
		return nil
	}
	if doc.Error != nil {
		return doc.Error
	}
	code := CodeClientStatementFailed
	if doc.State == StateCancelled {
		code = CodeClientStatementCancelled
	}
	return &Problem{Code: code, CancelReason: doc.CancelReason,
		Detail: fmt.Sprintf("statement %s %s, and the server sent no error", doc.StatementID, doc.State)}
}

// SessionSetupError is a new session whose NewSessionHook failed. The driver
// ended that session rather than run requests in it without the setup.
type SessionSetupError struct {
	SessionID string
	// Replaces is the lost session it was to replace, or "".
	Replaces string
	Err      error
}

func (e *SessionSetupError) Error() string {
	if e.Replaces == "" {
		return fmt.Sprintf("setting up new session %s failed, so the driver ended it: %v", e.SessionID, e.Err)
	}
	return fmt.Sprintf("setting up session %s, which was to replace lost session %s, failed, so the driver "+
		"ended it: %v", e.SessionID, e.Replaces, e.Err)
}

func (e *SessionSetupError) Unwrap() error { return e.Err }

// setUp runs hook on session id, just created (replacing replaces, or ""). It
// reports what the hook's statements leave of the session's transaction
// (nextTx): possibly open after one that could have begun a transaction. When
// the hook fails, the session is ended (best effort) and the error is a
// *SessionSetupError.
func (c *Client) setUp(ctx context.Context, hook NewSessionHook, id, replaces string) (txKnowledge, error) {
	if hook == nil {
		return txNone, nil
	}
	n := &NewSession{client: c, id: id, replaces: replaces}
	err := hook(ctx, n)
	n.mu.Lock()
	n.done = true
	tx := n.tx
	n.mu.Unlock()
	if err == nil {
		return tx, nil
	}
	endCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, _ = c.doJSON(endCtx, &Request{Method: http.MethodDelete, Path: sessionPath(id), Idempotent: true, MaxAttempts: 1,
		NoRenew: true}, nil)
	return txNone, &SessionSetupError{SessionID: id, Replaces: replaces, Err: err}
}

// setupFailed reports whether err says a replacement session's setup failed,
// which the next request that finds the session gone tries again.
func setupFailed(err error) bool {
	var setup *SessionSetupError
	return errors.As(err, &setup)
}
