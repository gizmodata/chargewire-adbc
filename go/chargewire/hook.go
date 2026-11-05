// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
)

// NewSessionHook sets up a session the driver just created for a connection:
// its first session, when the connection opens, and every session that
// replaces one the server lost (an idle eviction, an expired token, a session
// ended for idling in a transaction; see OptionSessionReconnect). It runs
// before any request of the application uses the session, even one that is
// then refused because a transaction was lost with the old session, so the
// connection's next request finds the session set up.
//
// It is for what session options can't carry: SQL-level settings, tags, a
// search path. The driver replays the session options (catalog, schema,
// autocommit) itself.
//
// When the hook returns an error, the driver ends the session instead of
// running requests in it without the setup:
//
//   - on the first session, Database.Open fails with that error;
//   - on a replacement, the request that found the session lost fails with an
//     error that says so (StatusIO), and the connection's next request opens
//     another session and runs the hook again.
//
// Return the error from NewSession.Exec, or one of your own; an adbc.Error is
// reduced to its message, so that the driver's error says what happened to
// the session. A session the driver attached to (OptionSessionID) was not
// created by it and is never set up.
type NewSessionHook func(ctx context.Context, session NewSession) error

// NewSession is a session being set up by a NewSessionHook.
type NewSession interface {
	// ID is the new session's id.
	ID() string
	// Replaces is the id of the lost session this one replaces, or "" for
	// the connection's first session.
	Replaces() string
	// Exec runs one SQL statement in the new session and waits for it to
	// finish, discarding any rows. It goes to this session only: if the
	// server loses it meanwhile, Exec fails and so should the hook. It may be
	// called only until the hook returns.
	Exec(ctx context.Context, sql string) error
}

type hookKey struct{}

// WithNewSessionHook returns a context for Database.Open (of this driver, or
// of one built on it) whose connection runs hook on every session the driver
// creates for it. The hook belongs to that one connection, so it can carry
// the connection's own state:
//
//	cnxn, err := db.Open(chargewire.WithNewSessionHook(ctx, func(ctx context.Context, s chargewire.NewSession) error {
//		return s.Exec(ctx, "SET search_path = 'analytics'")
//	}))
func WithNewSessionHook(ctx context.Context, hook NewSessionHook) context.Context {
	return context.WithValue(ctx, hookKey{}, hook)
}

// newSessionHook is the protocol hook for Open's context, or nil.
func newSessionHook(ctx context.Context) protocol.NewSessionHook {
	hook, _ := ctx.Value(hookKey{}).(NewSessionHook)
	if hook == nil {
		return nil
	}
	return func(ctx context.Context, session *protocol.NewSession) error {
		err := hook(ctx, session)
		if _, ok := errors.AsType[adbc.Error](err); ok {
			// An adbc.Error inside would stand for the driver's own error
			// (wrapError keeps the first one it finds).
			err = errors.New(err.Error())
		}
		return err
	}
}
