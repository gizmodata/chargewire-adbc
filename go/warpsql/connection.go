// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// closeTimeout bounds ending the session when a connection closes.
const closeTimeout = 5 * time.Second

// longPoll is how long the server may hold a status request (§6.3). It stays
// well below the idle timeouts of common proxies.
const longPoll = 15 * time.Second

type connectionImpl struct {
	driverbase.ConnectionImplBase

	db        *databaseImpl
	client    *protocol.Client
	discovery *protocol.Discovery
	session   *protocol.Session
	defaults  execOptions
	janitor   *janitor
	// running counts the connection's statements between submit and the end
	// of their result.
	running atomic.Int32
}

func newConnection(db *databaseImpl, client *protocol.Client, discovery *protocol.Discovery, session *protocol.Session) driverbase.Connection {
	c := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase),
		db:                 db,
		client:             client,
		discovery:          discovery,
		session:            session,
		defaults:           db.defaults,
		janitor:            newJanitor(db.Logger),
	}
	return driverbase.NewConnectionBuilder(c).
		WithAutocommitSetter(c).
		WithCurrentNamespacer(c).
		WithTableTypeLister(c).
		Connection()
}

func (c *connectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	if value, ok := c.defaults.get(key); ok {
		return value, nil
	}
	switch key {
	case OptionSessionID:
		return c.session.ID(), nil
	case OptionSessionReplacements:
		return strconv.Itoa(c.session.Replacements()), nil
	case OptionSessionTransaction:
		// The server's view (§5.2), which includes SQL BEGIN and COMMIT.
		transaction, err := c.session.Transaction(ctx)
		if err != nil {
			return "", wrapError(err, "reading the session's transaction")
		}
		return transaction, nil
	}
	return c.ConnectionImplBase.GetOption(ctx, key)
}

func (c *connectionImpl) GetOptionInt(ctx context.Context, key string) (int64, error) {
	if key == OptionSessionReplacements {
		return int64(c.session.Replacements()), nil
	}
	return c.ConnectionImplBase.GetOptionInt(ctx, key)
}

func (c *connectionImpl) SetOption(ctx context.Context, key, value string) error {
	if handled, err := c.defaults.set(key, value); handled {
		return err
	}
	switch key {
	case OptionSessionID, OptionSessionReplacements, OptionSessionTransaction:
		return c.ErrorHelper.InvalidArgument("option %s is read-only", key)
	case adbc.OptionKeyReadOnly, adbc.OptionKeyIsolationLevel:
		return c.ErrorHelper.NotImplemented("option %s is not supported by Warp SQL", key)
	}
	return c.ConnectionImplBase.SetOption(ctx, key, value)
}

// SetAutocommit implements driverbase.AutocommitSetter: turning autocommit off
// starts a transaction (§5.5).
func (c *connectionImpl) SetAutocommit(ctx context.Context, enabled bool) error {
	if !enabled && !c.discovery.Has(protocol.CapabilityTransactions) {
		return c.ErrorHelper.NotImplemented("the server does not support transactions")
	}
	return wrapError(c.session.SetOptions(ctx, map[string]any{"autocommit": enabled}), "setting autocommit")
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	return c.sessionOption(ctx, "catalog")
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	return c.sessionOption(ctx, "schema")
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(ctx context.Context, catalog string) error {
	return wrapError(c.session.SetOptions(ctx, map[string]any{"catalog": catalog}), "setting the current catalog")
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(ctx context.Context, schema string) error {
	return wrapError(c.session.SetOptions(ctx, map[string]any{"schema": schema}), "setting the current schema")
}

// sessionOption reads one option from the server's session document, which
// also reflects SQL such as USE.
func (c *connectionImpl) sessionOption(ctx context.Context, name string) (string, error) {
	doc, err := c.session.Get(ctx)
	if err != nil {
		return "", wrapError(err, "reading the session")
	}
	value, ok := doc.Options[name].(string)
	if !ok {
		return "", c.ErrorHelper.NotFound("the server does not report the session's %s", name)
	}
	return value, nil
}

// Commit ends the current transaction.
func (c *connectionImpl) Commit(ctx context.Context) error {
	return wrapError(c.session.Commit(ctx), "Commit")
}

// Rollback abandons the current transaction.
func (c *connectionImpl) Rollback(ctx context.Context) error {
	return wrapError(c.session.Rollback(ctx), "Rollback")
}

func (c *connectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	return newStatement(c), nil
}

// Close ends the session, which also stops its running statements and frees
// their results on the server. Ending it is best-effort: when the server
// can't be reached or refuses (it stopped, the network is down), Close still
// succeeds after at most closeTimeout, and the session ends on the server when
// it idles out. Whatever the connection holds locally is released either way.
func (c *connectionImpl) Close(ctx context.Context) error {
	c.janitor.close()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	if err := c.session.Close(ctx); err != nil {
		c.db.Logger.Debug("could not end the session on the server; it ends there when it idles out",
			"session", c.session.ID(), "error", err)
	}
	return nil
}

// requireCapability fails when the server does not offer an optional feature
// (§3.1).
func (c *connectionImpl) requireCapability(capability, what string) error {
	if c.discovery.Has(capability) {
		return nil
	}
	return c.ErrorHelper.NotImplemented("%s needs the server capability %q, which %s %s does not offer",
		what, capability, c.discovery.Server.Name, c.discovery.Server.Version)
}
