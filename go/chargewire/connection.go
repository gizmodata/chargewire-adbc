// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
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
	// closeOnRelease: Close ends the session (OptionSessionCloseOnRelease).
	closeOnRelease atomic.Bool
}

func newConnection(db *databaseImpl, client *protocol.Client, discovery *protocol.Discovery, session *protocol.Session,
	closeOnRelease bool) driverbase.Connection {
	c := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase),
		db:                 db,
		client:             client,
		discovery:          discovery,
		session:            session,
		defaults:           db.defaults,
		janitor:            newJanitor(db.Logger),
	}
	c.closeOnRelease.Store(closeOnRelease)
	return driverbase.NewConnectionBuilder(c).
		WithAutocommitSetter(c).
		WithCurrentNamespacer(c).
		WithTableTypeLister(c).
		Connection()
}

func (c *connectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	key = currentOptionKey(key)
	if value, ok := c.defaults.get(key); ok {
		return value, nil
	}
	switch key {
	case OptionSessionID:
		// The session's id names a session on the server: open a lazy one.
		if err := c.session.Open(ctx); err != nil {
			return "", wrapError(err, "opening the session")
		}
		return c.session.ID(), nil
	case OptionSessionReplacements:
		return strconv.Itoa(c.session.Replacements()), nil
	case OptionSessionCloseOnRelease:
		return strconv.FormatBool(c.closeOnRelease.Load()), nil
	case OptionAccessToken, OptionAccessTokenExpiresAt:
		return currentTokenOption(ctx, c.client, key)
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
	key = currentOptionKey(key)
	if key == OptionSessionReplacements {
		return int64(c.session.Replacements()), nil
	}
	return c.ConnectionImplBase.GetOptionInt(ctx, key)
}

func (c *connectionImpl) SetOption(ctx context.Context, key, value string) error {
	key = currentOptionKey(key)
	if handled, err := c.defaults.set(key, value); handled {
		return err
	}
	switch key {
	case OptionSessionID:
		return c.ErrorHelper.InvalidArgument("option %s is read-only on a connection; to attach to a session, "+
			"set it on the database before its first connection opens", key)
	case OptionSessionReplacements, OptionSessionTransaction, OptionAccessTokenExpiresAt:
		return c.ErrorHelper.InvalidArgument("option %s is read-only", key)
	case OptionSessionCloseOnRelease:
		b, err := parseBool(key, value)
		if err != nil {
			return err
		}
		c.closeOnRelease.Store(b)
		return nil
	case adbc.OptionKeyReadOnly, adbc.OptionKeyIsolationLevel:
		return c.ErrorHelper.NotImplemented("option %s is not supported by Chargewire", key)
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

// SetCurrentCatalog implements driverbase.CurrentNamespacer. A catalog that
// doesn't exist is NOT_FOUND.
func (c *connectionImpl) SetCurrentCatalog(ctx context.Context, catalog string) error {
	err := wrapError(c.session.SetOptions(ctx, map[string]any{"catalog": catalog}), "setting the current catalog")
	if errorStatus(err) != adbc.StatusInvalidArgument {
		return err
	}
	return c.notFoundUnless(ctx, err, func() (bool, error) { return c.catalogExists(ctx, catalog) })
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer. A schema that
// doesn't exist in the current catalog is NOT_FOUND.
//
// schema is one schema name, looked up in the current catalog; it is never
// split into a catalog and a schema at a dot, because a dot can be part of a
// schema's name (CREATE SCHEMA "sales.emea"). So "lake.reports" names a schema
// called lake.reports, and when there is none but lake is a catalog, the
// error says how to name that catalog's schema: the current catalog first,
// then the schema.
func (c *connectionImpl) SetCurrentDbSchema(ctx context.Context, schema string) error {
	err := wrapError(c.session.SetOptions(ctx, map[string]any{"schema": schema}), "setting the current schema")
	if errorStatus(err) != adbc.StatusInvalidArgument {
		return err
	}
	var current string
	err = c.notFoundUnless(ctx, err, func() (bool, error) {
		catalog, cerr := c.sessionOption(ctx, "catalog")
		if cerr != nil {
			return false, cerr
		}
		current = catalog
		return c.schemaExists(ctx, catalog, schema)
	})
	if errorStatus(err) != adbc.StatusNotFound {
		return err
	}
	catalog, rest, dotted := strings.Cut(schema, ".")
	if !dotted || catalog == "" || rest == "" {
		return err
	}
	if found, cerr := c.catalogExists(ctx, catalog); cerr != nil || !found {
		return err
	}
	return withHint(err, fmt.Sprintf("the current schema is one schema name in the current catalog (%q), never a "+
		"catalog.schema path: for the schema %q of the catalog %q, set the current catalog to %q and the "+
		"current schema to %q", current, rest, catalog, catalog, rest))
}

// notFoundUnless gives err, the server's refusal of a current catalog or
// schema (an invalid argument), the status NOT_FOUND when the metadata
// endpoints say the name doesn't exist. Whether it exists is asked, never read
// from the server's message; when asking fails, err stays as it is.
func (c *connectionImpl) notFoundUnless(ctx context.Context, err error, exists func() (bool, error)) error {
	found, cerr := exists()
	if cerr != nil || found {
		return err
	}
	return withStatus(err, adbc.StatusNotFound)
}

// catalogExists reports whether the catalogs endpoint lists catalog by
// exactly that name.
func (c *connectionImpl) catalogExists(ctx context.Context, catalog string) (bool, error) {
	found := false
	err := c.readMetadata(ctx, "catalogs", nil, func(rec arrow.RecordBatch) error {
		names, err := stringColumn(rec, "catalog_name")
		if err != nil {
			return err
		}
		for i := range names.Len() {
			if !names.IsNull(i) && names.Value(i) == catalog {
				found = true
			}
		}
		return nil
	})
	return found, err
}

// schemaExists reports whether the db-schemas endpoint lists schema in
// catalog by exactly those names (the endpoint's filter is a pattern, so the
// name is escaped and the rows compared).
func (c *connectionImpl) schemaExists(ctx context.Context, catalog, schema string) (bool, error) {
	found := false
	query := url.Values{}
	query.Set("db_schema_filter_pattern", escapeLike(schema))
	err := c.readMetadata(ctx, "db-schemas", query, func(rec arrow.RecordBatch) error {
		catalogs, err := stringColumn(rec, "catalog_name")
		if err != nil {
			return err
		}
		schemas, err := stringColumn(rec, "db_schema_name")
		if err != nil {
			return err
		}
		for i := range schemas.Len() {
			if !catalogs.IsNull(i) && catalogs.Value(i) == catalog && !schemas.IsNull(i) && schemas.Value(i) == schema {
				found = true
			}
		}
		return nil
	})
	return found, err
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
// their results on the server, unless OptionSessionCloseOnRelease is false (by
// default for an attached session): then the session stays on the server, its
// transaction open, for the next connection that attaches to it. Ending it is
// best-effort: when the server
// can't be reached or refuses (it stopped, the network is down), Close still
// succeeds after at most closeTimeout, and the session ends on the server when
// it idles out. Whatever the connection holds locally is released either way.
func (c *connectionImpl) Close(ctx context.Context) error {
	c.janitor.close()
	if !c.closeOnRelease.Load() {
		c.session.Detach()
		return nil
	}
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
