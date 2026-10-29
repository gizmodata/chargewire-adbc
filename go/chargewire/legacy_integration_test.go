// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// A connection string saved before the protocol was renamed to Chargewire (the
// warp:// scheme, adbc.warp.* options) connects to a live server, and its
// options take effect.
func TestIntegrationLegacyConnectionString(t *testing.T) {
	uri := "warp://" + shared.host + "?tls=false"
	if shared.scheme == "https" {
		t.Fatal("the shared server is expected to speak plain HTTP")
	}
	db := openDB(t, shared, map[string]string{
		adbc.OptionKeyURI:                     uri,
		"adbc.warp.statement.timeout_seconds": "0.5",
	})
	cnxn := connect(t, db)
	if got := scalarInt(t, cnxn, "SELECT 42"); got != 42 {
		t.Fatalf("SELECT 42 = %d", got)
	}
	options := cnxn.(adbc.GetSetOptionsWithContext)
	old, _ := options.GetOption(context.Background(), "adbc.warp.session.id")
	current, _ := options.GetOption(context.Background(), OptionSessionID)
	if old == "" || old != current {
		t.Errorf("session id under the old key %q, under the current one %q", old, current)
	}
	// The timeout set under its old key is in force: the server stops a long statement.
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, longQuery)
	_, err := stmt.ExecuteUpdate(context.Background())
	assertStatus(t, err, adbc.StatusTimeout, "statement_timeout")
	assertSessionUsable(t, cnxn)
}
