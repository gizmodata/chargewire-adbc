// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// An option the driver doesn't know is NOT_IMPLEMENTED before and after the
// first connection; one it knows is INVALID_STATE once a connection opened.
func TestDatabaseOptionStatusesAfterOpen(t *testing.T) {
	f := newFakeChargewire(t)
	ctx := context.Background()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: f.server.URL, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	opts := db.(adbc.GetSetOptionsWithContext)
	if got := adbcStatus(opts.SetOption(ctx, "this_option_does_not_exist", "v")); got != adbc.StatusNotImplemented {
		t.Fatalf("before the first connection: status %v, want NOT_IMPLEMENTED", got)
	}
	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cnxn.Close(ctx)
	if got := adbcStatus(opts.SetOption(ctx, "this_option_does_not_exist", "v")); got != adbc.StatusNotImplemented {
		t.Errorf("after the first connection: status %v, want NOT_IMPLEMENTED", got)
	}
	for key := range databaseOptionKeys {
		if key == OptionAccessToken {
			continue // replaces the token of an opened database
		}
		if got := adbcStatus(opts.SetOption(ctx, key, "v")); got != adbc.StatusInvalidState {
			t.Errorf("%s after the first connection: status %v, want INVALID_STATE", key, got)
		}
	}
}

// Every key in databaseOptionKeys is one SetOption knows: none is
// NOT_IMPLEMENTED on a database that never connected.
func TestDatabaseOptionKeysAreKnown(t *testing.T) {
	ctx := context.Background()
	for key := range databaseOptionKeys {
		db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := adbcStatus(db.(adbc.GetSetOptionsWithContext).SetOption(ctx, key, "v")); got == adbc.StatusNotImplemented {
			t.Errorf("%s: NOT_IMPLEMENTED on a new database", key)
		}
		_ = db.Close(ctx)
	}
}
