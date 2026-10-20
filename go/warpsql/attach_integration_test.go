// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

func setConnOption(t *testing.T, cnxn adbc.ConnectionWithContext, key, value string) {
	t.Helper()
	if err := cnxn.(adbc.GetSetOptionsWithContext).SetOption(context.Background(), key, value); err != nil {
		t.Fatalf("SetOption(%s, %s): %v", key, value, err)
	}
}

// A transaction survives its connection: the first connection begins it and
// detaches (close_on_release false), a second database attaches to the
// session, sees the uncommitted row and commits it. Closing an attached
// connection leaves the session; closing one with close_on_release true ends
// it, and attaching again is then session_not_found.
func TestIntegrationAttachToASession(t *testing.T) {
	ctx := context.Background()
	first := connect(t, openDB(t, shared, nil))
	execUpdate(t, first, "CREATE OR REPLACE TABLE attach_handoff (i INTEGER)")
	setConnOption(t, first, OptionSessionCloseOnRelease, "false")
	execUpdate(t, first, "BEGIN TRANSACTION")
	execUpdate(t, first, "INSERT INTO attach_handoff VALUES (1)")
	id, err := first.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}

	outside := connect(t, openDB(t, shared, nil))
	if got := scalarInt(t, outside, "SELECT count(*) FROM attach_handoff"); got != 0 {
		t.Fatalf("another session sees %d uncommitted rows", got)
	}
	second := connect(t, openDB(t, shared, map[string]string{OptionSessionID: id}))
	if got := transactionOf(t, second); got != "open" {
		t.Fatalf("the attached session's transaction is %q, want open", got)
	}
	if got := scalarInt(t, second, "SELECT count(*) FROM attach_handoff"); got != 1 {
		t.Fatalf("the attached session sees %d rows, want its uncommitted 1", got)
	}
	execUpdate(t, second, "COMMIT")
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := scalarInt(t, outside, "SELECT count(*) FROM attach_handoff"); got != 1 {
		t.Fatalf("after the attached session's COMMIT another session sees %d rows, want 1", got)
	}

	// The attached connection's close left the session: attach again, and
	// this time end it.
	third := connect(t, openDB(t, shared, map[string]string{OptionSessionID: id}))
	setConnOption(t, third, OptionSessionCloseOnRelease, "true")
	if err := third.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = openDB(t, shared, map[string]string{OptionSessionID: id}).Open(ctx)
	assertStatus(t, err, adbc.StatusNotFound, "session_not_found")
}

// A created connection's close ends its session by default.
func TestIntegrationClosingACreatedSessionEndsIt(t *testing.T) {
	ctx := context.Background()
	cnxn := connect(t, openDB(t, shared, nil))
	id, err := cnxn.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := cnxn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = openDB(t, shared, map[string]string{OptionSessionID: id}).Open(ctx)
	assertStatus(t, err, adbc.StatusNotFound, "session_not_found")
}

func TestIntegrationAttachToAMissingSession(t *testing.T) {
	_, err := openDB(t, shared, map[string]string{OptionSessionID: "s_does_not_exist"}).Open(context.Background())
	assertStatus(t, err, adbc.StatusNotFound, "session_not_found")
}
