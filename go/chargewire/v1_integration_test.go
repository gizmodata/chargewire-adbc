// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// transactionOf reads adbc.chargewire.session.transaction.
func transactionOf(t *testing.T, cnxn adbc.ConnectionWithContext) string {
	t.Helper()
	value, err := cnxn.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), OptionSessionTransaction)
	if err != nil {
		t.Fatalf("reading %s: %v", OptionSessionTransaction, err)
	}
	return value
}

// The session's transaction comes from the server's session document
// (§5.1, §5.2), which sees SQL BEGIN and COMMIT, not only autocommit.
func TestIntegrationSessionTransaction(t *testing.T) {
	cnxn := openConn(t)
	if got := transactionOf(t, cnxn); got != "none" {
		t.Fatalf("a new session's transaction is %q, want none", got)
	}
	execUpdate(t, cnxn, "BEGIN TRANSACTION")
	if got := transactionOf(t, cnxn); got != "open" {
		t.Fatalf("after BEGIN: %q, want open", got)
	}
	stmt := newStmt(t, cnxn)
	_ = stmt.SetSqlQuery(context.Background(), "SELECT * FROM a_table_that_does_not_exist")
	if _, err := stmt.ExecuteUpdate(context.Background()); err == nil {
		t.Fatal("a query of a missing table succeeded")
	}
	if got := transactionOf(t, cnxn); got != "aborted" && got != "open" {
		t.Fatalf("after a failed statement in the transaction: %q, want aborted (or open, for an engine that keeps it usable)", got)
	}
	execUpdate(t, cnxn, "ROLLBACK")
	if got := transactionOf(t, cnxn); got != "none" {
		t.Fatalf("after ROLLBACK: %q, want none", got)
	}
	if err := cnxn.(adbc.GetSetOptionsWithContext).SetOption(context.Background(), adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
		t.Fatal(err)
	}
	if got := transactionOf(t, cnxn); got != "open" {
		t.Fatalf("with autocommit off: %q, want open", got)
	}
	if err := cnxn.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := transactionOf(t, cnxn); got != "open" {
		t.Fatalf("after a commit with autocommit off: %q, want open (the next transaction)", got)
	}
}

// A query tag with non-ASCII characters is sent escaped, which the server
// accepts (it refuses a malformed tag with bad_request, §2.5).
func TestIntegrationQueryTagWithNonASCII(t *testing.T) {
	cnxn := connect(t, openDB(t, shared, map[string]string{OptionQueryTag: `{"job": "Zoë’s nightly load", "run": 7}`}))
	if got := scalarInt(t, cnxn, "SELECT 42"); got != 42 {
		t.Fatal(got)
	}
	execUpdate(t, cnxn, "CREATE TEMP TABLE tagged AS SELECT 1 AS v")
}
