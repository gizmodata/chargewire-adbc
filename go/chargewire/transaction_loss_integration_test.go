// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// serverIdleInTxFlagEnv names the server's flag for the idle-in-transaction
// timeout in seconds (§5.7): chargewire-duckdb's by default, Hardcharger's
// --idle-in-transaction-session-timeout.
const serverIdleInTxFlagEnv = "CHARGEWIRE_ADBC_TEST_SERVER_IDLE_IN_TRANSACTION_FLAG"

// assertLostTransaction checks the error of a statement that found its
// session gone with a transaction open: it says so, and the connection has a
// replacement session.
func assertLostTransaction(t *testing.T, cnxn adbc.ConnectionWithContext, err error, code string) {
	t.Helper()
	assertStatus(t, err, adbc.StatusIO, code)
	if !strings.Contains(err.Error(), "open transaction") || !strings.Contains(err.Error(), "did not run again") {
		t.Errorf("the error does not say the transaction was lost and the statement not run again: %v", err)
	}
	if got := replacements(t, cnxn); got != "1" {
		t.Errorf("replacements = %s, want 1: the connection should be usable again", got)
	}
}

// tableSum is sum(v) of table, read on a connection of its own.
func tableSum(t *testing.T, server *testServer, table string) int64 {
	t.Helper()
	return scalarInt(t, connect(t, openDB(t, server, nil)), "SELECT coalesce(sum(v), 0)::BIGINT FROM "+table)
}

// A session lost (evicted when idle, the server restarted) inside a
// transaction begun in SQL, which the driver doesn't see: the server rolled
// the transaction back, so the statement that finds the session gone must not
// run again in the replacement session, where autocommit would commit it on
// its own. It fails, and the connection works again afterwards.
func TestIntegrationLostSessionAfterSQLBegin(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE lost_sql_tx (v INTEGER)")
	execUpdate(t, cnxn, "BEGIN TRANSACTION")
	execUpdate(t, cnxn, "INSERT INTO lost_sql_tx VALUES (1)")
	killSession(t, cnxn)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "INSERT INTO lost_sql_tx VALUES (2)")
	_, err := stmt.ExecuteUpdate(context.Background())
	assertLostTransaction(t, cnxn, err, "session_not_found")
	if got := tableSum(t, shared, "lost_sql_tx"); got != 0 {
		t.Fatalf("sum = %d, want 0: the statement ran again in autocommit after its transaction was rolled back", got)
	}
	// The replacement session runs statements, in autocommit.
	execUpdate(t, cnxn, "INSERT INTO lost_sql_tx VALUES (4)")
	if got := tableSum(t, shared, "lost_sql_tx"); got != 4 {
		t.Fatalf("sum = %d, want 4", got)
	}
}

// The same through a query (ExecuteQuery, the DB-API cursor's path) and a
// prepared statement with parameters, which the driver prepares again when
// its session is replaced: never inside a lost transaction.
func TestIntegrationLostSessionAfterSQLBeginPrepared(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE lost_sql_tx_prepared (v INTEGER)")
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "INSERT INTO lost_sql_tx_prepared VALUES (?)")
	bindInt32(t, stmt, 0)
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		t.Fatal(err)
	}
	queryTable(t, cnxn, "BEGIN").Release()
	bindInt32(t, stmt, 1)
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		t.Fatal(err)
	}
	killSession(t, cnxn)
	bindInt32(t, stmt, 2)
	_, err := stmt.ExecuteUpdate(context.Background())
	assertLostTransaction(t, cnxn, err, "session_not_found")
	if got := tableSum(t, shared, "lost_sql_tx_prepared"); got != 0 {
		t.Fatalf("sum = %d, want 0: the prepared statement ran again in autocommit after its transaction was rolled back", got)
	}
	bindInt32(t, stmt, 4)
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		t.Fatalf("the prepared statement after the loss: %v", err)
	}
	if got := tableSum(t, shared, "lost_sql_tx_prepared"); got != 4 {
		t.Fatalf("sum = %d, want 4", got)
	}
}

// A metadata call that finds the session lost inside a transaction reports
// the loss, rather than run again quietly and leave the caller's next
// statements outside the transaction it thinks it is in.
func TestIntegrationLostTransactionReportedByMetadata(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "BEGIN")
	killSession(t, cnxn)
	_, err := cnxn.GetTableTypes(context.Background())
	assertLostTransaction(t, cnxn, err, "session_not_found")
	assertSessionUsable(t, cnxn)
}

// Once a transaction is committed (the driver reads the session document
// after COMMIT), a lost session is replaced and the statement runs again, as
// outside any transaction.
func TestIntegrationLostSessionAfterCommitRunsAgain(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE lost_after_commit (v INTEGER)")
	execUpdate(t, cnxn, "BEGIN")
	execUpdate(t, cnxn, "INSERT INTO lost_after_commit VALUES (1)")
	execUpdate(t, cnxn, "COMMIT")
	killSession(t, cnxn)
	execUpdate(t, cnxn, "INSERT INTO lost_after_commit VALUES (2)")
	if got := replacements(t, cnxn); got != "1" {
		t.Fatalf("replacements = %s, want 1", got)
	}
	if got := tableSum(t, shared, "lost_after_commit"); got != 3 {
		t.Fatalf("sum = %d, want 3", got)
	}
}

// A session the server ends for idling in a transaction (session_ended,
// reason idle_in_transaction, §5.6, §5.7) is replaced, and the statement that
// finds it ended fails instead of running again in autocommit: the
// transaction's earlier statements were rolled back. The connection works
// afterwards (before, the driver reported the error and left the connection
// on the ended session, so every later call failed too).
func TestIntegrationIdleInTransaction(t *testing.T) {
	server := startServer(t, envOr(serverIdleInTxFlagEnv, "--idle-in-transaction-timeout-seconds"), "1")
	for _, prepared := range []bool{false, true} {
		cnxn := connect(t, openDB(t, server, nil))
		execUpdate(t, cnxn, "CREATE OR REPLACE TABLE idle_tx (v INTEGER)")
		stmt := newStmt(t, cnxn)
		setQuery(t, stmt, "INSERT INTO idle_tx VALUES (1)")
		if prepared {
			setQuery(t, stmt, "INSERT INTO idle_tx VALUES (?)")
			bindInt32(t, stmt, 1)
		}
		execUpdate(t, cnxn, "BEGIN")
		if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
			t.Fatal(err)
		}
		// The server ends the session within 5 s after the 1 s limit.
		time.Sleep(7 * time.Second)
		if prepared {
			bindInt32(t, stmt, 2)
		} else {
			setQuery(t, stmt, "INSERT INTO idle_tx VALUES (2)")
		}
		_, err := stmt.ExecuteUpdate(context.Background())
		assertLostTransaction(t, cnxn, err, "session_ended")
		if !strings.Contains(err.Error(), "idle_in_transaction") {
			t.Errorf("the error does not give the reason: %v", err)
		}
		if got := tableSum(t, server, "idle_tx"); got != 0 {
			t.Fatalf("sum = %d, want 0: the statement ran again in autocommit after its transaction was rolled back", got)
		}
		if prepared {
			bindInt32(t, stmt, 4)
		} else {
			setQuery(t, stmt, "INSERT INTO idle_tx VALUES (4)")
		}
		if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
			t.Fatalf("the connection after the session ended: %v", err)
		}
		if got := tableSum(t, server, "idle_tx"); got != 4 {
			t.Fatalf("sum = %d, want 4", got)
		}
	}
}
