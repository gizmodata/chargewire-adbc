// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// runFailing runs sql to its end (query and result) and returns its error.
func runFailing(t *testing.T, cnxn adbc.ConnectionWithContext, sql string) error {
	t.Helper()
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, sql)
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err == nil {
		for reader.Next() {
		}
		err = reader.Err()
		reader.Release()
	}
	if err == nil {
		t.Fatalf("%s succeeded; want an error", sql)
	}
	return err
}

// A user's SQL that fails is the user's error, never the driver's or the
// server's (StatusInternal): before it runs (sql_error) or while it runs
// (execution_error, HTTP 422). The SQLSTATE and the server's message reach
// the adbc.Error.
func TestIntegrationSQLErrorStatuses(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE errstatus_keys (id INTEGER PRIMARY KEY)")
	execUpdate(t, cnxn, "INSERT INTO errstatus_keys VALUES (1)")
	for _, tc := range []struct {
		name, sql      string
		status         adbc.Status
		code, sqlstate string
		message        string
	}{
		// DuckDB's / divides floating point (1/0 is no error); // divides
		// integers. It reports a division by zero as invalid input, so the
		// server sends no SQLSTATE for it.
		{"division by zero", "SELECT 1 // 0", adbc.StatusInvalidArgument, "execution_error", "", "ivision by zero"},
		{"division by zero over rows", "SELECT 10 // (i - 3) FROM range(5) t(i)", adbc.StatusInvalidArgument, "execution_error", "", "ivision by zero"},
		{"conversion while running", "SELECT CAST(s AS INTEGER) FROM (VALUES ('1'), ('x')) t(s)", adbc.StatusInvalidArgument, "execution_error", "", "Conversion Error"},
		{"error()", "SELECT CASE WHEN i < 3 THEN i ELSE error('boom') END FROM range(10) t(i)", adbc.StatusInvalidArgument, "execution_error", "", "boom"},
		{"syntax error", "SELEC 1", adbc.StatusInvalidArgument, "sql_error", "42601", "Parser Error"},
		{"unknown column", "SELECT no_such_column FROM range(1)", adbc.StatusInvalidArgument, "sql_error", "", "no_such_column"},
		{"unknown table", "SELECT * FROM errstatus_no_such_table", adbc.StatusNotFound, "sql_error", "42P01", "errstatus_no_such_table"},
		{"existing table", "CREATE TABLE errstatus_keys (id INTEGER)", adbc.StatusAlreadyExists, "sql_error", "42P07", "errstatus_keys"},
		{"duplicate key", "INSERT INTO errstatus_keys VALUES (1)", adbc.StatusIntegrity, "execution_error", "23000", "onstraint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runFailing(t, cnxn, tc.sql)
			assertStatus(t, err, tc.status, tc.code)
			adbcErr, _ := errors.AsType[adbc.Error](err)
			if got := strings.TrimRight(string(adbcErr.SqlState[:]), "\x00"); got != tc.sqlstate {
				t.Errorf("SQLSTATE %q, want %q (%v)", got, tc.sqlstate, err)
			}
			if !strings.Contains(err.Error(), tc.message) || errorDetail(err, ErrorDetailCode) != tc.code {
				t.Errorf("error %v (details %+v) lost the server's message or code", err, adbcErr.Details)
			}
		})
	}
	assertSessionUsable(t, cnxn)
}

// A wrong password is StatusUnauthenticated, at the first request that signs
// in (NewDatabase validates nothing over the network; Open does).
func TestIntegrationBadCredentialsStatus(t *testing.T) {
	integrationTests.Add(1)
	options := shared.options()
	options[adbc.OptionKeyPassword] = "not-" + shared.password
	db, err := NewDriver(nil).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		assertStatus(t, err, adbc.StatusUnauthenticated, "invalid_credentials")
		return
	}
	defer db.Close(context.Background())
	cnxn, err := db.Open(context.Background())
	if err == nil {
		_ = cnxn.Close(context.Background())
		t.Fatal("a wrong password opened a connection")
	}
	assertStatus(t, err, adbc.StatusUnauthenticated, "invalid_credentials")
}
