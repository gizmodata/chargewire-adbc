// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestIntegrationSelectOne(t *testing.T) {
	cnxn := openConn(t)
	table := queryTable(t, cnxn, "SELECT 1 AS value")
	if table.NumRows() != 1 || table.NumCols() != 1 {
		t.Fatalf("expected 1x1 result, got %dx%d", table.NumRows(), table.NumCols())
	}
	if got := int64s(t, table, 0); got[0] != 1 {
		t.Errorf("SELECT 1 = %d, want 1", got[0])
	}
}

// Every spelling of a plain-HTTP URI reaches the same server.
func TestIntegrationURISpellings(t *testing.T) {
	for _, uri := range []string{
		"warp://" + shared.host + "?tls=false",
		"http://" + shared.host,
		"http://" + shared.host + "/warp",
		"warp://" + shared.host + "/warp/?tls=0",
	} {
		t.Run(uri, func(t *testing.T) {
			cnxn := connect(t, openDB(t, shared, map[string]string{adbc.OptionKeyURI: uri}))
			if got := scalarInt(t, cnxn, "SELECT 7"); got != 7 {
				t.Fatalf("got %d", got)
			}
		})
	}
}

// A result of many batches arrives complete and in order.
func TestIntegrationManyBatches(t *testing.T) {
	cnxn := openConn(t)
	table := queryTable(t, cnxn, "SELECT i FROM range(2000000) t(i)")
	values := int64s(t, table, 0)
	if len(values) != 2_000_000 {
		t.Fatalf("got %d rows", len(values))
	}
	for i, v := range values {
		if v != int64(i) {
			t.Fatalf("row %d = %d: rows out of order", i, v)
		}
	}
	if len(table.Column(0).Data().Chunks()) < 2 {
		t.Fatal("the result must span several batches for this test")
	}
}

func TestIntegrationTypesRoundTrip(t *testing.T) {
	cnxn := openConn(t)
	table := queryTable(t, cnxn, "SELECT 42::INTEGER AS i, 'text' AS s, 2.5::DOUBLE AS d, true AS b, "+
		"NULL::INTEGER AS n, DATE '2026-10-17' AS dt, 123.45::DECIMAL(10,2) AS dec")
	want := []arrow.Type{arrow.INT32, arrow.STRING, arrow.FLOAT64, arrow.BOOL, arrow.INT32, arrow.DATE32, arrow.DECIMAL128}
	for i, id := range want {
		if got := table.Schema().Field(i).Type.ID(); got != id {
			t.Errorf("column %s is %s, want %s", table.Schema().Field(i).Name, got, id)
		}
	}
	if got := table.Column(4).Data().Chunk(0).IsNull(0); !got {
		t.Error("NULL came back as a value")
	}
}

// DDL and DML run on submit: ExecuteQuery returns an empty result with the
// rows affected, and the change is there without reading anything.
func TestIntegrationDDLDMLThroughExecuteQuery(t *testing.T) {
	cnxn := openConn(t)
	for _, sql := range []string{"CREATE OR REPLACE TABLE ddl_q (id INTEGER)", "INSERT INTO ddl_q VALUES (1), (2), (3)"} {
		stmt := newStmt(t, cnxn)
		if err := stmt.SetSqlQuery(context.Background(), sql); err != nil {
			t.Fatal(err)
		}
		reader, affected, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if reader.Schema().NumFields() != 0 {
			t.Errorf("%s: schema %s, want the empty schema", sql, reader.Schema())
		}
		if strings.HasPrefix(sql, "INSERT") && affected != 3 {
			t.Errorf("rows affected = %d, want 3", affected)
		}
		reader.Release() // never read
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ddl_q"); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

func TestIntegrationExecuteUpdateRowsAffected(t *testing.T) {
	cnxn := openConn(t)
	// The server reports no row count for DDL: unknown, -1.
	if n := execUpdate(t, cnxn, "CREATE OR REPLACE TABLE eu (v INTEGER)"); n != -1 {
		t.Errorf("CREATE TABLE affected %d rows, want -1", n)
	}
	if n := execUpdate(t, cnxn, "INSERT INTO eu SELECT * FROM range(5)"); n != 5 {
		t.Errorf("INSERT affected %d rows, want 5", n)
	}
	if n := execUpdate(t, cnxn, "DELETE FROM eu WHERE v >= 2"); n != 3 {
		t.Errorf("DELETE affected %d rows, want 3", n)
	}
	if n := execUpdate(t, cnxn, "SELECT * FROM eu"); n != -1 {
		t.Errorf("ExecuteUpdate of a query = %d, want -1 (unknown)", n)
	}
}

func TestIntegrationReturningYieldsRowsAndPersists(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS ret; CREATE OR REPLACE SEQUENCE ret_seq; CREATE TABLE ret (id INTEGER DEFAULT nextval('ret_seq') PRIMARY KEY, name VARCHAR)")
	returned := queryTable(t, cnxn, "INSERT INTO ret (name) VALUES ('alice'), ('bob') RETURNING id, name")
	if returned.NumRows() != 2 {
		t.Fatalf("RETURNING gave %d rows, want 2", returned.NumRows())
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ret"); got != 2 {
		t.Fatalf("persisted %d rows, want 2", got)
	}
	updated := queryTable(t, cnxn, "UPDATE ret SET name = 'CAROL' WHERE name = 'alice' RETURNING name")
	if got := stringsOf(t, updated, 0); len(got) != 1 || got[0] != "CAROL" {
		t.Fatalf("UPDATE RETURNING = %v", got)
	}
	deleted := queryTable(t, cnxn, "DELETE FROM ret WHERE name = 'bob' RETURNING id")
	if deleted.NumRows() != 1 {
		t.Fatalf("DELETE RETURNING gave %d rows", deleted.NumRows())
	}
	if got := stringsOf(t, queryTable(t, cnxn, "SELECT name FROM ret"), 0); len(got) != 1 || got[0] != "CAROL" {
		t.Fatalf("after DELETE: %v", got)
	}
}

// DML with RETURNING has run by the time ExecuteQuery returns (the result
// starts only after the rows are written), so it persists even when the
// returned rows are never read and the result is released, which cancels it.
func TestIntegrationReturningPersistsWithoutFetch(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE ret_unread (id INTEGER)")
	for i, sql := range []string{
		"INSERT INTO ret_unread SELECT * FROM range(100000) RETURNING id",
		"INSERT INTO ret_unread VALUES (-1) RETURNING id",
	} {
		stmt := newStmt(t, cnxn)
		setQuery(t, stmt, sql)
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		reader.Release() // never read
		want := []int64{100000, 100001}[i]
		if got := scalarInt(t, cnxn, "SELECT count(*) FROM ret_unread"); got != want {
			t.Fatalf("after %q: %d rows, want %d", sql, got, want)
		}
	}
}

// A string literal that contains RETURNING is just a value.
func TestIntegrationStringLiteralReturning(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE lit_ret (msg VARCHAR)")
	if n := execUpdate(t, cnxn, "INSERT INTO lit_ret VALUES ('RETURNING soon')"); n != 1 {
		t.Fatalf("rows affected %d", n)
	}
	if got := stringsOf(t, queryTable(t, cnxn, "SELECT msg FROM lit_ret"), 0); got[0] != "RETURNING soon" {
		t.Fatalf("got %v", got)
	}
}

func TestIntegrationMultipleStatementsInOneQuery(t *testing.T) {
	cnxn := openConn(t)
	if n := execUpdate(t, cnxn, "CREATE OR REPLACE TABLE multi (a INTEGER); INSERT INTO multi VALUES (1), (2)"); n != 2 {
		t.Fatalf("rows affected %d, want 2 (the last statement's)", n)
	}
}

func TestIntegrationBindWithoutPrepareQuery(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE autoprep_q (id INTEGER); INSERT INTO autoprep_q VALUES (1), (2), (3)")
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT id FROM autoprep_q WHERE id = ?"); err != nil {
		t.Fatal(err)
	}
	bindInt32(t, stmt, 2) // no explicit Prepare
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery after unprepared Bind: %v", err)
	}
	if got := int64s(t, readAll(t, reader), 0); len(got) != 1 || got[0] != 2 {
		t.Errorf("rows = %v, want [2]", got)
	}
}

func bindInt32(t *testing.T, stmt *statementImpl, values ...int32) {
	t.Helper()
	rec := int32Batch("p", values...)
	defer rec.Release()
	if err := stmt.Bind(context.Background(), rec); err != nil {
		t.Fatalf("Bind: %v", err)
	}
}

// One Arrow parameter row per execution (§7.2): a three-row bind inserts three
// rows in one request, and ExecuteUpdate reports all of them.
func TestIntegrationBindWithoutPrepareUpdate(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE autoprep_u (id INTEGER)")
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "INSERT INTO autoprep_u VALUES (?)"); err != nil {
		t.Fatal(err)
	}
	bindInt32(t, stmt, 10, 20, 30)
	n, err := stmt.ExecuteUpdate(context.Background())
	if err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	if n != 3 {
		t.Errorf("rows affected = %d, want 3", n)
	}
	if got := scalarInt(t, cnxn, "SELECT sum(id)::BIGINT FROM autoprep_u"); got != 60 {
		t.Errorf("sum = %d, want 60", got)
	}
}

// An explicit Prepare followed by several executions creates one prepared
// statement on the server, and reuses it.
func TestIntegrationExplicitPrepareThenBindPreparesOnce(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT ? * 2 AS doubled"); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	var first string
	for _, v := range []int32{1, 21, -4} {
		bindInt32(t, stmt, v)
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got := int64s(t, readAll(t, reader), 0); got[0] != int64(v)*2 {
			t.Errorf("%d * 2 = %d", v, got[0])
		}
		if first == "" {
			first = stmt.prepared.ref.PreparedID
		} else if stmt.prepared.ref.PreparedID != first {
			t.Errorf("prepared again: %s, then %s", first, stmt.prepared.ref.PreparedID)
		}
	}
}

// A new query drops the parameters bound for the old one.
func TestIntegrationNewQueryDropsParameters(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "SELECT ? + 1")
	bindInt32(t, stmt, 1)
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, reader)
	setQuery(t, stmt, "SELECT 5")
	reader, _, err = stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("a query without parameters after a parameterized one: %v", err)
	}
	if got := int64s(t, readAll(t, reader), 0); got[0] != 5 {
		t.Fatalf("got %d", got[0])
	}
}

// A new query replaces the prepared statement.
func TestIntegrationRebindAfterNewQueryReprepares(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	for i, sql := range []string{"SELECT ? + 1", "SELECT ? + 100"} {
		if err := stmt.SetSqlQuery(context.Background(), sql); err != nil {
			t.Fatal(err)
		}
		bindInt32(t, stmt, 1)
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := []int64{2, 101}[i]
		if got := int64s(t, readAll(t, reader), 0); got[0] != want {
			t.Errorf("%s = %d, want %d", sql, got[0], want)
		}
	}
}

// Several parameter rows of a query concatenate their results, in order.
func TestIntegrationBindStreamConcatenatesResults(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT ? AS v"); err != nil {
		t.Fatal(err)
	}
	first, second := int32Batch("p", 1, 2), int32Batch("p", 3)
	defer first.Release()
	defer second.Release()
	stream, err := array.NewRecordReader(first.Schema(), []arrow.RecordBatch{first, second})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Release()
	if err := stmt.BindStream(context.Background(), stream); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := int64s(t, readAll(t, reader), 0); len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("rows = %v, want [1 2 3]", got)
	}
	// A stream is read once: executing again runs without parameters.
	_, _, err = stmt.ExecuteQuery(context.Background())
	assertStatus(t, err, adbc.StatusInvalidArgument, "sql_error")
}

// Bound DML persists without reading anything, and a bound SELECT streams.
func TestIntegrationBoundDMLAndSelect(t *testing.T) {
	cnxn := openConn(t)
	const uuid = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE bound_dml (id UUID, v INTEGER); INSERT INTO bound_dml VALUES ('"+uuid+"', 1)")
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "DELETE FROM bound_dml WHERE id = ?"); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Bind(context.Background(), stringBatch(uuid)); err != nil {
		t.Fatal(err)
	}
	reader, affected, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader.Release() // never read
	if affected != 1 {
		t.Errorf("rows affected %d, want 1", affected)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM bound_dml"); got != 0 {
		t.Fatalf("the bound DELETE left %d rows", got)
	}
}

func stringBatch(values ...string) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).AppendValues(values, nil)
	return builder.NewRecordBatch()
}

func TestIntegrationGetParameterSchema(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT CAST(? AS INTEGER) + 1 AS a, CAST(? AS VARCHAR) AS b"); err != nil {
		t.Fatal(err)
	}
	if err := stmt.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	schema, err := stmt.GetParameterSchema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if schema.NumFields() != 2 {
		t.Fatalf("parameter schema %s, want 2 fields", schema)
	}
}

// ExecuteSchema returns what the server reported when preparing, and says
// plainly when it reported nothing.
func TestIntegrationExecuteSchema(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT 1::INTEGER AS a, 'x' AS b"); err != nil {
		t.Fatal(err)
	}
	schema, err := stmt.ExecuteSchema(context.Background())
	if stmt.prepared.resultSchema == nil {
		assertStatus(t, err, adbc.StatusNotImplemented, "")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if schema.NumFields() != 2 || schema.Field(0).Name != "a" {
		t.Fatalf("schema %s", schema)
	}
}

func TestIntegrationStatementIDOption(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, reader)
	id, err := stmt.GetOption(context.Background(), OptionStatementID)
	if err != nil || id == "" {
		t.Fatalf("statement id %q, %v", id, err)
	}
	sessionID, err := cnxn.(interface {
		GetOption(context.Context, string) (string, error)
	}).GetOption(context.Background(), OptionSessionID)
	if err != nil || sessionID == "" {
		t.Fatalf("session id %q, %v", sessionID, err)
	}
}
