// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func insecureTLS() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true}
}

func newStmt(t *testing.T, cnxn adbc.ConnectionWithContext) *statementImpl {
	t.Helper()
	stmt, err := cnxn.NewStatement(context.Background())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	t.Cleanup(func() { _ = stmt.Close(context.Background()) })
	return stmt.(*statementImpl)
}

// execUpdate runs sql with ExecuteUpdate and returns the rows affected.
func execUpdate(t *testing.T, cnxn adbc.ConnectionWithContext, sql string) int64 {
	t.Helper()
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
	n, err := stmt.ExecuteUpdate(context.Background())
	if err != nil {
		t.Fatalf("ExecuteUpdate(%q): %v", sql, err)
	}
	return n
}

// queryTable runs sql and reads its whole result.
func queryTable(t *testing.T, cnxn adbc.ConnectionWithContext, sql string) arrow.Table {
	t.Helper()
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery(%q): %v", sql, err)
	}
	return readAll(t, reader)
}

// readAll drains and releases a reader.
func readAll(t *testing.T, reader array.RecordReader) arrow.Table {
	t.Helper()
	defer reader.Release()
	var batches []arrow.RecordBatch
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		batches = append(batches, rec)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading the result: %v", err)
	}
	table := array.NewTableFromRecords(reader.Schema(), batches)
	for _, rec := range batches {
		rec.Release()
	}
	t.Cleanup(table.Release)
	return table
}

// int64s reads an integer column of any width.
func int64s(t *testing.T, table arrow.Table, column int) []int64 {
	t.Helper()
	var out []int64
	for _, chunk := range table.Column(column).Data().Chunks() {
		for i := range chunk.Len() {
			switch c := chunk.(type) {
			case *array.Int64:
				out = append(out, c.Value(i))
			case *array.Int32:
				out = append(out, int64(c.Value(i)))
			case *array.Int16:
				out = append(out, int64(c.Value(i)))
			case *array.Int8:
				out = append(out, int64(c.Value(i)))
			default:
				t.Fatalf("column %d is %s, not an integer", column, chunk.DataType())
			}
		}
	}
	return out
}

// strings reads a utf8 column.
func stringsOf(t *testing.T, table arrow.Table, column int) []string {
	t.Helper()
	var out []string
	for _, chunk := range table.Column(column).Data().Chunks() {
		c, ok := chunk.(*array.String)
		if !ok {
			t.Fatalf("column %d is %s, not utf8", column, chunk.DataType())
		}
		for i := range c.Len() {
			out = append(out, c.Value(i))
		}
	}
	return out
}

func scalarInt(t *testing.T, cnxn adbc.ConnectionWithContext, sql string) int64 {
	t.Helper()
	values := int64s(t, queryTable(t, cnxn, sql), 0)
	if len(values) != 1 {
		t.Fatalf("%q returned %d rows, want 1", sql, len(values))
	}
	return values[0]
}

func int32Batch(name string, values ...int32) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues(values, nil)
	return builder.NewRecordBatch()
}

// assertStatus checks an error's ADBC status and Chargewire code ("" for an
// error that did not come from the server).
func assertStatus(t *testing.T, err error, status adbc.Status, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error with status %v and code %q, got none", status, code)
	}
	adbcErr, ok := errors.AsType[adbc.Error](err)
	if !ok {
		t.Fatalf("error is not an adbc.Error: %v", err)
	}
	if adbcErr.Code != status {
		t.Errorf("status = %v, want %v (%v)", adbcErr.Code, status, err)
	}
	if got := ErrorCode(err); got != code {
		t.Errorf("Chargewire code = %q, want %q (%v)", got, code, err)
	}
}

// assertSessionUsable proves the session is free again: a statement still
// running in it would hold this one up.
func assertSessionUsable(t *testing.T, cnxn adbc.ConnectionWithContext) {
	t.Helper()
	done := make(chan int64, 1)
	go func() { done <- scalarInt(t, cnxn, "SELECT 42") }()
	select {
	case got := <-done:
		if got != 42 {
			t.Fatalf("SELECT 42 = %d", got)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the session did not answer SELECT 42 within 30s: an earlier statement is still running")
	}
}
