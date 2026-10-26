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

// ingest bulk-loads rec into table with the given ADBC mode and extra
// statement options, and returns the rows written.
func ingest(t *testing.T, cnxn adbc.ConnectionWithContext, table, mode string, rec arrow.RecordBatch, options map[string]string) (int64, error) {
	t.Helper()
	stmt := newStmt(t, cnxn)
	options = mergeOptions(map[string]string{adbc.OptionKeyIngestTargetTable: table, adbc.OptionKeyIngestMode: mode}, options)
	for key, value := range options {
		if err := stmt.SetOption(context.Background(), key, value); err != nil {
			t.Fatalf("SetOption(%s): %v", key, err)
		}
	}
	if err := stmt.Bind(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	return stmt.ExecuteUpdate(context.Background())
}

func mergeOptions(base, extra map[string]string) map[string]string {
	for key, value := range extra {
		base[key] = value
	}
	return base
}

func keyValueBatch(keys []int64, values []string) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues(keys, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(values, nil)
	return builder.NewRecordBatch()
}

func TestIntegrationIngestModes(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS ing_modes; DROP TABLE IF EXISTS ing_ca_new")
	rec := keyValueBatch([]int64{1, 2, 3}, []string{"alice", "bob", "carol"})
	defer rec.Release()

	n, err := ingest(t, cnxn, "ing_modes", adbc.OptionValueIngestModeCreate, rec, nil)
	if err != nil || n != 3 {
		t.Fatalf("create: %d rows, %v", n, err)
	}
	if got := stringsOf(t, queryTable(t, cnxn, "SELECT v FROM ing_modes ORDER BY k"), 0); got[2] != "carol" {
		t.Fatalf("stored %v", got)
	}
	if n, err := ingest(t, cnxn, "ing_modes", adbc.OptionValueIngestModeAppend, rec, nil); err != nil || n != 3 {
		t.Fatalf("append: %d rows, %v", n, err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ing_modes"); got != 6 {
		t.Fatalf("after append: %d rows", got)
	}
	if n, err := ingest(t, cnxn, "ing_modes", adbc.OptionValueIngestModeCreateAppend, rec, nil); err != nil || n != 3 {
		t.Fatalf("create_append on an existing table: %d rows, %v", n, err)
	}
	if n, err := ingest(t, cnxn, "ing_ca_new", adbc.OptionValueIngestModeCreateAppend, rec, nil); err != nil || n != 3 {
		t.Fatalf("create_append on a new table: %d rows, %v", n, err)
	}
	one := keyValueBatch([]int64{99}, []string{"only"})
	defer one.Release()
	if _, err := ingest(t, cnxn, "ing_modes", adbc.OptionValueIngestModeReplace, one, nil); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ing_modes"); got != 1 {
		t.Fatalf("after replace: %d rows", got)
	}
}

// The ADBC default mode is create: it refuses an existing table, and the
// failed upload leaves the table as it was.
func TestIntegrationIngestCreateRefusesExistingTable(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS ing_once")
	rec := keyValueBatch([]int64{1}, []string{"a"})
	defer rec.Release()
	if _, err := ingest(t, cnxn, "ing_once", adbc.OptionValueIngestModeCreate, rec, nil); err != nil {
		t.Fatal(err)
	}
	_, err := ingest(t, cnxn, "ing_once", adbc.OptionValueIngestModeCreate, rec, nil)
	assertStatus(t, err, adbc.StatusAlreadyExists, "sql_error")
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ing_once"); got != 1 {
		t.Fatalf("the refused upload changed the table: %d rows", got)
	}
}

func TestIntegrationIngestAppendNeedsTable(t *testing.T) {
	cnxn := openConn(t)
	rec := keyValueBatch([]int64{1}, []string{"a"})
	defer rec.Release()
	_, err := ingest(t, cnxn, "ing_missing", adbc.OptionValueIngestModeAppend, rec, nil)
	assertStatus(t, err, adbc.StatusNotFound, "sql_error")
}

func TestIntegrationIngestTargetCatalogSchemaAndTemporary(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "ATTACH IF NOT EXISTS ':memory:' AS ing_cat; CREATE SCHEMA IF NOT EXISTS ing_cat.s; DROP TABLE IF EXISTS ing_cat.s.t")
	rec := keyValueBatch([]int64{1, 2}, []string{"a", "b"})
	defer rec.Release()
	if _, err := ingest(t, cnxn, "t", adbc.OptionValueIngestModeCreate, rec, map[string]string{
		adbc.OptionValueIngestTargetCatalog: "ing_cat", adbc.OptionValueIngestTargetDBSchema: "s",
	}); err != nil {
		t.Fatal(err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ing_cat.s.t"); got != 2 {
		t.Fatalf("ing_cat.s.t has %d rows", got)
	}
	if _, err := ingest(t, cnxn, "ing_tmp", adbc.OptionValueIngestModeCreate, rec, map[string]string{
		adbc.OptionValueIngestTemporary: "true",
	}); err != nil {
		t.Fatal(err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'ing_tmp' AND temporary"); got != 1 {
		t.Fatal("ing_tmp is not a temporary table")
	}
	// Temporary tables belong to their session.
	other := openConn(t)
	stmt := newStmt(t, other)
	_ = stmt.SetSqlQuery(context.Background(), "SELECT * FROM ing_tmp")
	_, _, err := stmt.ExecuteQuery(context.Background())
	assertStatus(t, err, adbc.StatusNotFound, "sql_error")
}

// An upload too big to keep in memory streams (chunked), in many batches,
// with zstd buffer compression when asked.
func TestIntegrationIngestLargeStreamedUpload(t *testing.T) {
	for _, compression := range []string{"none", "zstd", "lz4_frame"} {
		t.Run(compression, func(t *testing.T) {
			cnxn := openConn(t)
			execUpdate(t, cnxn, "DROP TABLE IF EXISTS ing_big_"+compression)
			const batches, rowsPerBatch = 40, 50_000 // 2M rows, ~32 MB of int64s: over the in-memory limit
			schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}, {Name: "j", Type: arrow.PrimitiveTypes.Int64}}, nil)
			var records []arrow.RecordBatch
			for b := range batches {
				builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
				for r := range rowsPerBatch {
					i := int64(b*rowsPerBatch + r)
					builder.Field(0).(*array.Int64Builder).Append(i)
					builder.Field(1).(*array.Int64Builder).Append(i * 2)
				}
				records = append(records, builder.NewRecordBatch())
				builder.Release()
			}
			stream, err := array.NewRecordReader(schema, records)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range records {
				rec.Release()
			}
			defer stream.Release()
			stmt := newStmt(t, cnxn)
			for key, value := range map[string]string{
				adbc.OptionKeyIngestTargetTable: "ing_big_" + compression, OptionIPCCompression: compression,
			} {
				if err := stmt.SetOption(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			if err := stmt.BindStream(context.Background(), stream); err != nil {
				t.Fatal(err)
			}
			n, err := stmt.ExecuteUpdate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			const total = batches * rowsPerBatch
			if n != total {
				t.Fatalf("ingested %d rows, want %d", n, total)
			}
			if got := scalarInt(t, cnxn, "SELECT (sum(j) - 2 * sum(i))::BIGINT FROM ing_big_"+compression); got != 0 {
				t.Fatalf("rows were altered: %d", got)
			}
			if got := scalarInt(t, cnxn, "SELECT sum(i)::BIGINT FROM ing_big_"+compression); got != total*(total-1)/2 {
				t.Fatalf("sum(i) = %d", got)
			}
		})
	}
}

// A type the server cannot keep exactly is refused, never stored as text
// (§8.1).
func TestIntegrationIngestUnsupportedType(t *testing.T) {
	cnxn := openConn(t)
	schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: arrow.FixedWidthTypes.Duration_ns}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.DurationBuilder).Append(1_500_000_123)
	rec := builder.NewRecordBatch()
	builder.Release()
	defer rec.Release()
	_, err := ingest(t, cnxn, "ing_ns", adbc.OptionValueIngestModeCreate, rec, nil)
	assertStatus(t, err, adbc.StatusNotImplemented, "unsupported_arrow_type")
}

func TestIntegrationIngestWithoutData(t *testing.T) {
	cnxn := openConn(t)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetOption(context.Background(), adbc.OptionKeyIngestTargetTable, "ing_nothing"); err != nil {
		t.Fatal(err)
	}
	_, err := stmt.ExecuteUpdate(context.Background())
	assertStatus(t, err, adbc.StatusInvalidState, "")
	// Ingest runs with ExecuteUpdate only.
	_, _, err = stmt.ExecuteQuery(context.Background())
	assertStatus(t, err, adbc.StatusInvalidState, "")
}

// An append binds the upload's columns to the table's by name (spec §8.1): two
// columns of one type uploaded the other way round land where their names say,
// an omitted column takes its default, and a column the table lacks is refused
// with nothing written. warp-duckdb before 1.0.2 bound by position and stored
// the swapped columns crosswise without an error.
func TestIntegrationIngestAppendBindsColumnsByName(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE ing_by_name (a INTEGER, b INTEGER, c VARCHAR DEFAULT 'dflt')")
	swapped := int32Columns([]string{"b", "a"}, [][]int32{{10, 20}, {1, 2}})
	defer swapped.Release()
	if n, err := ingest(t, cnxn, "ing_by_name", adbc.OptionValueIngestModeAppend, swapped, nil); err != nil || n != 2 {
		t.Fatalf("append: %d rows, %v", n, err)
	}
	got := stringsOf(t, queryTable(t, cnxn, "SELECT a || ':' || b || ':' || c AS r FROM ing_by_name ORDER BY a"), 0)
	if len(got) != 2 || got[0] != "1:10:dflt" || got[1] != "2:20:dflt" {
		t.Fatalf("stored %v; want the values under their own names", got)
	}
	extra := int32Columns([]string{"a", "nope"}, [][]int32{{3}, {0}})
	defer extra.Release()
	if _, err := ingest(t, cnxn, "ing_by_name", adbc.OptionValueIngestModeAppend, extra, nil); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("a column the table lacks: %v; want a refusal naming it", err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM ing_by_name"); got != 2 {
		t.Fatalf("after the refused upload: %d rows", got)
	}
}

func int32Columns(columns []string, values [][]int32) arrow.RecordBatch {
	fields := make([]arrow.Field, len(columns))
	for i, name := range columns {
		fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer builder.Release()
	for i, column := range values {
		builder.Field(i).(*array.Int32Builder).AppendValues(column, nil)
	}
	return builder.NewRecordBatch()
}
