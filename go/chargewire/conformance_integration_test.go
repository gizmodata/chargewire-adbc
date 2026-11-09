// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The ADBC statuses the specification asks for, which the ADBC validation
// (conformance) suite checks too (validation/), each next to the case that
// must keep its own status.

func wantStatus(t *testing.T, err error, status adbc.Status, contains string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error; want status %v", status)
	}
	if got := errorStatus(err); got != status {
		t.Errorf("status %v, want %v (%v)", got, status, err)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Errorf("error %q doesn't say %q", err, contains)
	}
}

func TestIntegrationUnknownDatabaseOptionAfterOpenIsNotImplemented(t *testing.T) {
	db := openDB(t, shared, nil)
	connect(t, db)
	ctx := context.Background()
	opts := db.(adbc.GetSetOptionsWithContext)
	wantStatus(t, opts.SetOption(ctx, "this_option_does_not_exist", "value"), adbc.StatusNotImplemented,
		"this_option_does_not_exist")
	// The twin: a known option is still too late to set once a connection has opened.
	wantStatus(t, opts.SetOption(ctx, adbc.OptionKeyUsername, "someone"), adbc.StatusInvalidState,
		adbc.OptionKeyUsername)
}

func TestIntegrationMissingCurrentCatalogOrSchemaIsNotFound(t *testing.T) {
	cnxn := openConn(t)
	ctx := context.Background()
	opts := cnxn.(adbc.GetSetOptionsWithContext)
	execUpdate(t, cnxn, "CREATE SCHEMA IF NOT EXISTS conformance_schema")
	catalog, err := opts.GetOption(ctx, adbc.OptionKeyCurrentCatalog)
	if err != nil {
		t.Fatal(err)
	}

	wantStatus(t, opts.SetOption(ctx, adbc.OptionKeyCurrentCatalog, "conformance_no_catalog"), adbc.StatusNotFound,
		"conformance_no_catalog")
	wantStatus(t, opts.SetOption(ctx, adbc.OptionKeyCurrentDbSchema, "conformance_no_schema"), adbc.StatusNotFound,
		"conformance_no_schema")
	// The endpoint's filter is a pattern: a name that only matches as one is still missing.
	wantStatus(t, opts.SetOption(ctx, adbc.OptionKeyCurrentDbSchema, "conformance_schem_"), adbc.StatusNotFound,
		"conformance_schem_")

	// The twins: names that exist are set.
	if err := opts.SetOption(ctx, adbc.OptionKeyCurrentDbSchema, "conformance_schema"); err != nil {
		t.Fatal(err)
	}
	if got, err := opts.GetOption(ctx, adbc.OptionKeyCurrentDbSchema); err != nil || got != "conformance_schema" {
		t.Errorf("current schema %q (%v), want conformance_schema", got, err)
	}
	if err := opts.SetOption(ctx, adbc.OptionKeyCurrentCatalog, catalog); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationAppendWithAColumnTheTableLacksIsAlreadyExists(t *testing.T) {
	cnxn := openConn(t)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE conformance_append (a BIGINT, b VARCHAR DEFAULT 'none')")
	batch := func(columns map[string]arrow.Array) arrow.RecordBatch {
		var fields []arrow.Field
		var arrays []arrow.Array
		for _, name := range []string{"a", "b", "c", "A"} {
			if column, ok := columns[name]; ok {
				fields = append(fields, arrow.Field{Name: name, Type: column.DataType(), Nullable: true})
				arrays = append(arrays, column)
				defer column.Release()
			}
		}
		return array.NewRecordBatch(arrow.NewSchema(fields, nil), arrays, int64(arrays[0].Len()))
	}
	appendRows := func(mode string, columns map[string]arrow.Array, options map[string]string) error {
		t.Helper()
		rec := batch(columns)
		defer rec.Release()
		_, err := ingest(t, cnxn, "conformance_append", mode, rec, options)
		return err
	}
	ints := func(values ...int64) arrow.Array {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(values, nil)
		return b.NewArray()
	}
	strs := func(values ...string) arrow.Array {
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(values, nil)
		return b.NewArray()
	}

	for _, mode := range []string{adbc.OptionValueIngestModeAppend, adbc.OptionValueIngestModeCreateAppend} {
		err := appendRows(mode, map[string]arrow.Array{"a": ints(1), "c": ints(2)}, nil)
		wantStatus(t, err, adbc.StatusAlreadyExists, `"c"`)
	}
	// Create, which this doesn't touch, still says the table exists.
	wantStatus(t, appendRows(adbc.OptionValueIngestModeCreate, map[string]arrow.Array{"a": ints(1)}, nil),
		adbc.StatusAlreadyExists, "conformance_append")
	// The twins: a column that exists with a value that doesn't convert is the
	// data's error; a subset of the columns, in any case or order, appends.
	wantStatus(t, appendRows(adbc.OptionValueIngestModeAppend, map[string]arrow.Array{"a": strs("x")}, nil),
		adbc.StatusInvalidArgument, "Conversion")
	if err := appendRows(adbc.OptionValueIngestModeAppend, map[string]arrow.Array{"A": ints(7)}, nil); err != nil {
		t.Fatal(err)
	}
	table := queryTable(t, cnxn, "SELECT a || '/' || b FROM conformance_append")
	defer table.Release()
	if got := stringsOf(t, table, 0); len(got) != 1 || got[0] != "7/none" {
		t.Errorf("appended rows %q, want [7/none]", got)
	}

	// A temporary table is looked up where DuckDB keeps it (temp.main).
	execUpdate(t, cnxn, "CREATE OR REPLACE TEMPORARY TABLE conformance_append (a BIGINT)")
	wantStatus(t, appendRows(adbc.OptionValueIngestModeAppend, map[string]arrow.Array{"a": ints(1), "b": strs("x")},
		map[string]string{adbc.OptionValueIngestTemporary: adbc.OptionValueEnabled}), adbc.StatusAlreadyExists, `"b"`)
}

// The same for an ingest that goes in parts (§8.3), where the server refuses
// the data when the ingest is opened or committed.
func TestIntegrationAppendInPartsWithAColumnTheTableLacksIsAlreadyExists(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE conformance_append_parts (k BIGINT, v VARCHAR)")
	batches := partsBatches(64, 20_000)
	defer releaseAll(batches)
	reader := readerOf(t, batches)
	defer reader.Release()
	_, err := ingestStream(t, cnxn, "conformance_append_parts", reader,
		map[string]string{adbc.OptionKeyIngestMode: adbc.OptionValueIngestModeAppend})
	wantStatus(t, err, adbc.StatusAlreadyExists, "conformance_append_parts")
	if _, opens, _, _, _ := ingestRequests(proxy); opens != 1 {
		t.Fatalf("%d partitioned ingests opened; want 1", opens)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM conformance_append_parts"); got != 0 {
		t.Fatalf("the refused append wrote %d rows", got)
	}
}
