// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
)

var partsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "k", Type: arrow.PrimitiveTypes.Int64},
	{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "d", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
}, nil)

// partsBatches is batches × rows rows: k counts up from 0, v is NULL on every
// seventh row, d on every eleventh.
func partsBatches(batches, rows int) []arrow.RecordBatch {
	var out []arrow.RecordBatch
	for b := range batches {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, partsSchema)
		for r := range rows {
			k := int64(b*rows + r)
			builder.Field(0).(*array.Int64Builder).Append(k)
			if k%7 == 0 {
				builder.Field(1).AppendNull()
			} else {
				builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row %d", k))
			}
			if k%11 == 0 {
				builder.Field(2).AppendNull()
			} else {
				builder.Field(2).(*array.Float64Builder).Append(float64(k) / 4)
			}
		}
		out = append(out, builder.NewRecordBatch())
		builder.Release()
	}
	return out
}

// failingReader yields its batches, then fails instead of ending.
type failingReader struct {
	array.RecordReader
	failed bool
}

func (r *failingReader) Next() bool {
	if r.RecordReader.Next() {
		return true
	}
	r.failed = true
	return false
}

func (r *failingReader) Err() error {
	if r.failed {
		return errors.New("the application's reader broke")
	}
	return nil
}

// ingestStream bulk-loads batches into table through BindStream.
func ingestStream(t *testing.T, cnxn adbc.ConnectionWithContext, table string, reader array.RecordReader, options map[string]string) (int64, error) {
	t.Helper()
	stmt := newStmt(t, cnxn)
	for key, value := range mergeOptions(map[string]string{adbc.OptionKeyIngestTargetTable: table}, options) {
		if err := stmt.SetOption(context.Background(), key, value); err != nil {
			t.Fatalf("SetOption(%s): %v", key, err)
		}
	}
	if err := stmt.BindStream(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	return stmt.ExecuteUpdate(context.Background())
}

func readerOf(t *testing.T, batches []arrow.RecordBatch) array.RecordReader {
	t.Helper()
	reader, err := array.NewRecordReader(partsSchema, batches)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func releaseAll(batches []arrow.RecordBatch) {
	for _, batch := range batches {
		batch.Release()
	}
}

// checkPartsTable checks every row of a table loaded from partsBatches.
func checkPartsTable(t *testing.T, cnxn adbc.ConnectionWithContext, table string, total int64) {
	t.Helper()
	checks := map[string]int64{
		"SELECT count(*) FROM " + table:                             total,
		"SELECT count(DISTINCT k) FROM " + table:                    total,
		"SELECT sum(k)::BIGINT FROM " + table:                       total * (total - 1) / 2,
		"SELECT count(v) FROM " + table:                             total - (total+6)/7,
		"SELECT count(d) FROM " + table:                             total - (total+10)/11,
		"SELECT count(*) FROM " + table + " WHERE v <> 'row ' || k": 0,
		"SELECT count(*) FROM " + table + " WHERE d <> k / 4":       0,
	}
	var want4 int64
	for k := int64(0); k < total; k += 4 {
		if k%11 != 0 {
			want4 += k
		}
	}
	checks["SELECT (sum(d) * 4)::BIGINT FROM "+table+" WHERE k % 4 = 0"] = want4
	for sql, want := range checks {
		if got := scalarInt(t, cnxn, sql); got != want {
			t.Errorf("%s = %d, want %d", sql, got, want)
		}
	}
}

func ingestRequests(proxy *faultProxy) (single, opens, parts, commits, aborts int) {
	for _, r := range proxy.requests(func(*http.Request) bool { return true }) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/ingest"):
			single++
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/ingests"):
			opens++
		case r.Method == http.MethodPost && strings.Contains(path, "/parts/"):
			parts++
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/commit"):
			commits++
		case r.Method == http.MethodDelete && strings.Contains(path, "/ingests/"):
			aborts++
		}
	}
	return
}

// A large upload goes in parts by default (§8.3): every row arrives once,
// unaltered, committed by one commit.
func TestIntegrationIngestInParts(t *testing.T) {
	for _, compression := range []string{"none", "zstd", "lz4_frame"} {
		t.Run(compression, func(t *testing.T) {
			proxy, cnxn := viaProxy(t, map[string]string{OptionIPCCompression: compression})
			table := "ing_parts_" + compression
			execUpdate(t, cnxn, "DROP TABLE IF EXISTS "+table)
			batches := partsBatches(64, 20_000)
			defer releaseAll(batches)
			reader := readerOf(t, batches)
			defer reader.Release()
			n, err := ingestStream(t, cnxn, table, reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			if n != 64*20_000 {
				t.Fatalf("ingested %d rows", n)
			}
			checkPartsTable(t, cnxn, table, 64*20_000)
			single, opens, parts, commits, aborts := ingestRequests(proxy)
			if single != 0 || opens != 1 || parts != nearIngestParts || commits != 1 || aborts != 0 {
				t.Fatalf("requests: %d single, %d opens, %d parts, %d commits, %d aborts", single, opens, parts, commits, aborts)
			}
		})
	}
}

// The number of parts follows the upload's size: about one per two slices of
// uploadSliceRows, one stream for a single slice, at most the parts allowed. A
// large batch is sent as slices, so even a one-batch upload goes in parts.
func TestIntegrationIngestPartsScaleWithSize(t *testing.T) {
	for _, tc := range []struct {
		batches, rows int
		wantParts     int // 0: one stream
	}{
		{1, 10_000, 0},
		{3, 13_000, 2},                // 39,000 rows: three slices
		{1, 122_880, 4},               // one batch, eight slices
		{40, 50_000, nearIngestParts}, // two million rows
	} {
		t.Run(fmt.Sprintf("%dx%d", tc.batches, tc.rows), func(t *testing.T) {
			proxy, cnxn := viaProxy(t, nil)
			table := fmt.Sprintf("ing_scale_%d_%d", tc.batches, tc.rows)
			execUpdate(t, cnxn, "DROP TABLE IF EXISTS "+table)
			batches := partsBatches(tc.batches, tc.rows)
			defer releaseAll(batches)
			reader := readerOf(t, batches)
			defer reader.Release()
			n, err := ingestStream(t, cnxn, table, reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			if n != int64(tc.batches*tc.rows) {
				t.Fatalf("ingested %d rows", n)
			}
			checkPartsTable(t, cnxn, table, int64(tc.batches*tc.rows))
			single, opens, parts, commits, _ := ingestRequests(proxy)
			if tc.wantParts == 0 {
				if single != 1 || opens != 0 || parts != 0 {
					t.Fatalf("want one stream: %d single, %d opens, %d parts", single, opens, parts)
				}
				return
			}
			if single != 0 || opens != 1 || parts != tc.wantParts || commits != 1 {
				t.Fatalf("want %d parts: %d single, %d opens, %d parts, %d commits", tc.wantParts, single, opens, parts, commits)
			}
		})
	}
}

// One part, or a one-batch upload, is one ordered stream (§8.1).
func TestIntegrationIngestOneStreamWhenAsked(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS ing_one_part; DROP TABLE IF EXISTS ing_one_batch")
	batches := partsBatches(16, 5_000)
	defer releaseAll(batches)
	reader := readerOf(t, batches)
	defer reader.Release()
	if _, err := ingestStream(t, cnxn, "ing_one_part", reader, map[string]string{OptionIngestParts: "1"}); err != nil {
		t.Fatal(err)
	}
	checkPartsTable(t, cnxn, "ing_one_part", 16*5_000)
	// The upload's order is kept: the table reads back in it.
	if got := int64s(t, queryTable(t, cnxn, "SELECT k FROM ing_one_part"), 0); !slices.IsSorted(got) || len(got) != 16*5_000 {
		t.Fatalf("the order was not kept")
	}
	one := partsBatches(1, 5_000)
	defer releaseAll(one)
	reader1 := readerOf(t, one)
	defer reader1.Release()
	if _, err := ingestStream(t, cnxn, "ing_one_batch", reader1, nil); err != nil {
		t.Fatal(err)
	}
	checkPartsTable(t, cnxn, "ing_one_batch", 5_000)
	single, opens, parts, _, _ := ingestRequests(proxy)
	if single != 2 || opens != 0 || parts != 0 {
		t.Fatalf("requests: %d single, %d opens, %d parts", single, opens, parts)
	}
}

// A server without partitioned_ingest gets one stream.
func TestIntegrationIngestWithoutTheCapability(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS ing_no_cap")
	stmt := newStmt(t, cnxn)
	discovery := *stmt.cnxn.discovery
	discovery.Capabilities = slices.DeleteFunc(slices.Clone(discovery.Capabilities),
		func(c string) bool { return c == protocol.CapabilityPartitionedIngest })
	stmt.cnxn.discovery = &discovery
	batches := partsBatches(16, 5_000)
	defer releaseAll(batches)
	reader := readerOf(t, batches)
	defer reader.Release()
	if err := stmt.SetOption(context.Background(), adbc.OptionKeyIngestTargetTable, "ing_no_cap"); err != nil {
		t.Fatal(err)
	}
	if err := stmt.BindStream(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkPartsTable(t, cnxn, "ing_no_cap", 16*5_000)
	if single, opens, _, _, _ := ingestRequests(proxy); single != 1 || opens != 0 {
		t.Fatalf("requests: %d single, %d opens", single, opens)
	}
}

// A failure in any part leaves nothing: no new table, no rows appended, and
// the ingest aborted on the server.
func TestIntegrationIngestInPartsFailureLeavesNothing(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS ing_parts_fail; DROP TABLE IF EXISTS ing_parts_kept")
	// More rows than the driver reads ahead to choose the parts (2 slices per part),
	// so the reader breaks after the parts are open.
	batches := partsBatches(80, 10_000)
	defer releaseAll(batches)

	// The application's reader breaks after 80 batches: create makes nothing.
	reader := &failingReader{RecordReader: readerOf(t, batches)}
	defer reader.Release()
	_, err := ingestStream(t, cnxn, "ing_parts_fail", reader, map[string]string{adbc.OptionKeyIngestMode: adbc.OptionValueIngestModeCreate})
	if err == nil || !strings.Contains(err.Error(), "the application's reader broke") {
		t.Fatalf("error %v", err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'ing_parts_fail'"); got != 0 {
		t.Fatalf("the failed ingest created its table")
	}
	if _, _, _, commits, aborts := ingestRequests(proxy); commits != 0 || aborts != 1 {
		t.Fatalf("%d commits, %d aborts", commits, aborts)
	}

	// Into an existing table, a broken upload appends nothing.
	kept := partsBatches(2, 1_000)
	defer releaseAll(kept)
	keptReader := readerOf(t, kept)
	defer keptReader.Release()
	if _, err := ingestStream(t, cnxn, "ing_parts_kept", keptReader, nil); err != nil {
		t.Fatal(err)
	}
	reader2 := &failingReader{RecordReader: readerOf(t, batches)}
	defer reader2.Release()
	if _, err := ingestStream(t, cnxn, "ing_parts_kept", reader2, map[string]string{adbc.OptionKeyIngestMode: adbc.OptionValueIngestModeAppend}); err == nil {
		t.Fatal("the broken append succeeded")
	}
	checkPartsTable(t, cnxn, "ing_parts_kept", 2_000)
	assertSessionUsable(t, cnxn)
}

// A part whose answer is lost is sent again with its key and answered from
// the first upload: no row twice.
func TestIntegrationIngestPartAnswerLost(t *testing.T) {
	proxy, cnxn := viaProxy(t, nil)
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS ing_parts_lost")
	proxy.add(&faultRule{match: isPost("/parts/1"), times: 1})
	batches := partsBatches(16, 5_000) // 80,000 rows: five slices, three parts
	defer releaseAll(batches)
	reader := readerOf(t, batches)
	defer reader.Release()
	n, err := ingestStream(t, cnxn, "ing_parts_lost", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 16*5_000 {
		t.Fatalf("ingested %d rows", n)
	}
	checkPartsTable(t, cnxn, "ing_parts_lost", 16*5_000)
	if _, _, parts, _, _ := ingestRequests(proxy); parts != 3+1 {
		t.Fatalf("%d part requests, want %d (one sent again)", parts, 3+1)
	}
}
