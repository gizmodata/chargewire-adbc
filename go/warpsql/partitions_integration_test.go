// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Partitioned results (§6.10): the server splits a large result, and the
// driver reads every partition at once and merges them.

func splitDB(t *testing.T, server *testServer, partitions, order string, extra map[string]string) adbc.ConnectionWithContext {
	t.Helper()
	options := map[string]string{OptionResultPartitions: partitions, OptionResultOrder: order}
	for key, value := range extra {
		options[key] = value
	}
	return connect(t, openDB(t, server, options))
}

func int64Column(t *testing.T, table arrow.Table, name string) []int64 {
	t.Helper()
	indexes := table.Schema().FieldIndices(name)
	if len(indexes) != 1 {
		t.Fatalf("no column %q in %s", name, table.Schema())
	}
	var out []int64
	for _, chunk := range table.Column(indexes[0]).Data().Chunks() {
		values := chunk.(*array.Int64)
		for i := 0; i < values.Len(); i++ {
			out = append(out, values.Value(i))
		}
	}
	return out
}

func descending(n int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = n - 1 - int64(i)
	}
	return out
}

func TestIntegrationSplitResultKeepsOrder(t *testing.T) {
	for _, async := range []string{"false", "true"} {
		t.Run("async="+async, func(t *testing.T) {
			cnxn := splitDB(t, shared, "4", "preserve", map[string]string{OptionAsync: async})
			table := queryTable(t, cnxn, "SELECT i, i * 2 AS j FROM range(1000000) t(i) ORDER BY i DESC")
			defer table.Release()
			if got := int64Column(t, table, "i"); !slices.Equal(got, descending(1000000)) {
				t.Fatalf("the split result is out of order (%d rows)", len(got))
			}
			// It really was split.
			stmt := newStmt(t, cnxn)
			if err := stmt.SetSqlQuery(context.Background(), "SELECT i FROM range(1000000) t(i)"); err != nil {
				t.Fatal(err)
			}
			reader, _, err := stmt.ExecuteQuery(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			if p, ok := reader.(*partitionedReader); !ok || (p.feeds == nil && p.merged == nil) {
				t.Fatalf("a million rows were not split: %T", reader)
			}
		})
	}
}

func TestIntegrationSplitResultHasEveryRowOnce(t *testing.T) {
	cnxn := splitDB(t, shared, "8", "any", nil)
	table := queryTable(t, cnxn, "SELECT i FROM range(2000000) t(i)")
	defer table.Release()
	got := int64Column(t, table, "i")
	slices.Sort(got)
	want := make([]int64, 2000000)
	for i := range want {
		want[i] = int64(i)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %d rows, not each of 0..1999999 once", len(got))
	}
}

func TestIntegrationSplitResultKeepsTypesAndNulls(t *testing.T) {
	sql := "SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END AS n, " +
		"CASE WHEN i % 5 = 0 THEN NULL ELSE 'row ' || i END AS s, (i % 1000)::DECIMAL(18, 3) AS d, " +
		"TIMESTAMP '2026-01-01' + to_seconds(i) AS ts, [i, NULL] AS l, {'a': i} AS st " +
		"FROM range(400000) t(i) ORDER BY i"
	whole := queryTable(t, openConn(t), sql)
	defer whole.Release()
	split := queryTable(t, splitDB(t, shared, "5", "preserve", nil), sql)
	defer split.Release()
	if !array.TableEqual(whole, split) {
		t.Fatal("the split result differs from the whole one")
	}
}

func TestIntegrationSplitResultWithEmptyPartitions(t *testing.T) {
	// Two batches' worth of rows split eight ways leaves partitions empty.
	probe := queryTable(t, openConn(t), "SELECT i FROM range(1000000) t(i)")
	batchRows := probe.Column(0).Data().Chunk(0).Len()
	probe.Release()
	n := int64(2 * batchRows)
	table := queryTable(t, splitDB(t, shared, "8", "preserve", nil), "SELECT i FROM range("+itoa(n)+") t(i) ORDER BY i DESC")
	defer table.Release()
	if got := int64Column(t, table, "i"); !slices.Equal(got, descending(n)) {
		t.Fatalf("got %d rows out of order, want %d", len(got), n)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestIntegrationSplitResultFailureFailsTheWhole(t *testing.T) {
	cnxn := splitDB(t, shared, "4", "preserve", nil)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(),
		"SELECT CASE WHEN i < 30000000 THEN i ELSE error('boom') END AS v FROM range(100000000) t(i)"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err == nil {
		rows := 0
		for reader.Next() {
			rows += int(reader.RecordBatch().NumRows())
		}
		err = reader.Err()
		reader.Release()
		if err == nil {
			t.Fatalf("a failing statement's split result ended cleanly after %d rows", rows)
		}
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("the error does not report the statement's failure: %v", err)
	}
}

func TestIntegrationSplitResultCancel(t *testing.T) {
	cnxn := splitDB(t, shared, "4", "any", nil)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT i, i % 7 AS m FROM range(3000000000) t(i) WHERE i % 3 = 0"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatalf("no first batch: %v", reader.Err())
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = stmt.Cancel(context.Background())
	}()
	for reader.Next() {
	}
	err = reader.Err()
	var adbcErr adbc.Error
	if err == nil || !errors.As(err, &adbcErr) || adbcErr.Code != adbc.StatusCancelled {
		t.Fatalf("a cancelled split result ended with %v, want a cancellation", err)
	}
}

func TestIntegrationSplitResultReleasedEarly(t *testing.T) {
	cnxn := splitDB(t, shared, "4", "preserve", nil)
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), "SELECT i FROM range(3000000000) t(i)"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3 && reader.Next(); i++ {
	}
	done := make(chan struct{})
	go func() {
		reader.Release() // stops every partition's download; the checked allocator sees nothing leak
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("releasing a split result early did not return")
	}
	// The connection still works.
	table := queryTable(t, cnxn, "SELECT 42 AS answer")
	table.Release()
}

func TestIntegrationSplitResultOverTLS(t *testing.T) {
	ca := newTestCA(t)
	server := startServer(t, "--tls-cert", ca.serverCertFile, "--tls-key", ca.serverKeyFile)
	cnxn := splitDB(t, server, "4", "preserve", map[string]string{OptionTLSRootCerts: ca.caPEM})
	table := queryTable(t, cnxn, "SELECT i FROM range(1000000) t(i) ORDER BY i DESC")
	defer table.Release()
	if got := int64Column(t, table, "i"); !slices.Equal(got, descending(1000000)) {
		t.Fatal("the split result over TLS is out of order")
	}
}

func TestIntegrationSplitOptionsAreValidated(t *testing.T) {
	ctx := context.Background()
	stmt := newStmt(t, openConn(t))
	for key, value := range map[string]string{OptionResultPartitions: "0", OptionResultOrder: "sorted"} {
		if err := stmt.SetOption(ctx, key, value); err == nil {
			t.Errorf("%s=%s was accepted", key, value)
		}
	}
	if err := stmt.SetOption(ctx, OptionResultPartitions, "6"); err != nil {
		t.Fatal(err)
	}
	if got, _ := stmt.GetOption(ctx, OptionResultPartitions); got != "6" {
		t.Fatalf("result_partitions reads back %q", got)
	}
}
