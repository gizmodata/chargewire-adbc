// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// legacyServerBinaryEnv names a server from before the protocol was renamed
// to Chargewire (warp-duckdb, password in WARP_DUCKDB_PASSWORD, discovery at
// /warp). Without it the tests that need one skip.
const legacyServerBinaryEnv = "CHARGEWIRE_ADBC_TEST_LEGACY_SERVER_BINARY"

func startLegacyServer(t *testing.T) *testServer {
	t.Helper()
	binary := os.Getenv(legacyServerBinaryEnv)
	if binary == "" {
		t.Skipf("set %s to a warp-duckdb binary to test servers from before the rename", legacyServerBinaryEnv)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("%s: %v", legacyServerBinaryEnv, err)
	}
	integrationTests.Add(1)
	server, err := launchServerAt(binary, nil, "WARP_DUCKDB_PASSWORD", "/warp")
	if err != nil {
		t.Fatalf("starting %s: %v", binary, err)
	}
	t.Cleanup(server.stop)
	return server
}

// A connection string saved before the protocol was renamed to Chargewire (the
// warp:// scheme, adbc.warp.* options) connects to a live server, and its
// options take effect.
func TestIntegrationLegacyConnectionString(t *testing.T) {
	uri := "warp://" + shared.host + "?tls=false"
	if shared.scheme == "https" {
		t.Fatal("the shared server is expected to speak plain HTTP")
	}
	db := openDB(t, shared, map[string]string{
		adbc.OptionKeyURI:                     uri,
		"adbc.warp.statement.timeout_seconds": "0.5",
	})
	cnxn := connect(t, db)
	if got := scalarInt(t, cnxn, "SELECT 42"); got != 42 {
		t.Fatalf("SELECT 42 = %d", got)
	}
	options := cnxn.(adbc.GetSetOptionsWithContext)
	old, _ := options.GetOption(context.Background(), "adbc.warp.session.id")
	current, _ := options.GetOption(context.Background(), OptionSessionID)
	if old == "" || old != current {
		t.Errorf("session id under the old key %q, under the current one %q", old, current)
	}
	// The timeout set under its old key is in force: the server stops a long statement.
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, longQuery)
	_, err := stmt.ExecuteUpdate(context.Background())
	assertStatus(t, err, adbc.StatusTimeout, "statement_timeout")
	assertSessionUsable(t, cnxn)
}

// A server from before the rename, at the default base path: the driver falls
// back to its spelling, and queries, parameters, bulk ingest, errors,
// statement options and metadata all work through it.
func TestIntegrationLegacyServer(t *testing.T) {
	server := startLegacyServer(t)
	for _, uri := range []string{"chargewire://" + server.host + "?tls=false", "warp://" + server.host + "/warp?tls=false"} {
		t.Run(uri[:strings.Index(uri, ":")], func(t *testing.T) {
			cnxn := connect(t, openDB(t, server, map[string]string{adbc.OptionKeyURI: uri}))
			if got := scalarInt(t, cnxn, "SELECT 42"); got != 42 {
				t.Fatalf("SELECT 42 = %d", got)
			}
		})
	}
	cnxn := connect(t, openDB(t, server, nil))

	// Prepared statement with parameters.
	stmt := newStmt(t, cnxn)
	setQuery(t, stmt, "SELECT ? * 2 AS doubled")
	if err := stmt.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, v := range []int32{1, 21} {
		bindInt32(t, stmt, v)
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got := int64s(t, readAll(t, reader), 0); got[0] != int64(v)*2 {
			t.Errorf("%d * 2 = %d", v, got[0])
		}
	}
	// The statement id comes back in the old spelling's response header.
	if id, _ := stmt.GetOption(context.Background(), OptionStatementID); id == "" {
		t.Error("no statement id")
	}

	// Bulk ingest.
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS legacy_ingest")
	rec := keyValueBatch([]int64{1, 2, 3}, []string{"alice", "bob", "carol"})
	defer rec.Release()
	if n, err := ingest(t, cnxn, "legacy_ingest", adbc.OptionValueIngestModeCreate, rec, nil); err != nil || n != 3 {
		t.Fatalf("ingest: %d rows, %v", n, err)
	}
	if got := stringsOf(t, queryTable(t, cnxn, "SELECT v FROM legacy_ingest ORDER BY k"), 0); len(got) != 3 || got[2] != "carol" {
		t.Fatalf("ingested %v", got)
	}

	// A SQL error keeps its status, code and SQLSTATE.
	err := runFailing(t, cnxn, "SELECT * FROM legacy_no_such_table")
	assertStatus(t, err, adbc.StatusNotFound, "sql_error")
	if adbcErr, _ := errors.AsType[adbc.Error](err); strings.TrimRight(string(adbcErr.SqlState[:]), "\x00") != "42P01" {
		t.Errorf("SQLSTATE %q (%v)", adbcErr.SqlState, err)
	}

	// A statement timeout set as an option goes out in the old spelling's
	// request header, and the server enforces it.
	slow := newStmt(t, cnxn)
	if err := slow.SetOption(context.Background(), OptionStatementTimeout, "0.5"); err != nil {
		t.Fatal(err)
	}
	setQuery(t, slow, longQuery)
	_, err = slow.ExecuteUpdate(context.Background())
	assertStatus(t, err, adbc.StatusTimeout, "statement_timeout")
	assertSessionUsable(t, cnxn)

	// Metadata.
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE legacy_meta (id INTEGER PRIMARY KEY, note VARCHAR)")
	infos := getObjects(t, cnxn, adbc.ObjectDepthAll, nil, nil, new("legacy_meta"), nil, nil)
	found := false
	for i := range infos {
		for _, schema := range infos[i].CatalogDbSchemas {
			for _, table := range schema.DbSchemaTables {
				if table.TableName == "legacy_meta" && len(table.TableColumns) == 2 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Errorf("GetObjects did not list legacy_meta with its two columns: %+v", infos)
	}
	schema, err := cnxn.GetTableSchema(context.Background(), nil, nil, "legacy_meta")
	if err != nil {
		t.Fatal(err)
	}
	if schema.NumFields() != 2 || schema.Field(1).Name != "note" {
		t.Errorf("schema %s", schema)
	}
}

// A current server is never asked for the old spelling: every request a
// connection makes, through a recording proxy, is under /chargewire.
func TestIntegrationLegacyNoOldPathsOnCurrentServer(t *testing.T) {
	target, _ := url.Parse("http://" + shared.host)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	var paths []string
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		proxy.ServeHTTP(w, r)
	}))
	defer front.Close()
	cnxn := connect(t, openDB(t, shared, map[string]string{
		adbc.OptionKeyURI: "chargewire://" + strings.TrimPrefix(front.URL, "http://") + "?tls=false"}))
	if got := scalarInt(t, cnxn, "SELECT 42"); got != 42 {
		t.Fatalf("SELECT 42 = %d", got)
	}
	execUpdate(t, cnxn, "DROP TABLE IF EXISTS legacy_proxy_ingest")
	rec := keyValueBatch([]int64{1}, []string{"x"})
	defer rec.Release()
	if _, err := ingest(t, cnxn, "legacy_proxy_ingest", adbc.OptionValueIngestModeCreate, rec, nil); err != nil {
		t.Fatal(err)
	}
	_ = runFailing(t, cnxn, "SELECT * FROM legacy_no_such_table")
	mu.Lock()
	defer mu.Unlock()
	discoveries := 0
	for _, path := range paths {
		if path == "/chargewire" {
			discoveries++
		}
		if path != "/chargewire" && !strings.HasPrefix(path, "/chargewire/") {
			t.Errorf("request to %s", path)
		}
	}
	if discoveries != 1 || len(paths) < 5 {
		t.Errorf("%d discoveries in %d requests: %v", discoveries, len(paths), paths)
	}
}
