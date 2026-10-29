// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Connection strings saved before the protocol was renamed to Chargewire:
// the old URI scheme and option keys read as their current spellings.

func TestLegacyURIScheme(t *testing.T) {
	for raw, want := range map[string]string{
		"warp://h:1":                      "https://h:1/chargewire",
		"WARP://h:1":                      "https://h:1/chargewire",
		"warp://127.0.0.1:8080?tls=false": "http://127.0.0.1:8080/chargewire",
		"warp://h:1/custom/prefix":        "https://h:1/custom/prefix",
	} {
		got, err := parseURI(raw)
		if err != nil || got.String() != want {
			t.Errorf("parseURI(%q) = %v, %v; want %s", raw, got, err, want)
		}
	}
	for _, raw := range []string{"warpx://h:1", "chargewire+warp://h:1"} {
		if _, err := parseURI(raw); err == nil {
			t.Errorf("parseURI(%q) accepted", raw)
		}
	}
}

func TestLegacyOptionKeys(t *testing.T) {
	for key, want := range map[string]string{
		"adbc.warp.statement.timeout_seconds": OptionStatementTimeout,
		"adbc.warp.tls.skip_verify":           OptionTLSSkipVerify,
		"adbc.warp.session.id":                OptionSessionID,
		OptionQueryTag:                        OptionQueryTag,
		"adbc.warpx.statement.timeout":        "adbc.warpx.statement.timeout",
		"uri":                                 "uri",
	} {
		if got := currentOptionKey(key); got != want {
			t.Errorf("currentOptionKey(%q) = %q, want %q", key, got, want)
		}
	}
	// Both spellings of one key: the current one wins, whatever the map order.
	got := currentOptions(map[string]string{"adbc.warp.retry.max_attempts": "9", OptionRetryMaxAttempts: "2", "uri": "u"})
	if len(got) != 2 || got[OptionRetryMaxAttempts] != "2" || got["uri"] != "u" {
		t.Errorf("currentOptions = %v", got)
	}
}

// A database, connection and statement configured only with the old spellings
// behave as with the current ones, end to end against the fake server.
func TestLegacyConnectionString(t *testing.T) {
	f := newFakeChargewire(t)
	ctx := context.Background()
	uri := "warp://" + strings.TrimPrefix(f.server.URL, "http://") + "?tls=false"
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: uri, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p",
		"adbc.warp.statement.query_tag": `{"job": "legacy"}`,
		"adbc.warp.retry.max_attempts":  "7",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if got, _ := db.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionRetryMaxAttempts); got != "7" {
		t.Errorf("%s = %q, want 7", OptionRetryMaxAttempts, got)
	}
	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cnxn.Close(ctx)
	values, err := f.query(t, cnxn)
	if err != nil {
		t.Fatal(err)
	}
	wantSequence(t, values, 1, f.batches)
	if got := f.lastSubmit().Get("Chargewire-Query-Tag"); got != `{"job":"legacy"}` {
		t.Errorf("Chargewire-Query-Tag %q", got)
	}

	// Set and read on a connection and a statement under the old keys.
	options := cnxn.(adbc.GetSetOptionsWithContext)
	if err := options.SetOption(ctx, "adbc.warp.statement.query_tag", `{"job": "conn"}`); err != nil {
		t.Fatal(err)
	}
	if got, _ := options.GetOption(ctx, "adbc.warp.statement.query_tag"); got != `{"job":"conn"}` {
		t.Errorf("connection reads back %q", got)
	}
	if got, _ := options.GetOption(ctx, "adbc.warp.session.id"); got != "s1" {
		t.Errorf("session id %q", got)
	}
	stmt, err := cnxn.NewStatement(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close(ctx)
	if err := stmt.SetOption(ctx, "adbc.warp.statement.query_tag", `{"job": "stmt"}`); err != nil {
		t.Fatal(err)
	}
	if got, _ := stmt.(adbc.GetSetOptionsWithContext).GetOption(ctx, OptionQueryTag); got != `{"job":"stmt"}` {
		t.Errorf("statement reads back %q", got)
	}
	_ = stmt.SetSqlQuery(ctx, "SELECT v")
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader.Release()
	if got := f.lastSubmit().Get("Chargewire-Query-Tag"); got != `{"job":"stmt"}` {
		t.Errorf("Chargewire-Query-Tag %q", got)
	}
}
