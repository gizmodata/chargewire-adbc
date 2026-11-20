// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
)

// The catalog, then the schema, then the others by name, every time: the map
// is ranged over many times, since Go starts each range at a random entry.
func TestInitOptionOrder(t *testing.T) {
	options := map[string]string{
		adbc.OptionKeyCurrentDbSchema: "reports",
		OptionStatementTimeout:        "30",
		OptionSessionCloseOnRelease:   "false",
		adbc.OptionKeyAutoCommit:      adbc.OptionValueDisabled,
		adbc.OptionKeyCurrentCatalog:  "lake",
		OptionIPCCompression:          "zstd",
	}
	want := []string{
		adbc.OptionKeyCurrentCatalog,
		adbc.OptionKeyCurrentDbSchema,
		OptionIPCCompression,
		OptionStatementTimeout,
		OptionSessionCloseOnRelease,
		adbc.OptionKeyAutoCommit,
	}
	if !slices.IsSorted(want[2:5]) {
		t.Fatalf("the test's own expectation isn't sorted after the catalog and schema: %v", want)
	}
	for range 200 {
		if got := InitOptionOrder(options); !slices.Equal(got, want) {
			t.Fatalf("InitOptionOrder = %v, want %v", got, want)
		}
	}
	if got := InitOptionOrder(map[string]string{adbc.OptionKeyCurrentDbSchema: "s"}); !slices.Equal(got, []string{adbc.OptionKeyCurrentDbSchema}) {
		t.Errorf("the schema alone: %v", got)
	}
	if got := InitOptionOrder(nil); len(got) != 0 {
		t.Errorf("no options: %v", got)
	}
	// One option under several names: the name a database would keep is
	// applied last, so it wins on a connection too.
	several := map[string]string{
		"adbc.chargewire.statement.timeout_seconds": "1",
		"adbc.warp.statement.timeout_seconds":       "2",
		OptionStatementTimeout:                      "3",
		"acme.query.timeout_seconds":                "4",
	}
	wantSeveral := []string{"adbc.warp.statement.timeout_seconds", "adbc.chargewire.statement.timeout_seconds",
		OptionStatementTimeout, "acme.query.timeout_seconds"}
	for range 50 {
		if got := InitOptionOrder(several); !slices.Equal(got, wantSeveral) {
			t.Fatalf("InitOptionOrder = %v, want %v", got, wantSeveral)
		}
	}
	host := &Host{Name: "acme", OptionPrefix: "acme."}
	if got := host.CurrentOptions(several); got[OptionStatementTimeout] != "4" {
		t.Errorf("a database keeps %q, want the host's 4 (the name applied last above)", got[OptionStatementTimeout])
	}
	if got := currentOptions(map[string]string{"adbc.chargewire.statement.timeout_seconds": "1",
		"adbc.warp.statement.timeout_seconds": "2"}); got[OptionStatementTimeout] != "1" {
		t.Errorf("a database keeps %q of two old names, want adbc.chargewire's 1 (applied last above)", got[OptionStatementTimeout])
	}
}

// The generated cgo entry points apply the options set before
// AdbcConnectionInit in InitOptionOrder. `make regenerate` writes the file
// again from driverbase's template, which ranges over the map; this fails
// when a regenerated file lost the change (scripts/patch_generated_exports.py
// re-applies it).
func TestGeneratedConnectionInitAppliesOptionsInOrder(t *testing.T) {
	source, err := os.ReadFile("../pkg/chargewire/driver.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "range chargewire.InitOptionOrder(conn.initArgs)") {
		t.Error("pkg/chargewire/driver.go doesn't apply the connection's init options in chargewire.InitOptionOrder")
	}
	if strings.Contains(string(source), "range conn.initArgs") {
		t.Error("pkg/chargewire/driver.go ranges over conn.initArgs: the current schema can be applied before the current catalog")
	}
}
