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
		OptionSessionCloseOnRelease,
		OptionStatementTimeout,
		adbc.OptionKeyAutoCommit,
	}
	if !slices.IsSorted(want[2:]) {
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
