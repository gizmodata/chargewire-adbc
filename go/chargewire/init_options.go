// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"slices"

	"github.com/apache/arrow-adbc/go/adbc"
)

// InitOptionOrder returns the keys of options, the connection options a
// caller set before the connection was initialized (the C API allows
// AdbcConnectionSetOption before AdbcConnectionInit), in the order to apply
// them once it is open: the current catalog, then the current schema, then
// the others by name.
//
// Options set before a connection opens are a set, not a sequence, and the
// C entry points keep them in a map. The current schema is looked up in the
// current catalog, so it must follow the catalog: applied first, a schema
// only the requested catalog has fails the connect, and one the server's
// default catalog also has is set there and then lost when the catalog is,
// leaving the connection in the requested catalog's default schema without an
// error. The generated cgo entry points (pkg/chargewire, and those of drivers
// built on this one) call this instead of ranging over the map, whose order
// Go randomizes.
func InitOptionOrder(options map[string]string) []string {
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if ra, rb := initOptionRank(a), initOptionRank(b); ra != rb {
			return ra - rb
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	return keys
}

func initOptionRank(key string) int {
	switch key {
	case adbc.OptionKeyCurrentCatalog:
		return 0
	case adbc.OptionKeyCurrentDbSchema:
		return 1
	}
	return 2
}
