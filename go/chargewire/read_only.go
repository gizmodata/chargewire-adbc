// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
)

type readOnlyKey struct{}

// WithReadOnly returns a context for Database.Open that requests a read-only
// session in its creation request, before any setup hook or statement runs.
// A server that does not confirm the option makes opening the session fail.
// For a lazy connection, that confirmation happens at the first operation.
func WithReadOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, readOnlyKey{}, true)
}

// ConnectionInitContext carries options known before AdbcConnectionInit into
// Database.Open. C entry points must use this before opening a connection;
// applying read-only afterwards would create an initially writable session.
func ConnectionInitContext(ctx context.Context, options map[string]string) (context.Context, error) {
	if value, ok := options[adbc.OptionKeyReadOnly]; ok {
		enabled, err := parseBool(adbc.OptionKeyReadOnly, value)
		if err != nil {
			return nil, err
		}
		if enabled {
			ctx = WithReadOnly(ctx)
		}
	}
	return ctx, nil
}

// SetInitOption stores a C connection option before initialization. Read-only
// remains one-way even before the server session exists.
func SetInitOption(options map[string]string, key, value string) error {
	if key == adbc.OptionKeyReadOnly {
		enabled, err := parseBool(key, value)
		if err != nil {
			return err
		}
		previous, _ := parseBool(key, options[key])
		if previous && !enabled {
			return adbc.Error{Code: adbc.StatusInvalidState, Msg: "read-only is one-way; open another connection to write"}
		}
	}
	options[key] = value
	return nil
}
