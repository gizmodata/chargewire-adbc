// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"net/url"
	"strings"

	"github.com/gizmodata/chargewire-adbc/go/internal/protocol"
)

// Connection strings saved before the protocol was renamed to Chargewire keep
// working: the old URI scheme, base path and option-key prefix are read as
// their current spellings, silently. Servers from before the rename keep
// working too: a database at the default base path whose server doesn't
// answer there speaks the old spelling if the server does (legacyDialect).
// This file is the only place that knows the old names.
const (
	legacyScheme       = "warp"
	legacyBasePath     = "/warp"
	legacyOptionPrefix = "adbc.warp."
	optionPrefix       = "adbc.chargewire."
)

// legacyDialect is protocol version 1 as servers from before the rename spell
// it. The driver reads no column-default schema metadata and no browser
// sign-in fragment, so their old keys need no translation.
var legacyDialect = protocol.Dialect{
	Protocol:        "warp-sql",
	Root:            legacyBasePath,
	HeaderPrefix:    "Warp-",
	ErrorTypePrefix: "urn:warp-sql:error:",
}

// fallbackFor is the spelling a client for base falls back to when the
// server doesn't answer discovery there: the old one at the default base
// path, none at a custom one.
func fallbackFor(base *url.URL) *protocol.Dialect {
	if base.Path != defaultBasePath {
		return nil
	}
	dialect := legacyDialect
	return &dialect
}

// currentBasePath maps the old default base path to the current one; any
// other path is returned unchanged.
func currentBasePath(path string) string {
	if path == legacyBasePath {
		return defaultBasePath
	}
	return path
}

// currentOptionKey returns key with an old option-key prefix replaced by the
// current one; any other key is returned unchanged.
func currentOptionKey(key string) string {
	if rest, ok := strings.CutPrefix(key, legacyOptionPrefix); ok {
		return optionPrefix + rest
	}
	return key
}

// currentOptions returns options with every old option key replaced by the
// current one. When a map holds both spellings of a key, the current one wins.
func currentOptions(options map[string]string) map[string]string {
	out := make(map[string]string, len(options))
	for key, value := range options {
		if current := currentOptionKey(key); current != key {
			if _, both := options[current]; both {
				continue
			}
			out[current] = value
			continue
		}
		out[key] = value
	}
	return out
}

// currentScheme returns the current URI scheme for an old one (in any case);
// any other scheme is returned unchanged.
func currentScheme(scheme string) string {
	if strings.EqualFold(scheme, legacyScheme) {
		return SchemeChargewire
	}
	return scheme
}
