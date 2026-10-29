// Licensed under the Apache License, Version 2.0.

package chargewire

import "strings"

// Connection strings saved before the protocol was renamed to Chargewire keep
// working: the old URI scheme and the old option-key prefix are read as their
// current spellings, silently. This file is the only place that knows them.
const (
	legacyScheme       = "warp"
	legacyOptionPrefix = "adbc.warp."
	optionPrefix       = "adbc.chargewire."
)

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
