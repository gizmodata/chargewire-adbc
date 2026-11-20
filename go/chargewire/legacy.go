// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
)

// Connection strings saved before the protocol was renamed to Chargewire keep
// working: the old URI scheme and base path are read as their current
// spellings, silently. Option keys from before the ADBC driver style guide
// (adbc.chargewire.*, and adbc.warp.* from before the rename) are read as
// their current names (chargewire.*), with a warning on stderr the first time
// a process uses each. Servers from before the rename keep
// working too: a database at the default base path whose server doesn't
// answer there speaks the old spelling if the server does (legacyDialect).
// This file is the only place that knows the old names.
const (
	legacyScheme       = "warp"
	legacyBasePath     = "/warp"
	legacyOptionPrefix = "adbc.warp."
	oldOptionPrefix    = "adbc.chargewire."
	optionPrefix       = "chargewire."
)

// renamedOptions maps the part after an old prefix to the part after
// optionPrefix where the name changed beyond its prefix: statement behaviour
// moved under query.*, as the ADBC driver style guide groups it.
var renamedOptions = map[string]string{
	"statement.async":             "query.async",
	"statement.retain_result":     "query.retain_result",
	"statement.timeout_seconds":   "query.timeout_seconds",
	"statement.query_tag":         "query.tag",
	"statement.result_partitions": "query.result_partitions",
	"statement.result_order":      "query.result_order",
	"statement.id":                "query.id",
	"resume_timeout":              "retry.resume_timeout_seconds",
	"ipc_compression":             "ipc.compression",
}

// knownOptions are the current names of the driver's own options. An old key
// is renamed only to one of these; any other is left as given, so the error
// for an unknown option names the key the caller wrote.
var knownOptions = map[string]bool{
	OptionTLSSkipVerify: true, OptionTLSRootCerts: true, OptionTLSClientCert: true,
	OptionTLSClientKey: true, OptionTLSServerName: true, OptionAuthType: true,
	OptionOAuthBrowser: true, OptionOAuthTimeoutSeconds: true, OptionAccessToken: true,
	OptionAccessTokenExpiresAt: true, OptionSubjectToken: true, OptionRetryMaxAttempts: true,
	OptionResumeTimeout: true, OptionSessionReconnect: true, OptionSessionCloseOnRelease: true,
	OptionSessionID: true, OptionSessionReplacements: true, OptionSessionTransaction: true,
	OptionAsync: true, OptionRetainResult: true, OptionStatementTimeout: true,
	OptionQueryTag: true, OptionIPCCompression: true, OptionResultPartitions: true,
	OptionResultOrder: true, OptionIngestParts: true, OptionStatementID: true,
}

// RenamedOption returns the current name of an option key from before the
// ADBC driver style guide (adbc.chargewire.*, or adbc.warp.*) and true; for
// any other key, including an old-prefixed key that names no option, it
// returns key and false. It does not warn: drivers built on this one use it
// to rename keys under their own names and warn in their own terms.
func RenamedOption(key string) (string, bool) {
	for _, prefix := range [...]string{oldOptionPrefix, legacyOptionPrefix} {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		if renamed, ok := renamedOptions[rest]; ok {
			rest = renamed
		}
		if current := optionPrefix + rest; knownOptions[current] {
			return current, true
		}
		return key, false
	}
	return key, false
}

// deprecationOutput is where deprecation warnings go; tests replace it.
var (
	deprecationMu     sync.Mutex
	deprecationOutput io.Writer = os.Stderr
	deprecationWarned sync.Map  // old key → struct{}
)

// warnDeprecatedOption writes, once per process for each old key, that the
// key is deprecated and which to use instead. It names keys only: option
// values (passwords, tokens) never appear in it.
func warnDeprecatedOption(driver, old, current string) {
	if _, seen := deprecationWarned.LoadOrStore(driver+"\x00"+old, struct{}{}); seen {
		return
	}
	deprecationMu.Lock()
	defer deprecationMu.Unlock()
	fmt.Fprintf(deprecationOutput, "[%s] option %q is deprecated; use %q\n", driver, old, current)
}

// SetDeprecationOutput sends deprecation warnings to w from now on (os.Stderr
// by default) and forgets which were sent, so each warns once again; it
// returns the previous writer. For tests, of this driver and of drivers built
// on it.
func SetDeprecationOutput(w io.Writer) io.Writer {
	deprecationMu.Lock()
	defer deprecationMu.Unlock()
	previous := deprecationOutput
	deprecationOutput = w
	deprecationWarned.Clear()
	return previous
}

// WarnDeprecatedOption is warnDeprecatedOption for drivers built on this one,
// which rename keys under their own names (see RenamedOption): driver is
// their name as it prefixes their error messages.
func WarnDeprecatedOption(driver, old, current string) {
	warnDeprecatedOption(driver, old, current)
}

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

// Host is a driver built on this one under its own name, such as
// Hardcharger's: it accepts every option chargewire.X as OptionPrefix+X too,
// and its deprecation warnings name that spelling.
type Host struct {
	// Name is its slug, as it prefixes its error messages ("hardcharger").
	Name string
	// OptionPrefix is its option-key prefix ("hardcharger.").
	OptionPrefix string
	// Own reports whether a key under OptionPrefix is one of the host's own
	// options (nil: none are). Such a key is never taken as this driver's
	// option of the same name, and a deprecation warning for that option of
	// this driver suggests chargewire.X, not the host's spelling.
	Own func(key string) bool
}

func (h *Host) owns(key string) bool { return h != nil && h.Own != nil && h.Own(key) }

func (h *Host) name() string {
	if h == nil {
		return driverName
	}
	return h.Name
}

// spelling is the name a deprecation warning suggests for current
// (chargewire.X): the host's OptionPrefix+X, or current itself.
func (h *Host) spelling(current string) string {
	if h == nil {
		return current
	}
	spelled := h.OptionPrefix + strings.TrimPrefix(current, optionPrefix)
	if h.owns(spelled) {
		return current
	}
	return spelled
}

// CurrentOptionKey returns the name this driver reads for key: chargewire.X
// for the host's OptionPrefix+X when chargewire.X is one of its options, and
// the current name for one from before the ADBC driver style guide
// (RenamedOption), with a warning the first time the process uses it, in the
// host's terms. Any other key is returned unchanged. A nil Host is this
// driver itself.
func (h *Host) CurrentOptionKey(key string) string {
	current, _ := h.currentOptionKey(key)
	return current
}

// currentOptionKey is CurrentOptionKey, also saying how key was spelled:
// 0 the host's spelling, 1 this driver's current name (or not an option of
// its), 2 an old name.
func (h *Host) currentOptionKey(key string) (string, int) {
	if current, renamed := RenamedOption(key); renamed {
		warnDeprecatedOption(h.name(), key, h.spelling(current))
		return current, 2
	}
	if h != nil {
		if rest, ok := strings.CutPrefix(key, h.OptionPrefix); ok && knownOptions[optionPrefix+rest] && !h.owns(key) {
			return optionPrefix + rest, 0
		}
	}
	return key, 1
}

// CurrentOptions returns options under the names CurrentOptionKey gives.
// When several keys name one option, the host's spelling wins over this
// driver's, and either over an old name (which still warns).
func (h *Host) CurrentOptions(options map[string]string) map[string]string {
	out := make(map[string]string, len(options))
	rank := make(map[string]int, len(options))
	for _, key := range slices.Sorted(maps.Keys(options)) {
		current, r := h.currentOptionKey(key)
		if previous, seen := rank[current]; seen && previous <= r {
			continue
		}
		out[current], rank[current] = options[key], r
	}
	return out
}

// currentOptionKey is CurrentOptionKey for this driver itself.
func currentOptionKey(key string) string { return (*Host)(nil).CurrentOptionKey(key) }

// currentOptions is CurrentOptions for this driver itself.
func currentOptions(options map[string]string) map[string]string {
	return (*Host)(nil).CurrentOptions(options)
}

// currentScheme returns the current URI scheme for an old one (in any case);
// any other scheme is returned unchanged.
func currentScheme(scheme string) string {
	if strings.EqualFold(scheme, legacyScheme) {
		return SchemeChargewire
	}
	return scheme
}
