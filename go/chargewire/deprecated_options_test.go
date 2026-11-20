// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// captureDeprecations sends deprecation warnings to a buffer, with a fresh
// "already warned" set, for the rest of the test.
func captureDeprecations(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	previous := SetDeprecationOutput(&out)
	t.Cleanup(func() { SetDeprecationOutput(previous) })
	return &out
}

// Every option from before the style guide, under both old prefixes, renames
// to its current constant.
func TestRenamedOptionTable(t *testing.T) {
	for old, want := range map[string]string{
		"tls.skip_verify":              OptionTLSSkipVerify,
		"tls.root_certs":               OptionTLSRootCerts,
		"tls.client_cert":              OptionTLSClientCert,
		"tls.client_key":               OptionTLSClientKey,
		"tls.server_name":              OptionTLSServerName,
		"auth.type":                    OptionAuthType,
		"oauth.browser":                OptionOAuthBrowser,
		"oauth.timeout_seconds":        OptionOAuthTimeoutSeconds,
		"auth.access_token":            OptionAccessToken,
		"auth.access_token_expires_at": OptionAccessTokenExpiresAt,
		"auth.subject_token":           OptionSubjectToken,
		"retry.max_attempts":           OptionRetryMaxAttempts,
		"resume_timeout":               OptionResumeTimeout,
		"session.reconnect":            OptionSessionReconnect,
		"session.close_on_release":     OptionSessionCloseOnRelease,
		"session.id":                   OptionSessionID,
		"session.replacements":         OptionSessionReplacements,
		"session.transaction":          OptionSessionTransaction,
		"statement.async":              OptionAsync,
		"statement.retain_result":      OptionRetainResult,
		"statement.timeout_seconds":    OptionStatementTimeout,
		"statement.query_tag":          OptionQueryTag,
		"ipc_compression":              OptionIPCCompression,
		"statement.result_partitions":  OptionResultPartitions,
		"statement.result_order":       OptionResultOrder,
		"ingest.parts":                 OptionIngestParts,
		"statement.id":                 OptionStatementID,
	} {
		for _, prefix := range []string{"adbc.chargewire.", "adbc.warp."} {
			if got, renamed := RenamedOption(prefix + old); got != want || !renamed {
				t.Errorf("RenamedOption(%q) = %q, %v; want %q, true", prefix+old, got, renamed, want)
			}
		}
	}
	if len(knownOptions) != 27 {
		t.Errorf("knownOptions has %d entries; the table above lists 27", len(knownOptions))
	}
	for _, key := range []string{
		OptionQueryTag, "uri", "adbc.connection.readonly", "adbc.statement.bind_by_name",
		"adbc.chargewire.nonsense", "adbc.warpx.statement.timeout", "chargewire.statement.timeout_seconds",
	} {
		if got, renamed := RenamedOption(key); got != key || renamed {
			t.Errorf("RenamedOption(%q) = %q, %v; want it unchanged", key, got, renamed)
		}
	}
	// The current names are the style guide's: no adbc. prefix.
	for key := range knownOptions {
		if !strings.HasPrefix(key, "chargewire.") {
			t.Errorf("option %q doesn't start with chargewire.", key)
		}
	}
}

// An old key warns once per process, names both keys and never the value, and
// sets and reads the same option as the current key; when both are given the
// current one wins.
func TestDeprecatedOptionWarnsOnce(t *testing.T) {
	out := captureDeprecations(t)
	ctx := context.Background()
	const secret = "s3cr3t-token-value-never-logged"
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI:                     "chargewire://127.0.0.1:1?tls=false",
		"adbc.chargewire.auth.access_token":   secret,
		"adbc.chargewire.retry.max_attempts":  "9",
		OptionRetryMaxAttempts:                "4",
		"adbc.chargewire.resume_timeout":      "12",
		"adbc.chargewire.statement.query_tag": `{"job": "old"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	options := db.(adbc.GetSetOptionsWithContext)
	for _, key := range []string{OptionRetryMaxAttempts, "adbc.chargewire.retry.max_attempts"} {
		if got, _ := options.GetOption(ctx, key); got != "4" {
			t.Errorf("%s = %q, want the current key's 4", key, got)
		}
	}
	for _, key := range []string{OptionResumeTimeout, "adbc.chargewire.resume_timeout", "adbc.warp.resume_timeout"} {
		if got, _ := options.GetOption(ctx, key); got != "12" {
			t.Errorf("%s = %q, want 12", key, got)
		}
	}
	if err := options.SetOption(ctx, "adbc.chargewire.resume_timeout", "13"); err != nil {
		t.Fatal(err)
	}
	if got, _ := options.GetOption(ctx, OptionResumeTimeout); got != "13" {
		t.Errorf("%s = %q after setting the old key, want 13", OptionResumeTimeout, got)
	}

	warnings := out.String()
	if strings.Contains(warnings, secret) {
		t.Fatalf("a deprecation warning printed an option value: %q", warnings)
	}
	for old, current := range map[string]string{
		"adbc.chargewire.auth.access_token":   OptionAccessToken,
		"adbc.chargewire.retry.max_attempts":  OptionRetryMaxAttempts,
		"adbc.chargewire.resume_timeout":      OptionResumeTimeout,
		"adbc.chargewire.statement.query_tag": OptionQueryTag,
		"adbc.warp.resume_timeout":            OptionResumeTimeout,
	} {
		line := `[chargewire] option "` + old + `" is deprecated; use "` + current + `"` + "\n"
		if n := strings.Count(warnings, line); n != 1 {
			t.Errorf("warning %q printed %d times, want once; all: %q", line, n, warnings)
		}
	}
	// adbc.chargewire.resume_timeout was used three times (create, get,
	// set) and warned once; the old max_attempts, given alongside the current
	// key and dropped for it, still warns: the caller is using it.
	if got := strings.Count(warnings, "\n"); got != 5 {
		t.Errorf("%d warning lines, want 5: %q", got, warnings)
	}
}

// Current names never warn.
func TestCurrentOptionNamesDontWarn(t *testing.T) {
	out := captureDeprecations(t)
	for key := range knownOptions {
		if got := currentOptionKey(key); got != key {
			t.Errorf("currentOptionKey(%q) = %q", key, got)
		}
	}
	if out.Len() != 0 {
		t.Errorf("current names warned: %q", out.String())
	}
}

// A driver built on this one under its own name: every option is accepted
// under its prefix too, on a database, connection and statement; old names
// warn in its terms; its spelling wins over the others; unknown keys keep the
// spelling the caller wrote (the error names it).
func TestHostOptionNames(t *testing.T) {
	out := captureDeprecations(t)
	host := &Host{Name: "hardcharger", OptionPrefix: "hardcharger."}
	for key, want := range map[string]string{
		"hardcharger.query.timeout_seconds":         OptionStatementTimeout,
		"hardcharger.tls.root_certs":                OptionTLSRootCerts,
		"hardcharger.retry.resume_timeout_seconds":  OptionResumeTimeout,
		OptionIngestParts:                           OptionIngestParts,
		"adbc.chargewire.statement.timeout_seconds": OptionStatementTimeout,
		"hardcharger.nonsense":                      "hardcharger.nonsense",
		"hardcharger.statement.timeout_seconds":     "hardcharger.statement.timeout_seconds",
		"adbc.hardcharger.query_tag":                "adbc.hardcharger.query_tag",
		"adbc.chargewire.tls.no_such_option":        "adbc.chargewire.tls.no_such_option",
	} {
		if got := host.CurrentOptionKey(key); got != want {
			t.Errorf("CurrentOptionKey(%q) = %q, want %q", key, got, want)
		}
	}
	if got, want := out.String(), `[hardcharger] option "adbc.chargewire.statement.timeout_seconds" is deprecated; use "hardcharger.query.timeout_seconds"`+"\n"; got != want {
		t.Errorf("warnings %q, want %q", got, want)
	}
	for range 50 { // map order is random: the winner must not depend on it
		got := host.CurrentOptions(map[string]string{
			"adbc.chargewire.statement.timeout_seconds": "1",
			OptionStatementTimeout:                      "2",
			"hardcharger.query.timeout_seconds":         "3",
			"adbc.warp.retry.max_attempts":              "4",
			OptionRetryMaxAttempts:                      "5",
		})
		if len(got) != 2 || got[OptionStatementTimeout] != "3" || got[OptionRetryMaxAttempts] != "5" {
			t.Fatalf("CurrentOptions = %v", got)
		}
	}

	ctx := context.Background()
	db, err := NewDriverForHost(memory.DefaultAllocator, "Host - Go", *host).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI:                   "chargewire://127.0.0.1:1?tls=false",
		"hardcharger.query.timeout_seconds": "7",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	options := db.(adbc.GetSetOptionsWithContext)
	for _, key := range []string{"hardcharger.query.timeout_seconds", OptionStatementTimeout} {
		if got, _ := options.GetOption(ctx, key); got != "7" {
			t.Errorf("%s = %q, want 7", key, got)
		}
	}
	if err := options.SetOption(ctx, "hardcharger.nonsense", "1"); err == nil || !strings.Contains(err.Error(), "hardcharger.nonsense") {
		t.Errorf("an unknown key under the host's prefix: %v", err)
	}
	// This driver itself doesn't take the host's spelling.
	plain, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: "chargewire://127.0.0.1:1?tls=false", "hardcharger.query.timeout_seconds": "7",
	})
	if err == nil {
		plain.Close(ctx)
		t.Error("the Chargewire driver accepted hardcharger.query.timeout_seconds")
	}
}

// A host's own options under its prefix are never this driver's, and a
// warning for this driver's option of the same name doesn't send the caller
// to the host's.
func TestHostOwnOptions(t *testing.T) {
	out := captureDeprecations(t)
	host := &Host{Name: "acme", OptionPrefix: "acme.", Own: func(key string) bool {
		return key == "acme.auth.type" || strings.HasPrefix(key, "acme.oauth.")
	}}
	for key, want := range map[string]string{
		"acme.auth.type":                  "acme.auth.type",
		"acme.oauth.browser":              "acme.oauth.browser",
		"acme.tls.root_certs":             OptionTLSRootCerts,
		"adbc.chargewire.auth.type":       OptionAuthType,
		"adbc.chargewire.tls.server_name": OptionTLSServerName,
	} {
		if got := host.CurrentOptionKey(key); got != want {
			t.Errorf("CurrentOptionKey(%q) = %q, want %q", key, got, want)
		}
	}
	for _, line := range []string{
		`[acme] option "adbc.chargewire.auth.type" is deprecated; use "chargewire.auth.type"`,
		`[acme] option "adbc.chargewire.tls.server_name" is deprecated; use "acme.tls.server_name"`,
	} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Errorf("no warning %q in %q", line, out.String())
		}
	}
}

// The host's spelling of the TLS options reaches a browser sign-in: an
// unreachable server is refused with the host's server name in effect, not
// ignored. (Checked by the options it reads, without a server.)
func TestHostBrowserSignInReadsHostSpelling(t *testing.T) {
	host := &Host{Name: "acme", OptionPrefix: "acme."}
	got := host.CurrentOptions(map[string]string{"acme.tls.root_certs": "PEM", "acme.retry.resume_timeout_seconds": "0"})
	if got[OptionTLSRootCerts] != "PEM" || got[OptionResumeTimeout] != "0" {
		t.Fatalf("CurrentOptions = %v", got)
	}
	_, err := host.BrowserSignIn(context.Background(), map[string]string{
		adbc.OptionKeyURI: "chargewire://127.0.0.1:1", "acme.tls.root_certs": "not a PEM",
	}, nil, 0)
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("a bad root certificate under the host's spelling was not read: %v", err)
	}
}
