// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// Database options. The standard "uri", "username" and "password" options
// and the adbc.telemetry.* options apply too.
const (
	// OptionTLSSkipVerify ("true"/"false") turns off verification of the
	// server's certificate. For development only; it is off unless set.
	OptionTLSSkipVerify = "adbc.warp.tls.skip_verify"
	// OptionTLSRootCerts is PEM-encoded CA certificates to trust instead of
	// the system's.
	OptionTLSRootCerts = "adbc.warp.tls.root_certs"
	// OptionTLSClientCert and OptionTLSClientKey are a PEM certificate chain
	// and private key presented to servers that require mutual TLS.
	OptionTLSClientCert = "adbc.warp.tls.client_cert"
	OptionTLSClientKey  = "adbc.warp.tls.client_key"
	// OptionTLSServerName overrides the host name the certificate is checked
	// against.
	OptionTLSServerName = "adbc.warp.tls.server_name"

	// OptionAuthType is how the driver signs in: AuthTypePassword (the
	// default: the username and password options, or a subject or access
	// token when one is given) or AuthTypeClientCertificate (the TLS client
	// certificate of OptionTLSClientCert/OptionTLSClientKey is the
	// credential, with no password; spec §4.4). "mtls" is accepted for
	// AuthTypeClientCertificate. The server binds a certificate sign-in's
	// token to the certificate, and every connection the driver opens
	// presents it. AuthTypeOAuthBrowser signs in through a browser with the
	// server's identity provider (spec §4.3) when the first connection opens:
	// see BrowserSignIn.
	OptionAuthType = "adbc.warp.auth.type"

	// OptionOAuthBrowser is the command that opens the browser sign-in page,
	// given its URL as the last argument. Default: the system's (open,
	// xdg-open, rundll32). BrowserNone only prints the URL to stderr.
	OptionOAuthBrowser = "adbc.warp.oauth.browser"
	// OptionOAuthTimeoutSeconds is how long the person has to sign in
	// (default 300).
	OptionOAuthTimeoutSeconds = "adbc.warp.oauth.timeout_seconds"

	// OptionAccessToken is a Warp SQL access token obtained elsewhere, used
	// instead of a username and password. The driver cannot renew it, but it
	// may be set again on an open database that signs in with a token (or
	// through the browser): its connections send the new one from then on.
	//
	// GetOption reads the access token in use, on the database and on its
	// connections (they share it): the one the driver got by signing in,
	// renewed first when it is about to expire, or the caller's own; "" before
	// the first connection signs in with a password. It is a bearer secret:
	// anyone holding it acts as the user until it expires. Store it encrypted
	// and never log it.
	OptionAccessToken = "adbc.warp.auth.access_token"
	// OptionAccessTokenExpiresAt (read-only) is when that token expires, in
	// RFC 3339 UTC: from the token response's expires_in, or the token's own
	// exp claim when it is a JWT. "" when unknown.
	OptionAccessTokenExpiresAt = "adbc.warp.auth.access_token_expires_at"
	// OptionSubjectToken is an identity provider's token (for example an
	// OpenID Connect ID token) exchanged for an access token with the
	// token_exchange grant.
	OptionSubjectToken = "adbc.warp.auth.subject_token"

	// OptionRetryMaxAttempts bounds how often a request that is safe to
	// repeat is sent, first try included (default 4; 1 turns retries off).
	OptionRetryMaxAttempts = "adbc.warp.retry.max_attempts"

	// OptionSessionReconnect ("true"/"false", default "true") says whether a
	// session the server lost (evicted when idle, server restarted) is
	// replaced by a new one with the connection's options. Work that needs
	// the old session (an open transaction, a pending result) fails either
	// way; this decides whether the connection is usable afterwards.
	OptionSessionReconnect = "adbc.warp.session.reconnect"

	// OptionSessionID, set on the database before its first connection
	// opens, makes every connection attach to that existing session instead
	// of creating one: the driver reads the session (GET
	// /warp/v1/sessions/{id}) to check that the server has it for this
	// principal, and the connection then works in it, inside its open
	// transaction if it has one. The credentials must sign in as the
	// session's principal (typically OptionAccessToken with the token its
	// creator used). An id the server doesn't have, or another principal's,
	// is session_not_found (StatusNotFound). An attached session is never
	// replaced when the server loses it. On a connection the option is the
	// current session id, read-only.
	//
	// A session lives on the server instance that created it: behind a load
	// balancer with several instances, attaching works only when the request
	// reaches that instance (affinity by client address does not, when the
	// attaching client's address differs).
	//
	// OptionSessionCloseOnRelease ("true"/"false") says whether closing the
	// connection ends the session on the server. Default: "true" for a
	// session the connection created, "false" for one it attached to, which
	// then stays (with its transaction) for the next client until it is
	// closed or idles out. Set on the database, it applies to its new
	// connections; on a connection, it can be changed at any time.
	OptionSessionCloseOnRelease = "adbc.warp.session.close_on_release"
)

// Values of OptionAuthType.
const (
	AuthTypePassword          = "password"
	AuthTypeClientCertificate = "client_certificate"
	// AuthTypeOAuthBrowser: browser sign-in (spec §4.3).
	AuthTypeOAuthBrowser = "oauth_browser"
	// BrowserNone, as OptionOAuthBrowser, prints the sign-in URL instead of
	// opening a browser.
	BrowserNone = "none"
	// authTypeMTLS is accepted as AuthTypeClientCertificate.
	authTypeMTLS = "mtls"
)

// Connection options, read-only. OptionSessionID is a database option too
// (attach to a session), and OptionSessionCloseOnRelease a connection option.
const (
	// OptionSessionID is the connection's current server session id.
	OptionSessionID = "adbc.warp.session.id"
	// OptionSessionReplacements is how often the connection's session was
	// lost and replaced.
	OptionSessionReplacements = "adbc.warp.session.replacements"
	// OptionSessionTransaction is the session's transaction as the server
	// reports it (§5.2): "none", "open", "aborted", or "unknown" for a value
	// this driver doesn't know. It sees SQL BEGIN and COMMIT too. Reading it
	// costs a request.
	OptionSessionTransaction = "adbc.warp.session.transaction"
)

// Statement options. They can also be set on the database or the connection,
// as defaults for the statements created from it.
const (
	// OptionAsync ("true"/"false", default "false") runs queries through the
	// asynchronous path: submit, poll, then download a retained result that
	// resumes where it stopped if the download breaks. The default streams
	// the result in the response to one request.
	OptionAsync = "adbc.warp.statement.async"
	// OptionRetainResult ("true"/"false") asks the server to keep results
	// for re-reading, so an interrupted download resumes (§6.5). Unset, the
	// server decides (it keeps async results and drops streamed ones).
	OptionRetainResult = "adbc.warp.statement.retain_result"
	// OptionStatementTimeout is the seconds (fractions allowed) after which
	// the server cancels a statement; 0 means none.
	OptionStatementTimeout = "adbc.warp.statement.timeout_seconds"
	// OptionQueryTag is a JSON object recorded with every statement and
	// ingest, for observability on the server (Warp-Query-Tag, §2.5). It is
	// sent compact and in ASCII (other characters as \u escapes), and may be
	// at most 4,096 bytes so; reading the option back gives that form.
	OptionQueryTag = "adbc.warp.statement.query_tag"
	// OptionIPCCompression is the Arrow buffer compression for results and
	// uploads (§8.2): "auto" (default), "lz4_frame", "zstd" or "none". A codec
	// the server doesn't offer is not used. "auto" chooses per link, from the
	// round trip measured when connecting: on a fast, near link the bytes
	// compression saves cost more CPU than they are worth; on a far one
	// bandwidth is scarce and zstd's smaller results win.
	OptionIPCCompression = "adbc.warp.ipc_compression"
	// OptionResultPartitions is the most partitions a query's result may be
	// split into (default 32; 1: never). A server that offers it splits a
	// large result, and the driver reads the partitions at once, each over
	// its own connection (§6.10): one TCP connection carries what one network
	// flow allows, and decoding spreads over several cores. The server
	// decides how many by the result's size (Hardcharger: a few batches get a
	// partition per two batches, a large result all of them), and a server
	// without the capability is never asked.
	OptionResultPartitions = "adbc.warp.statement.result_partitions"
	// OptionResultOrder is "preserve" (default) or "any". With "preserve" a
	// split result comes back in the server's order (round robin over the
	// partitions); with "any" batches come in whichever order they arrive.
	OptionResultOrder = "adbc.warp.statement.result_order"
	// OptionIngestParts is the most connections one bulk ingest is sent over
	// at once: "auto" (default) or 1 to 256 (1: one stream). On a server that
	// offers it, an upload of more than one slice (uploadSliceRows) is a
	// partitioned ingest (§8.3): its batches are spread over the parts, which
	// are committed as one, all or nothing. A small upload uses fewer parts,
	// about one per two slices. "auto" is 16 parts on a near link and 32 on a
	// far one, where each connection carries less. The rows then land in no
	// defined order; set 1 to keep the upload's order.
	OptionIngestParts = "adbc.warp.ingest.parts"
)

// Defaults of the options above.
const (
	defaultResultPartitions = 32
	autoIngestParts         = 0 // OptionIngestParts "auto": nearIngestParts or farIngestParts
	nearIngestParts         = 16
	farIngestParts          = 32
)

// Statement options, read-only.
const (
	// OptionStatementID is the id of the statement's last execution.
	OptionStatementID = "adbc.warp.statement.id"
)

// maxResultPartitions is the most partitions a client may ask for (§6.10).
const maxResultPartitions = 256

// maxIngestParts is the most parts one ingest may have (§8.3).
const maxIngestParts = 256

// optionBindByName is the standard ADBC option the Python DB-API sets for
// parameters given as a dict. Arrow parameters carry their names, so it
// needs no handling beyond being accepted.
const optionBindByName = "adbc.statement.bind_by_name"

// execOptions are the statement options, inherited database → connection →
// statement.
type execOptions struct {
	async       bool
	retain      *bool
	timeout     time.Duration
	queryTag    string
	compression string // codecAuto, codecLZ4Frame, codecZstd or codecNone
	partitions  int    // 0 or 1: one partition
	anyOrder    bool   // the rows' order doesn't matter
	ingestParts int    // autoIngestParts, or 1 (one stream) to maxIngestParts
}

// defaultExecOptions are the statement options' defaults.
func defaultExecOptions() execOptions {
	return execOptions{compression: codecAuto, partitions: defaultResultPartitions, ingestParts: autoIngestParts}
}

// set applies one statement option; it reports false for keys it does not
// know.
func (o *execOptions) set(key, value string) (bool, error) {
	switch key {
	case OptionAsync:
		b, err := parseBool(key, value)
		if err != nil {
			return true, err
		}
		o.async = b
	case OptionRetainResult:
		if value == "" {
			o.retain = nil
			return true, nil
		}
		b, err := parseBool(key, value)
		if err != nil {
			return true, err
		}
		o.retain = &b
	case OptionStatementTimeout:
		seconds, err := strconv.ParseFloat(value, 64)
		if err != nil || seconds < 0 {
			return true, invalidOption(key, value, "a number of seconds, 0 or more")
		}
		o.timeout = time.Duration(seconds * float64(time.Second))
	case OptionQueryTag:
		if value != "" {
			tag, err := protocol.NormalizeQueryTag(value)
			if err != nil {
				return true, invalidOption(key, value, fmt.Sprintf("a JSON object of at most %d bytes as sent (%v)",
					protocol.MaxQueryTagBytes, err))
			}
			value = tag
		}
		o.queryTag = value
	case OptionResultPartitions:
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maxResultPartitions {
			return true, invalidOption(key, value, fmt.Sprintf("a whole number from 1 to %d", maxResultPartitions))
		}
		o.partitions = n
	case OptionIngestParts:
		if v := strings.ToLower(value); v == "" || v == codecAuto {
			o.ingestParts = autoIngestParts
			return true, nil
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maxIngestParts {
			return true, invalidOption(key, value, fmt.Sprintf(`"auto" or a whole number from 1 to %d`, maxIngestParts))
		}
		o.ingestParts = n
	case OptionResultOrder:
		switch strings.ToLower(value) {
		case "", "preserve":
			o.anyOrder = false
		case "any":
			o.anyOrder = true
		default:
			return true, invalidOption(key, value, `"preserve" or "any"`)
		}
	case OptionIPCCompression:
		switch v := strings.ToLower(value); v {
		case "", codecAuto:
			o.compression = codecAuto
		case codecNone, codecLZ4Frame, codecZstd:
			o.compression = v
		default:
			return true, invalidOption(key, value, `"auto", "lz4_frame", "zstd" or "none"`)
		}
	default:
		return false, nil
	}
	return true, nil
}

// get reads one statement option; it reports false for keys it does not know.
func (o *execOptions) get(key string) (string, bool) {
	switch key {
	case OptionAsync:
		return strconv.FormatBool(o.async), true
	case OptionRetainResult:
		if o.retain == nil {
			return "", true
		}
		return strconv.FormatBool(*o.retain), true
	case OptionStatementTimeout:
		return strconv.FormatFloat(o.timeout.Seconds(), 'f', -1, 64), true
	case OptionQueryTag:
		return o.queryTag, true
	case OptionResultPartitions:
		return strconv.Itoa(max(o.partitions, 1)), true
	case OptionIngestParts:
		if o.ingestParts == autoIngestParts {
			return codecAuto, true
		}
		return strconv.Itoa(o.ingestParts), true
	case OptionResultOrder:
		if o.anyOrder {
			return "any", true
		}
		return "preserve", true
	case OptionIPCCompression:
		return o.compression, true
	}
	return "", false
}

func parseBool(key, value string) (bool, error) {
	switch strings.ToLower(value) {
	case adbc.OptionValueEnabled, "1":
		return true, nil
	case adbc.OptionValueDisabled, "0":
		return false, nil
	}
	return false, invalidOption(key, value, `"true" or "false"`)
}

func invalidOption(key, value, want string) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("[%s] invalid value %q for option %s: want %s", driverName, value, key, want),
	}
}
