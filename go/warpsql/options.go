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

	// OptionAccessToken is a Warp SQL access token obtained elsewhere, used
	// instead of a username and password. It cannot be renewed.
	OptionAccessToken = "adbc.warp.auth.access_token"
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
)

// Connection options, read-only.
const (
	// OptionSessionID is the connection's current server session id.
	OptionSessionID = "adbc.warp.session.id"
	// OptionSessionReplacements is how often the connection's session was
	// lost and replaced.
	OptionSessionReplacements = "adbc.warp.session.replacements"
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
	// OptionQueryTag is a JSON object recorded with every statement, for
	// observability on the server.
	OptionQueryTag = "adbc.warp.statement.query_tag"
	// OptionIPCCompression is "zstd" or "none" (default): Arrow buffer
	// compression for results and uploads, used when the server offers it.
	OptionIPCCompression = "adbc.warp.ipc_compression"
	// OptionResultPartitions is the most partitions a query's result may be
	// split into (default 1: never). With more, a server that offers it
	// splits a large result, and the driver reads the partitions at once,
	// each over its own connection (§6.10): one TCP connection carries what
	// one network flow allows, and decoding spreads over several cores.
	OptionResultPartitions = "adbc.warp.statement.result_partitions"
	// OptionResultOrder is "preserve" (default) or "any". With "preserve" a
	// split result comes back in the server's order (round robin over the
	// partitions); with "any" batches come in whichever order they arrive.
	OptionResultOrder = "adbc.warp.statement.result_order"
)

// Statement options, read-only.
const (
	// OptionStatementID is the id of the statement's last execution.
	OptionStatementID = "adbc.warp.statement.id"
)

// maxResultPartitions is the most partitions a client may ask for (§6.10).
const maxResultPartitions = 256

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
	compression string
	partitions  int  // 0 or 1: one partition
	anyOrder    bool // the rows' order doesn't matter
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
			if err := protocol.ValidQueryTag(value); err != nil {
				return true, invalidOption(key, value, "a JSON object")
			}
		}
		o.queryTag = value
	case OptionResultPartitions:
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maxResultPartitions {
			return true, invalidOption(key, value, fmt.Sprintf("a whole number from 1 to %d", maxResultPartitions))
		}
		o.partitions = n
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
		switch strings.ToLower(value) {
		case "", "none":
			o.compression = ""
		case "zstd":
			o.compression = "zstd"
		default:
			return true, invalidOption(key, value, `"zstd" or "none"`)
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
	case OptionResultOrder:
		if o.anyOrder {
			return "any", true
		}
		return "preserve", true
	case OptionIPCCompression:
		if o.compression == "" {
			return "none", true
		}
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
