// Licensed under the Apache License, Version 2.0.

// Package protocol is a client for the Warp SQL wire protocol: JSON control
// messages and Arrow IPC bodies over plain HTTP. It knows nothing about ADBC;
// the warpsql package builds the ADBC driver on top of it.
//
// The spec this follows is warp-sql/spec/warp-sql.md, version 1 (1.0.0).
// Section numbers in comments (§6.7, ...) refer to it.
package protocol

import (
	"encoding/base64"
	"slices"
	"time"
)

// Media types (§2.2).
const (
	MediaTypeJSON        = "application/json"
	MediaTypeProblem     = "application/problem+json"
	MediaTypeArrowStream = "application/vnd.apache.arrow.stream"
)

// Optional capabilities a server lists in its discovery document (§3.1).
// A client must not use one the server does not list, and ignores names it
// doesn't know. (Result retention is core: any server may be asked, and may
// decline, §6.5.)
const (
	CapabilityPreparedStatements = "prepared_statements"
	CapabilityTransactions       = "transactions"
	CapabilityIngest             = "ingest"
	CapabilityPartitionedIngest  = "partitioned_ingest"
	CapabilityLongPoll           = "long_poll"
	CapabilityIPCCompressionZstd = "ipc_compression_zstd"
	// CapabilityResultPartitions: results split over several partitions on
	// this server, read at once (§6.10).
	CapabilityResultPartitions = "result_partitions"
	// CapabilityResultLocations: partitions held by other servers, given
	// only to a client that sends Warp-Result-Locations: remote (§6.2).
	CapabilityResultLocations = "result_locations"
	// CapabilityIPCCompressionLZ4Frame: Arrow buffers compressed with LZ4_FRAME (§8.2).
	CapabilityIPCCompressionLZ4Frame = "ipc_compression_lz4_frame"
	// CapabilityMTLSIdentity: the client_certificate grant (§4.4).
	CapabilityMTLSIdentity = "mtls_identity"
	// CapabilityOAuthBrowser: browser sign-in (§4.3).
	CapabilityOAuthBrowser = "oauth_browser"
)

// How a result split into partitions spreads its batches over them (§6.10).
const (
	// PartitionOrderInterleaved: batch j of the result is batch j div k of
	// partition j mod k, so reading the partitions round robin rebuilds the
	// result's order.
	PartitionOrderInterleaved = "interleaved"
	// PartitionOrderAny: the rows' order means nothing.
	PartitionOrderAny = "any"
)

// Grant types for POST /warp/v1/auth/token (§4.1).
const (
	GrantPassword      = "password"
	GrantTokenExchange = "token_exchange"
	// GrantClientCertificate signs in with the TLS client certificate the
	// connection presents, with no password (§4.4). The token is bound to the
	// certificate.
	GrantClientCertificate = "client_certificate"
)

// Discovery is the document GET /warp returns (§3.1).
type Discovery struct {
	Protocol         string   `json:"protocol"`
	ProtocolVersions []string `json:"protocol_versions"`
	Server           struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"server"`
	Capabilities []string `json:"capabilities"`
	Auth         struct {
		GrantTypes []string `json:"grant_types"`
		// OAuthBrowser describes browser sign-in (§4.3) when the server
		// offers it.
		OAuthBrowser *struct {
			Provider string `json:"provider"`
		} `json:"oauth_browser"`
	} `json:"auth"`
	// RTT is how long the discovery request took to answer once sent: about
	// one network round trip (the server answers it at once). Zero when it
	// could not be measured.
	RTT time.Duration `json:"-"`
}

// BrowserSignInProvider is the identity provider's name the server gives for
// browser sign-in (§4.3), or "".
func (d *Discovery) BrowserSignInProvider() string {
	if d.Auth.OAuthBrowser == nil {
		return ""
	}
	return d.Auth.OAuthBrowser.Provider
}

// Has reports whether the server lists capability.
func (d *Discovery) Has(capability string) bool {
	return slices.Contains(d.Capabilities, capability)
}

// SupportsGrant reports whether the server accepts grant type grant.
func (d *Discovery) SupportsGrant(grant string) bool {
	return slices.Contains(d.Auth.GrantTypes, grant)
}

// tokenResponse is the answer to POST /warp/v1/auth/token (§4.1).
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

// SessionDocument describes a session (§5.1).
type SessionDocument struct {
	SessionID string         `json:"session_id"`
	Options   map[string]any `json:"options"`
	// Transaction is the session's transaction: TransactionNone, Open or
	// Aborted, or a value a later version adds (§12).
	Transaction  string `json:"transaction"`
	IdleTimeoutS int64  `json:"idle_timeout_s"`
}

// A session's transaction (§5.1).
const (
	TransactionNone    = "none"
	TransactionOpen    = "open"
	TransactionAborted = "aborted"
	// TransactionUnknown is what the driver reports for a value it doesn't
	// know, or none at all (§12).
	TransactionUnknown = "unknown"
)

// TransactionState is the session's transaction as the driver reports it:
// one of the version 1 values, else TransactionUnknown.
func (d *SessionDocument) TransactionState() string {
	switch d.Transaction {
	case TransactionNone, TransactionOpen, TransactionAborted:
		return d.Transaction
	}
	return TransactionUnknown
}

// optionsResult is the answer to PATCH /warp/v1/sessions/{id} (§5.3): the
// options now in effect and, per option, why one was refused.
type optionsResult struct {
	Options map[string]any     `json:"options"`
	Errors  map[string]Problem `json:"errors"`
}

// StatementState is a statement's lifecycle state (§6.2). The set is open:
// a state this driver doesn't know is not terminal (§12).
type StatementState string

const (
	StateQueued    StatementState = "queued"
	StateRunning   StatementState = "running"
	StateSucceeded StatementState = "succeeded"
	StateFailed    StatementState = "failed"
	StateCancelled StatementState = "cancelled"
)

// Terminal reports whether the statement has finished one way or another.
func (s StatementState) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCancelled
}

// StatementDocument is the status of a statement (§6.2).
type StatementDocument struct {
	StatementID  string         `json:"statement_id"`
	State        StatementState `json:"state"`
	Error        *Problem       `json:"error"`
	CancelReason string         `json:"cancel_reason"`
	Result       *Result        `json:"result"`
}

// Failed reports a statement that ended without succeeding: a client must
// treat it as the statement's error whatever the response's status (§6.2).
func (d *StatementDocument) Failed() bool {
	return d.State == StateFailed || d.State == StateCancelled
}

// Result kinds (§6.2).
const (
	ResultRows   = "rows"
	ResultUpdate = "update"
)

// Result is a succeeded statement's result (§6.2).
type Result struct {
	Kind       string      `json:"kind"`
	Schema     string      `json:"schema"`
	Partitions []Partition `json:"partitions"`
	// PartitionOrder is "interleaved" or "any" for a result split into
	// several partitions (§6.10), empty for one partition.
	PartitionOrder string `json:"partition_order"`
	Retained       bool   `json:"retained"`
	ExpiresAt      string `json:"expires_at"`
	RowsAffected   *int64 `json:"rows_affected"`
}

// SchemaBytes decodes the base64 Arrow schema (§2.2).
func (r *Result) SchemaBytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(r.Schema)
}

// Partition is one piece of a row result (§6.2). Location, when set, is the
// base URL of the server that holds it (only for a client that sent
// Warp-Result-Locations: remote); Batches and Rows are known only once the
// partition is complete.
type Partition struct {
	Index    int     `json:"index"`
	Location *string `json:"location"`
	Batches  *int64  `json:"batches"`
	Rows     *int64  `json:"rows"`
}

// PreparedDocument describes a prepared statement (§7.1). ResultKind is
// ResultRows or ResultUpdate, or nil when the server can tell only by running
// it; ResultSchema is set for rows the server could describe.
type PreparedDocument struct {
	PreparedID      string  `json:"prepared_id"`
	ParameterSchema string  `json:"parameter_schema"`
	ResultSchema    *string `json:"result_schema"`
	ResultKind      *string `json:"result_kind"`
}

// A partitioned ingest's state (§8.3). Open: a state this driver doesn't
// know is not terminal (§12).
const (
	IngestOpen       = "open"
	IngestCommitting = "committing"
	IngestCommitted  = "committed"
	IngestFailed     = "failed"
	IngestAborted    = "aborted"
)

// ingestResult is the answer to POST …/ingest (§8.1).
type ingestResult struct {
	RowsIngested int64 `json:"rows_ingested"`
}
