// Licensed under the Apache License, Version 2.0.

// Package protocol is a client for the Warp SQL wire protocol: JSON control
// messages and Arrow IPC bodies over plain HTTP. It knows nothing about ADBC;
// the warpsql package builds the ADBC driver on top of it.
//
// The spec this follows is warp-sql/spec/warp-sql.md (draft 0.1.0). Section
// numbers in comments (§6.7, ...) refer to it.
package protocol

import (
	"encoding/base64"
	"slices"
)

// Media types (§2.2).
const (
	MediaTypeJSON        = "application/json"
	MediaTypeProblem     = "application/problem+json"
	MediaTypeArrowStream = "application/vnd.apache.arrow.stream"
)

// Optional capabilities a server lists in its discovery document (§3.1, §10).
// A client must not use one the server does not list.
const (
	CapabilityPreparedStatements = "prepared_statements"
	CapabilityTransactions       = "transactions"
	CapabilityIngest             = "ingest"
	CapabilityResultRetention    = "result_retention"
	CapabilityLongPoll           = "long_poll"
	CapabilityIPCCompressionZstd = "ipc_compression_zstd"
	CapabilityPartitionedResults = "partitioned_results"
	CapabilityResultPartitions   = "result_partitions"
	CapabilityPartitionedIngest  = "partitioned_ingest"
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
)

// Discovery is the document GET /warp returns (§3.1).
type Discovery struct {
	Protocol         string   `json:"protocol"`
	ProtocolVersions []string `json:"protocol_versions"`
	Draft            string   `json:"draft"`
	Server           struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"server"`
	Capabilities []string `json:"capabilities"`
	Auth         struct {
		GrantTypes []string `json:"grant_types"`
	} `json:"auth"`
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
	SessionID    string         `json:"session_id"`
	Options      map[string]any `json:"options"`
	IdleTimeoutS int64          `json:"idle_timeout_s"`
}

// optionsResult is the answer to PATCH /warp/v1/sessions/{id} (§5.3): the
// options now in effect and, per option, why one was refused.
type optionsResult struct {
	Options map[string]any     `json:"options"`
	Errors  map[string]Problem `json:"errors"`
}

// StatementState is a statement's lifecycle state (§6.2).
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
	// several partitions on this server (§6.10), empty otherwise.
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
// base URL of the server that holds it; Batches and Rows are known only once
// the partition is complete.
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

// ingestResult is the answer to POST …/ingest (§8.1).
type ingestResult struct {
	RowsIngested int64 `json:"rows_ingested"`
}
