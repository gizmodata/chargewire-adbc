// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Error codes (§11). Code is the stable part of an error. The lists below
// are the codes of version 1, and they are open: a later version adds codes,
// and a code this driver doesn't know is treated by its HTTP status (§12).
const (
	CodeBadRequest           = "bad_request"
	CodeUnknownOption        = "unknown_option"
	CodeInvalidOptionValue   = "invalid_option_value"
	CodeSQLError             = "sql_error"
	CodeUnsupportedGrantType = "unsupported_grant_type"
	CodeInvalidCredentials   = "invalid_credentials"
	CodeInvalidToken         = "invalid_token"
	CodeSessionEnded         = "session_ended"
	CodePermissionDenied     = "permission_denied"
	CodeReadOnly             = "read_only"
	CodeSessionNotFound      = "session_not_found"
	CodeStatementNotFound    = "statement_not_found"
	CodePreparedNotFound     = "prepared_not_found"
	CodeIngestNotFound       = "ingest_not_found"
	CodePartitionNotFound    = "partition_not_found"
	// CodeNotFound means no such endpoint, never a handle the server lost.
	CodeNotFound             = "not_found"
	CodeMethodNotAllowed     = "method_not_allowed"
	CodeResultNotReady       = "result_not_ready"
	CodeResultNotResumable   = "result_not_resumable"
	CodeIngestIncomplete     = "ingest_incomplete"
	CodeIngestPartExists     = "ingest_part_exists"
	CodeStatementCancelled   = "statement_cancelled"
	CodeStatementTimeout     = "statement_timeout"
	CodeResultExpired        = "result_expired"
	CodeIngestExpired        = "ingest_expired"
	CodePayloadTooLarge      = "payload_too_large"
	CodeUnsupportedArrowType = "unsupported_arrow_type"
	CodeSessionElsewhere     = "session_elsewhere"
	CodeIdempotencyKeyReused = "idempotency_key_reused"
	CodeExecutionError       = "execution_error"
	CodeTooManySessions      = "too_many_sessions"
	CodeResultQuotaExceeded  = "result_quota_exceeded"
	CodeRateLimited          = "rate_limited"
	CodeHeadersTooLarge      = "headers_too_large"
	CodeInternalError        = "internal_error"
	CodeNotImplemented       = "not_implemented"
	CodeServerDraining       = "server_draining"

	// Client certificate sign-in and certificate-bound tokens (§4.4).
	CodeCertificateRequired         = "certificate_required"
	CodeUnknownCertificateIdentity  = "unknown_certificate_identity"
	CodeCertificateRevoked          = "certificate_revoked"
	CodeCertificateMismatch         = "certificate_mismatch"
	CodeCertificateIdentityMismatch = "certificate_identity_mismatch"

	// Browser sign-in (§4.3). flow_not_found (404) and invalid_return_to
	// (400) are responses' codes; access_denied and identity_provider_error
	// (and invalid_credentials) arrive as a failed flow's error.code inside a
	// 200 poll answer, never as a status (§4.3.2).
	CodeFlowNotFound          = "flow_not_found"
	CodeInvalidReturnTo       = "invalid_return_to"
	CodeAccessDenied          = "access_denied"
	CodeIdentityProviderError = "identity_provider_error"
)

// Codes the driver gives failures it found itself, with no problem document
// from the server (§11). They begin with "client_"; servers never send one.
const (
	// CodeClientProtocolError: the server's answer broke the protocol (a
	// missing field or header the spec requires).
	CodeClientProtocolError = "client_protocol_error"
	// CodeClientHTTPError: an error status without a problem document (a
	// proxy's error page, say). The status is in Problem.Status.
	CodeClientHTTPError = "client_http_error"
	// CodeClientStatementFailed and CodeClientStatementCancelled: a failed or
	// cancelled statement document without its error.
	CodeClientStatementFailed    = "client_statement_failed"
	CodeClientStatementCancelled = "client_statement_cancelled"
	// CodeClientSignInFailed: a failed browser sign-in flow without its
	// error (§4.3.2).
	CodeClientSignInFailed = "client_sign_in_failed"
)

// Reasons an invalid_token (§4.2) or session_ended (§5.6) error carries. The
// list is open: a reason this driver doesn't know is a generic one (§12).
const (
	ReasonTokenExpired       = "token_expired"
	ReasonTokenRevoked       = "token_revoked"
	ReasonCredentialsChanged = "credentials_changed"
	ReasonPrincipalDropped   = "principal_dropped"
	ReasonKilled             = "killed" // session_ended only
)

// Why a statement was cancelled (§6.2), in a statement document's or a
// problem document's cancel_reason. Open, like every enumeration (§12).
const (
	CancelReasonClient           = "client"
	CancelReasonTimeout          = "timeout"
	CancelReasonSessionClosed    = "session_closed"
	CancelReasonKilled           = "killed"
	CancelReasonCancelled        = "cancelled"
	CancelReasonPrivilegeRevoked = "privilege_revoked"
	CancelReasonServerShutdown   = "server_shutdown"
)

// Problem is an RFC 9457 problem document (§11), as an error. It is also the
// error inside a failed or cancelled statement document (§6.2), where Status
// may be zero, and the error of a failure the driver found itself (a code
// beginning with "client_").
type Problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail"`
	SQLState  string `json:"sqlstate"`
	RequestID string `json:"request_id"`
	// Reason is set on invalid_token (§4.2) and session_ended (§5.6).
	Reason string `json:"reason"`
	// CancelReason is set on statement_cancelled and statement_timeout.
	CancelReason string `json:"cancel_reason"`

	// RetryAfter is the response's Retry-After, when it had one.
	RetryAfter time.Duration `json:"-"`
	// StatementID is the response's Chargewire-Statement-Id: the statement that
	// failed or was cancelled, when the error is a statement's (§6.1).
	StatementID string `json:"-"`
}

// ProtocolError is a server answer that breaks the protocol.
func ProtocolError(format string, args ...any) *Problem {
	return &Problem{Code: CodeClientProtocolError, Detail: fmt.Sprintf(format, args...)}
}

func (p *Problem) Error() string {
	var b strings.Builder
	if p.Detail != "" {
		b.WriteString(p.Detail)
	} else if p.Title != "" {
		b.WriteString(p.Title)
	} else {
		b.WriteString("request failed")
	}
	b.WriteString(" (")
	b.WriteString(p.Code)
	if p.Reason != "" {
		b.WriteString(", reason: ")
		b.WriteString(p.Reason)
	}
	if p.CancelReason != "" {
		b.WriteString(", cancel reason: ")
		b.WriteString(p.CancelReason)
	}
	if p.Status != 0 {
		fmt.Fprintf(&b, ", HTTP %d", p.Status)
	}
	if p.RequestID != "" {
		b.WriteString(", request ")
		b.WriteString(p.RequestID)
	}
	b.WriteString(")")
	return b.String()
}

// HasCode reports whether err is (or wraps) a Problem with the given code.
func HasCode(err error, code string) bool {
	if p, ok := errors.AsType[*Problem](err); ok {
		return p.Code == code
	}
	return false
}

// HasStatus reports whether err is (or wraps) a Problem with one of the
// given HTTP statuses.
func HasStatus(err error, statuses ...int) bool {
	if p, ok := errors.AsType[*Problem](err); ok {
		for _, status := range statuses {
			if p.Status == status {
				return true
			}
		}
	}
	return false
}

// maxProblemBytes bounds how much of an error body is read.
const maxProblemBytes = 1 << 20

// readProblem turns an error response into a Problem. A body that is not a
// problem document (a proxy's HTML error page, say) yields a Problem with
// code client_http_error and the response's status, which decides how it is
// treated (§11, §12).
func readProblem(resp *http.Response) *Problem {
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProblemBytes))
	p := &Problem{}
	if err := json.Unmarshal(body, p); err != nil || p.Code == "" {
		p = &Problem{
			Status: resp.StatusCode,
			Code:   CodeClientHTTPError,
			Detail: strings.TrimSpace(string(body)),
		}
		if p.Detail == "" || len(p.Detail) > 512 {
			p.Detail = http.StatusText(resp.StatusCode)
		}
	}
	if p.Status == 0 {
		p.Status = resp.StatusCode
	}
	if p.RequestID == "" {
		p.RequestID = resp.Header.Get("Chargewire-Request-Id")
	}
	p.StatementID = resp.Header.Get("Chargewire-Statement-Id")
	p.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	return p
}

// parseRetryAfter reads a Retry-After header: integer delta-seconds, the only
// form Chargewire servers send (§2.6). Anything else is ignored.
func parseRetryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// TLS alerts a server refuses a client certificate with (§4.4), RFC 8446 §6.
const (
	alertCertificateRevoked  = 44
	alertCertificateRequired = 116
)

// tlsAlertProblem turns a TLS alert that stands for a refusal of the client's
// certificate into that refusal's code (§4.4), so it is not reported as a
// server that could not be reached. Other errors give nil.
func tlsAlertProblem(err error) *Problem {
	alert, ok := tlsAlert(err)
	if !ok {
		return nil
	}
	switch alert {
	case alertCertificateRequired:
		return &Problem{Code: CodeCertificateRequired, Status: http.StatusUnauthorized,
			Detail: "the server requires a client certificate (TLS alert certificate_required)"}
	case alertCertificateRevoked:
		return &Problem{Code: CodeCertificateRevoked, Status: http.StatusUnauthorized,
			Detail: "the server refused the client certificate as revoked (TLS alert certificate_revoked)"}
	}
	return nil
}

// tlsAlert finds the TLS alert a peer sent. crypto/tls reports one received
// over TCP as a net.OpError ("remote error") around its unexported alert
// type, a uint8; QUIC and handshake errors wrap tls.AlertError.
func tlsAlert(err error) (uint8, bool) {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "remote error" && op.Err != nil {
		value := reflect.ValueOf(op.Err)
		if value.Kind() == reflect.Uint8 && value.Type().PkgPath() == "crypto/tls" {
			return uint8(value.Uint()), true
		}
	}
	var alert tls.AlertError
	if errors.As(err, &alert) {
		return uint8(alert), true
	}
	return 0, false
}
