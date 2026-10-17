// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Error codes (§11). Code is the stable part of an error; clients treat a
// code they don't know by its HTTP status (§12).
const (
	CodeBadRequest           = "bad_request"
	CodeUnknownOption        = "unknown_option"
	CodeInvalidOptionValue   = "invalid_option_value"
	CodeSQLError             = "sql_error"
	CodeInvalidCredentials   = "invalid_credentials"
	CodeInvalidToken         = "invalid_token"
	CodeSessionEnded         = "session_ended"
	CodePermissionDenied     = "permission_denied"
	CodeReadOnly             = "read_only"
	CodeSessionNotFound      = "session_not_found"
	CodeStatementNotFound    = "statement_not_found"
	CodePreparedNotFound     = "prepared_not_found"
	CodeNotFound             = "not_found"
	CodeMethodNotAllowed     = "method_not_allowed"
	CodeResultNotReady       = "result_not_ready"
	CodeResultNotResumable   = "result_not_resumable"
	CodeResultExpired        = "result_expired"
	CodePayloadTooLarge      = "payload_too_large"
	CodeUnsupportedArrowType = "unsupported_arrow_type"
	CodeSessionElsewhere     = "session_elsewhere"
	CodeIdempotencyKeyReused = "idempotency_key_reused"
	CodeHeadersTooLarge      = "headers_too_large"
	CodeTooManySessions      = "too_many_sessions"
	CodeResultQuotaExceeded  = "result_quota_exceeded"
	CodeRateLimited          = "rate_limited"
	CodeStatementTimeout     = "statement_timeout"
	CodeStatementCancelled   = "statement_cancelled"
	CodeExecutionError       = "execution_error"
	CodeServerDraining       = "server_draining"
	CodeNotImplemented       = "not_implemented"
	CodeIngestNotFound       = "ingest_not_found"
	CodeIngestIncomplete     = "ingest_incomplete"
	CodeIngestPartExists     = "ingest_part_exists"
	CodeIngestExpired        = "ingest_expired"

	// Client certificate sign-in and certificate-bound tokens (§4.4).
	CodeClientCertificateRequired   = "client_certificate_required"
	CodeUnknownCertificateIdentity  = "unknown_certificate_identity"
	CodeCertificateRevoked          = "certificate_revoked"
	CodeCertificateMismatch         = "certificate_mismatch"
	CodeCertificateIdentityMismatch = "certificate_identity_mismatch"
)

// Reasons a session_ended error carries (§5.6).
const (
	ReasonKilled             = "killed"
	ReasonCredentialsChanged = "credentials_changed"
	ReasonPrincipalDropped   = "principal_dropped"
	ReasonTokenExpired       = "token_expired"
)

// Problem is an RFC 9457 problem document (§11), as an error. It is also the
// error inside a failed or cancelled statement document (§6.2), where Status
// may be zero.
type Problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail"`
	SQLState  string `json:"sqlstate"`
	RequestID string `json:"request_id"`
	// Reason is set on session_ended (§5.6).
	Reason string `json:"reason"`

	// RetryAfter is the response's Retry-After, when it had one.
	RetryAfter time.Duration `json:"-"`
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

// maxProblemBytes bounds how much of an error body is read.
const maxProblemBytes = 1 << 20

// readProblem turns an error response into a Problem. A body that is not a
// problem document (a proxy's HTML error page, say) still yields a Problem
// whose code is derived from the status, so callers can always switch on it.
func readProblem(resp *http.Response) *Problem {
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProblemBytes))
	p := &Problem{}
	if err := json.Unmarshal(body, p); err != nil || p.Code == "" {
		p = &Problem{
			Status: resp.StatusCode,
			Code:   codeForStatus(resp.StatusCode),
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
		p.RequestID = resp.Header.Get("Warp-Request-Id")
	}
	p.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	return p
}

// codeForStatus is the code a response without a problem document stands for.
func codeForStatus(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return CodeInvalidToken
	case http.StatusForbidden:
		return CodePermissionDenied
	case http.StatusNotFound:
		return CodeNotFound
	case http.StatusMethodNotAllowed:
		return CodeMethodNotAllowed
	case http.StatusRequestEntityTooLarge:
		return CodePayloadTooLarge
	case http.StatusTooManyRequests:
		return CodeRateLimited
	case http.StatusRequestHeaderFieldsTooLarge:
		return CodeHeadersTooLarge
	case http.StatusNotImplemented:
		return CodeNotImplemented
	case http.StatusServiceUnavailable:
		return CodeServerDraining
	}
	if status >= 500 {
		return CodeExecutionError
	}
	return CodeBadRequest
}

// parseRetryAfter reads a Retry-After header given in seconds. (The HTTP-date
// form is not used by Warp SQL servers and is ignored.)
func parseRetryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}
