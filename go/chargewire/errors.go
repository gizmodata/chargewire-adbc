// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/chargewire-adbc/go/internal/protocol"
)

// Error detail keys attached to errors that came from the server. The C API
// exposes them through AdbcErrorGetDetail, Python as Error.details.
const (
	// ErrorDetailCode is the Chargewire error code (§11), e.g. "sql_error".
	ErrorDetailCode = "chargewire.code"
	// ErrorDetailRequestID is the server's request id, for its logs.
	ErrorDetailRequestID = "chargewire.request_id"
	// ErrorDetailReason is why a token or session stopped working
	// (invalid_token §4.2, session_ended §5.6), e.g. "token_revoked".
	ErrorDetailReason = "chargewire.reason"
	// ErrorDetailCancelReason is why a statement was cancelled (§6.2).
	ErrorDetailCancelReason = "chargewire.cancel_reason"
)

// ErrorCode returns the Chargewire error code (§11) behind an error the driver
// returned, such as "sql_error" or "session_not_found": the server's, or one
// beginning with "client_" for a failure the driver found in the server's
// answer (client_protocol_error, client_http_error for an error status
// without a problem document). It is "" for errors with no answer behind them
// (a connection that failed, an invalid option).
func ErrorCode(err error) string {
	if p, ok := errors.AsType[*protocol.Problem](err); ok {
		return p.Code
	}
	return ""
}

// wrapError turns an error from the protocol layer into an adbc.Error with
// the status the Chargewire code stands for. op says what was being done.
// errors.As finds both the adbc.Error and the original error.
func wrapError(err error, op string) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[adbc.Error](err); ok {
		return err
	}
	adbcErr := adbc.Error{Code: statusOf(err), Msg: fmt.Sprintf("[%s] %s: %v", driverName, op, err)}
	if p, ok := errors.AsType[*protocol.Problem](err); ok {
		copy(adbcErr.SqlState[:], p.SQLState)
		adbcErr.Details = append(adbcErr.Details, &adbc.TextErrorDetail{Name: ErrorDetailCode, Detail: p.Code})
		if p.RequestID != "" {
			adbcErr.Details = append(adbcErr.Details, &adbc.TextErrorDetail{Name: ErrorDetailRequestID, Detail: p.RequestID})
		}
		if p.Reason != "" {
			adbcErr.Details = append(adbcErr.Details, &adbc.TextErrorDetail{Name: ErrorDetailReason, Detail: p.Reason})
		}
		if p.CancelReason != "" {
			adbcErr.Details = append(adbcErr.Details, &adbc.TextErrorDetail{Name: ErrorDetailCancelReason, Detail: p.CancelReason})
		}
	}
	return &driverError{adbcErr: adbcErr, cause: err}
}

// driverError is an adbc.Error that keeps the error it was made from.
type driverError struct {
	adbcErr adbc.Error
	cause   error
}

func (e *driverError) Error() string   { return e.adbcErr.Error() }
func (e *driverError) Unwrap() []error { return []error{e.adbcErr, e.cause} }

// statusOf maps an error to an ADBC status.
func statusOf(err error) adbc.Status {
	var transport *protocol.TransportError
	switch {
	case errors.Is(err, context.Canceled):
		return adbc.StatusCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return adbc.StatusTimeout
	case errors.As(err, new(*protocol.SessionLostError)):
		return adbc.StatusIO
	case errors.As(err, &transport):
		return adbc.StatusIO
	}
	if p, ok := errors.AsType[*protocol.Problem](err); ok {
		return statusOfProblem(p)
	}
	return adbc.StatusInternal
}

// SQLSTATEs that say more than their error code: the two §11 names, which
// clients map to "not found" and "already exists", and class 23, a violated
// constraint, which is what StatusIntegrity stands for. They refine only the
// codes of a statement's SQL, so a server that attaches one to another code
// (a permission or a session error) keeps that code's status.
const (
	sqlstateUndefinedTable          = "42P01"
	sqlstateDuplicateTable          = "42P07"
	sqlstateClassIntegrityViolation = "23"
)

// codeStatuses maps every Chargewire error code of version 1 (§11), and the
// driver's own client_ codes, to the ADBC status that says what to do about
// it. A user's SQL that fails, at prepare (sql_error) or while it runs
// (execution_error: a division by zero, a conversion), is
// StatusInvalidArgument, as the DuckDB and PostgreSQL ADBC drivers report it
// and as the Flight SQL driver does for gRPC INVALID_ARGUMENT, so DB-API
// layers raise ProgrammingError rather than InternalError. StatusInternal is
// kept for a failure that is no request's doing: internal_error, an answer
// that broke the protocol, an Idempotency-Key the driver reused (it makes
// them). Limits and a server that can't serve now are StatusIO, which callers
// may retry. A code missing here falls back to its HTTP status (§12).
//
// TestErrorStatusesCoverTheSpec checks this table against the spec's.
var codeStatuses = map[string]adbc.Status{
	// 400: what the request asked for is malformed or not allowed.
	protocol.CodeBadRequest:         adbc.StatusInvalidArgument,
	protocol.CodeInvalidOptionValue: adbc.StatusInvalidArgument,
	protocol.CodeInvalidReturnTo:    adbc.StatusInvalidArgument,
	protocol.CodeSQLError:           adbc.StatusInvalidArgument,
	// An option the server doesn't have: ADBC's status for an unknown option.
	protocol.CodeUnknownOption: adbc.StatusNotImplemented,
	// A grant this server doesn't offer: the connection can't sign in.
	protocol.CodeUnsupportedGrantType: adbc.StatusUnauthenticated,

	// 401: no valid identity.
	protocol.CodeInvalidCredentials:          adbc.StatusUnauthenticated,
	protocol.CodeInvalidToken:                adbc.StatusUnauthenticated,
	protocol.CodeSessionEnded:                adbc.StatusUnauthenticated,
	protocol.CodeCertificateRequired:         adbc.StatusUnauthenticated,
	protocol.CodeUnknownCertificateIdentity:  adbc.StatusUnauthenticated,
	protocol.CodeCertificateRevoked:          adbc.StatusUnauthenticated,
	protocol.CodeCertificateMismatch:         adbc.StatusUnauthenticated,
	protocol.CodeCertificateIdentityMismatch: adbc.StatusUnauthenticated,
	// A failed browser sign-in's codes (§4.3.2), and flow_not_found (404): a
	// sign-in that can't complete.
	protocol.CodeAccessDenied:          adbc.StatusUnauthenticated,
	protocol.CodeIdentityProviderError: adbc.StatusUnauthenticated,
	protocol.CodeFlowNotFound:          adbc.StatusUnauthenticated,
	protocol.CodeClientSignInFailed:    adbc.StatusUnauthenticated,

	// 403: an identity without the privilege.
	protocol.CodePermissionDenied: adbc.StatusUnauthorized,
	protocol.CodeReadOnly:         adbc.StatusUnauthorized,

	// 404.
	protocol.CodeSessionNotFound:   adbc.StatusNotFound,
	protocol.CodeStatementNotFound: adbc.StatusNotFound,
	protocol.CodePreparedNotFound:  adbc.StatusNotFound,
	protocol.CodeIngestNotFound:    adbc.StatusNotFound,
	protocol.CodePartitionNotFound: adbc.StatusNotFound,
	protocol.CodeNotFound:          adbc.StatusNotFound,

	// 405: the server doesn't support this operation on the endpoint.
	protocol.CodeMethodNotAllowed: adbc.StatusNotImplemented,

	// 409 and 410: the handle is not in a state that allows the request.
	protocol.CodeResultNotReady:     adbc.StatusInvalidState,
	protocol.CodeResultNotResumable: adbc.StatusInvalidState,
	protocol.CodeIngestIncomplete:   adbc.StatusInvalidState,
	protocol.CodeResultExpired:      adbc.StatusInvalidState,
	protocol.CodeIngestExpired:      adbc.StatusInvalidState,
	protocol.CodeIngestPartExists:   adbc.StatusAlreadyExists,
	// 409: the statement's own end.
	protocol.CodeStatementCancelled:       adbc.StatusCancelled,
	protocol.CodeClientStatementCancelled: adbc.StatusCancelled,
	protocol.CodeStatementTimeout:         adbc.StatusTimeout,

	// 413, 415, 431.
	protocol.CodePayloadTooLarge:      adbc.StatusInvalidArgument,
	protocol.CodeHeadersTooLarge:      adbc.StatusInvalidArgument,
	protocol.CodeUnsupportedArrowType: adbc.StatusNotImplemented,

	// 421, 429, 503: retry (elsewhere, or later).
	protocol.CodeSessionElsewhere:    adbc.StatusIO,
	protocol.CodeTooManySessions:     adbc.StatusIO,
	protocol.CodeResultQuotaExceeded: adbc.StatusIO,
	protocol.CodeRateLimited:         adbc.StatusIO,
	protocol.CodeServerDraining:      adbc.StatusIO,

	// 422: the statement failed while running; the user's SQL or data.
	protocol.CodeExecutionError: adbc.StatusInvalidArgument,
	// 422 too, but the driver makes every Idempotency-Key (§6.7): a reused
	// one is the driver's fault.
	protocol.CodeIdempotencyKeyReused: adbc.StatusInternal,

	// 500 and 501.
	protocol.CodeInternalError:  adbc.StatusInternal,
	protocol.CodeNotImplemented: adbc.StatusNotImplemented,

	// The driver's own: an answer that broke the protocol, a failed
	// statement document without its error (the spec requires one).
	protocol.CodeClientProtocolError:   adbc.StatusInternal,
	protocol.CodeClientStatementFailed: adbc.StatusInternal,
	// client_http_error is absent on purpose: its HTTP status decides.
}

// statusOfProblem maps a Chargewire error code to an ADBC status, falling back
// to the HTTP status for codes this driver does not know (§12).
func statusOfProblem(p *protocol.Problem) adbc.Status {
	if p.Code == protocol.CodeSQLError || p.Code == protocol.CodeExecutionError {
		switch p.SQLState {
		case sqlstateUndefinedTable:
			return adbc.StatusNotFound
		case sqlstateDuplicateTable:
			return adbc.StatusAlreadyExists
		}
		if len(p.SQLState) == 5 && strings.HasPrefix(p.SQLState, sqlstateClassIntegrityViolation) {
			return adbc.StatusIntegrity
		}
	}
	if status, ok := codeStatuses[p.Code]; ok {
		return status
	}
	return statusOfHTTP(p.Status)
}

// statusOfHTTP is the status of an error code this driver does not know, or
// of an error status without a problem document (client_http_error), by its
// HTTP status (§11, §12): a 4xx is the request's doing, a 5xx the server's.
func statusOfHTTP(status int) adbc.Status {
	switch {
	case status == http.StatusUnauthorized:
		return adbc.StatusUnauthenticated
	case status == http.StatusForbidden:
		return adbc.StatusUnauthorized
	case status == http.StatusNotFound:
		return adbc.StatusNotFound
	case status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented:
		return adbc.StatusNotImplemented
	case status == http.StatusConflict || status == http.StatusGone:
		return adbc.StatusInvalidState
	case status == http.StatusGatewayTimeout:
		return adbc.StatusTimeout
	// 408: the server gave up waiting for the request, which may be retried.
	case status == http.StatusRequestTimeout || status == http.StatusMisdirectedRequest ||
		status == http.StatusTooManyRequests ||
		status == http.StatusBadGateway || status == http.StatusServiceUnavailable:
		return adbc.StatusIO
	case status >= 400 && status < 500:
		return adbc.StatusInvalidArgument
	case status == 0:
		// No status to go by: a failed statement document's error with a
		// code this driver doesn't know.
		return adbc.StatusUnknown
	}
	return adbc.StatusInternal
}

// statementError is the error of a failed or cancelled statement (§6.2). A
// document without its error gets a client_ code: the server sent none.
func statementError(doc *protocol.StatementDocument) error {
	if doc.Error != nil {
		if doc.Error.CancelReason == "" {
			doc.Error.CancelReason = doc.CancelReason
		}
		return doc.Error
	}
	code := protocol.CodeClientStatementFailed
	if doc.State == protocol.StateCancelled {
		code = protocol.CodeClientStatementCancelled
	}
	return &protocol.Problem{Code: code, CancelReason: doc.CancelReason,
		Detail: fmt.Sprintf("statement %s %s, and the server sent no error", doc.StatementID, doc.State)}
}
