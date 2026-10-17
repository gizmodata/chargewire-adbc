// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// Error detail keys attached to errors that came from the server. The C API
// exposes them through AdbcErrorGetDetail, Python as Error.details.
const (
	// ErrorDetailCode is the Warp SQL error code (§11), e.g. "sql_error".
	ErrorDetailCode = "warp.code"
	// ErrorDetailRequestID is the server's request id, for its logs.
	ErrorDetailRequestID = "warp.request_id"
)

// ErrorCode returns the Warp SQL error code (§11) behind an error the driver
// returned, such as "sql_error" or "session_not_found", or "" when the error
// did not come from the server.
func ErrorCode(err error) string {
	if p, ok := errors.AsType[*protocol.Problem](err); ok {
		return p.Code
	}
	return ""
}

// wrapError turns an error from the protocol layer into an adbc.Error with
// the status the Warp SQL code stands for. op says what was being done.
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

// SQLSTATEs that say more than their error code.
const (
	sqlstateUndefinedTable = "42P01"
	sqlstateDuplicateTable = "42P07"
)

// statusOfProblem maps a Warp SQL error code to an ADBC status, falling back
// to the HTTP status for codes this driver does not know (§12).
func statusOfProblem(p *protocol.Problem) adbc.Status {
	switch p.SQLState {
	case sqlstateUndefinedTable:
		return adbc.StatusNotFound
	case sqlstateDuplicateTable:
		return adbc.StatusAlreadyExists
	}
	switch p.Code {
	case protocol.CodeBadRequest, protocol.CodeInvalidOptionValue, protocol.CodeSQLError,
		protocol.CodePayloadTooLarge, protocol.CodeHeadersTooLarge:
		return adbc.StatusInvalidArgument
	case protocol.CodeUnknownOption, protocol.CodeNotImplemented, protocol.CodeUnsupportedArrowType:
		return adbc.StatusNotImplemented
	case protocol.CodeInvalidCredentials, protocol.CodeInvalidToken, protocol.CodeSessionEnded:
		return adbc.StatusUnauthenticated
	case protocol.CodePermissionDenied, protocol.CodeReadOnly:
		return adbc.StatusUnauthorized
	case protocol.CodeSessionNotFound, protocol.CodeStatementNotFound, protocol.CodePreparedNotFound, protocol.CodeNotFound:
		return adbc.StatusNotFound
	case protocol.CodeResultNotReady, protocol.CodeResultNotResumable, protocol.CodeResultExpired,
		protocol.CodeMethodNotAllowed:
		return adbc.StatusInvalidState
	case protocol.CodeStatementTimeout:
		return adbc.StatusTimeout
	case protocol.CodeStatementCancelled:
		return adbc.StatusCancelled
	case protocol.CodeSessionElsewhere, protocol.CodeTooManySessions, protocol.CodeResultQuotaExceeded,
		protocol.CodeRateLimited, protocol.CodeServerDraining:
		return adbc.StatusIO
	case protocol.CodeExecutionError, protocol.CodeIdempotencyKeyReused:
		return adbc.StatusInternal
	}
	switch {
	case p.Status == http.StatusUnauthorized:
		return adbc.StatusUnauthenticated
	case p.Status == http.StatusForbidden:
		return adbc.StatusUnauthorized
	case p.Status == http.StatusNotFound:
		return adbc.StatusNotFound
	case p.Status == http.StatusConflict || p.Status == http.StatusGone:
		return adbc.StatusInvalidState
	case p.Status == http.StatusNotImplemented:
		return adbc.StatusNotImplemented
	case p.Status == http.StatusGatewayTimeout:
		return adbc.StatusTimeout
	case p.Status == http.StatusTooManyRequests || p.Status == http.StatusServiceUnavailable:
		return adbc.StatusIO
	case p.Status >= 400 && p.Status < 500:
		return adbc.StatusInvalidArgument
	}
	return adbc.StatusInternal
}

// statementError is the error of a failed or cancelled statement (§6.2).
func statementError(doc *protocol.StatementDocument) error {
	if doc.Error != nil {
		return doc.Error
	}
	code := protocol.CodeExecutionError
	if doc.State == protocol.StateCancelled {
		code = protocol.CodeStatementCancelled
	}
	return &protocol.Problem{Code: code, Detail: fmt.Sprintf("statement %s %s", doc.StatementID, doc.State)}
}
