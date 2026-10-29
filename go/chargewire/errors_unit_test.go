// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/chargewire-adbc/go/internal/protocol"
)

// wantCodeStatuses is the status each Chargewire code (§11) must give, written
// out apart from codeStatuses so that a change there has to be made here too.
// A user's SQL that fails, before or while it runs, is never StatusInternal.
var wantCodeStatuses = map[string]adbc.Status{
	"bad_request":                   adbc.StatusInvalidArgument,
	"unknown_option":                adbc.StatusNotImplemented,
	"invalid_option_value":          adbc.StatusInvalidArgument,
	"invalid_return_to":             adbc.StatusInvalidArgument,
	"sql_error":                     adbc.StatusInvalidArgument,
	"unsupported_grant_type":        adbc.StatusUnauthenticated,
	"invalid_credentials":           adbc.StatusUnauthenticated,
	"invalid_token":                 adbc.StatusUnauthenticated,
	"session_ended":                 adbc.StatusUnauthenticated,
	"certificate_required":          adbc.StatusUnauthenticated,
	"unknown_certificate_identity":  adbc.StatusUnauthenticated,
	"certificate_revoked":           adbc.StatusUnauthenticated,
	"certificate_mismatch":          adbc.StatusUnauthenticated,
	"certificate_identity_mismatch": adbc.StatusUnauthenticated,
	"permission_denied":             adbc.StatusUnauthorized,
	"read_only":                     adbc.StatusUnauthorized,
	"session_not_found":             adbc.StatusNotFound,
	"statement_not_found":           adbc.StatusNotFound,
	"prepared_not_found":            adbc.StatusNotFound,
	"ingest_not_found":              adbc.StatusNotFound,
	"flow_not_found":                adbc.StatusUnauthenticated,
	"partition_not_found":           adbc.StatusNotFound,
	"not_found":                     adbc.StatusNotFound,
	"method_not_allowed":            adbc.StatusNotImplemented,
	"result_not_ready":              adbc.StatusInvalidState,
	"result_not_resumable":          adbc.StatusInvalidState,
	"ingest_incomplete":             adbc.StatusInvalidState,
	"ingest_part_exists":            adbc.StatusAlreadyExists,
	"statement_cancelled":           adbc.StatusCancelled,
	"statement_timeout":             adbc.StatusTimeout,
	"result_expired":                adbc.StatusInvalidState,
	"ingest_expired":                adbc.StatusInvalidState,
	"payload_too_large":             adbc.StatusInvalidArgument,
	"unsupported_arrow_type":        adbc.StatusNotImplemented,
	"session_elsewhere":             adbc.StatusIO,
	"idempotency_key_reused":        adbc.StatusInternal, // the driver makes the keys
	"execution_error":               adbc.StatusInvalidArgument,
	"too_many_sessions":             adbc.StatusIO,
	"result_quota_exceeded":         adbc.StatusIO,
	"rate_limited":                  adbc.StatusIO,
	"headers_too_large":             adbc.StatusInvalidArgument,
	"internal_error":                adbc.StatusInternal,
	"not_implemented":               adbc.StatusNotImplemented,
	"server_draining":               adbc.StatusIO,
	"access_denied":                 adbc.StatusUnauthenticated,
	"identity_provider_error":       adbc.StatusUnauthenticated,

	// The driver's own codes (§11, "Codes a client makes up").
	"client_protocol_error":      adbc.StatusInternal,
	"client_statement_failed":    adbc.StatusInternal,
	"client_statement_cancelled": adbc.StatusCancelled,
	"client_sign_in_failed":      adbc.StatusUnauthenticated,
}

// specTableRow is a row of the error table: its Code and Status columns.
var specTableRow = regexp.MustCompile("^\\|\\s*(.+?)\\s*\\|\\s*(\\S+)\\s*\\|")

// specErrorTable reads the codes and HTTP statuses of a §11 error table in
// Markdown: every backticked code in a row's first column.
func specErrorTable(t *testing.T, markdown string) map[string]int {
	t.Helper()
	codes := map[string]int{}
	code := regexp.MustCompile("`([a-z_]+)`")
	for line := range strings.Lines(markdown) {
		row := specTableRow.FindStringSubmatch(line)
		if row == nil || row[1] == "Code" || strings.HasPrefix(row[1], "-") {
			continue
		}
		status, _ := strconv.Atoi(row[2]) // "—": no status
		for _, match := range code.FindAllStringSubmatch(row[1], -1) {
			codes[match[1]] = status
		}
	}
	if len(codes) < 40 {
		t.Fatalf("read only %d codes from the error table: is it still a §11 table?", len(codes))
	}
	return codes
}

// specSection11 cuts §11 (Errors) out of a whole chargewire.md.
func specSection11(t *testing.T, spec string) string {
	t.Helper()
	start := strings.Index(spec, "\n## 11. Errors")
	if start < 0 {
		t.Fatal("the spec has no '## 11. Errors' section")
	}
	section := spec[start+1:]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	return section
}

// protocolCodes reads the values of the protocol package's Code* constants.
func protocolCodes(t *testing.T) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "../internal/protocol/problem.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if !strings.HasPrefix(name.Name, "Code") || i >= len(value.Values) {
					continue
				}
				if lit, ok := value.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					codes[name.Name], _ = strconv.Unquote(lit.Value)
				}
			}
		}
	}
	return codes
}

// Every code maps to the status that says what to do about it.
func TestErrorStatusesEveryCode(t *testing.T) {
	for code, want := range wantCodeStatuses {
		if got := statusOfProblem(&protocol.Problem{Code: code}); got != want {
			t.Errorf("%s -> %v, want %v", code, got, want)
		}
		// The HTTP status never overrides a code the driver knows.
		if got := statusOfProblem(&protocol.Problem{Code: code, Status: http.StatusInternalServerError}); got != want {
			t.Errorf("%s with HTTP 500 -> %v, want %v", code, got, want)
		}
	}
	for code := range codeStatuses {
		if _, ok := wantCodeStatuses[code]; !ok {
			t.Errorf("codeStatuses maps %s, which this test doesn't check", code)
		}
	}
}

// The spec's error table and the protocol package's codes are all mapped: a
// code the spec gains fails this test until it has a status. The table is the
// copy in testdata; CHARGEWIRE_ADBC_TEST_SPEC may name a chargewire.md to check
// as well (and the copy against it).
func TestErrorStatusesCoverTheSpec(t *testing.T) {
	snapshot, err := os.ReadFile("testdata/chargewire-errors.md")
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]map[string]int{"testdata/chargewire-errors.md": specErrorTable(t, string(snapshot))}
	if path := os.Getenv("CHARGEWIRE_ADBC_TEST_SPEC"); path != "" {
		spec, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("CHARGEWIRE_ADBC_TEST_SPEC: %v", err)
		}
		live := specErrorTable(t, specSection11(t, string(spec)))
		tables[path] = live
		for code := range live {
			if _, ok := tables["testdata/chargewire-errors.md"][code]; !ok {
				t.Errorf("%s has %s, testdata/chargewire-errors.md doesn't: copy the new table there", path, code)
			}
		}
	}
	for source, codes := range tables {
		for code, httpStatus := range codes {
			if strings.HasPrefix(code, "client_") {
				t.Errorf("%s: servers never send a client_ code, but the table has %s", source, code)
			}
			if _, ok := codeStatuses[code]; !ok {
				t.Errorf("%s: %s (HTTP %d) has no status in codeStatuses", source, code, httpStatus)
			}
			if _, ok := wantCodeStatuses[code]; !ok {
				t.Errorf("%s: %s is not in wantCodeStatuses", source, code)
			}
			// A statement's own outcome is a 4xx (§11): only a 5xx may be
			// reported as the driver's or the server's fault.
			if codeStatuses[code] == adbc.StatusInternal && httpStatus < 500 && code != protocol.CodeIdempotencyKeyReused {
				t.Errorf("%s: %s is HTTP %d, the request's doing, but maps to StatusInternal", source, code, httpStatus)
			}
		}
	}
	for name, code := range protocolCodes(t) {
		if code == protocol.CodeClientHTTPError {
			if _, ok := codeStatuses[code]; ok {
				t.Error("client_http_error must fall back to its HTTP status, not have a status of its own")
			}
			continue
		}
		if _, ok := codeStatuses[code]; !ok {
			t.Errorf("protocol.%s (%s) has no status in codeStatuses", name, code)
		}
	}
}

// A SQLSTATE refines a statement's SQL error (§11), and only that.
func TestErrorStatusesSQLState(t *testing.T) {
	for _, tc := range []struct {
		code, sqlstate string
		want           adbc.Status
	}{
		{protocol.CodeSQLError, "42P01", adbc.StatusNotFound},
		{protocol.CodeSQLError, "42P07", adbc.StatusAlreadyExists},
		{protocol.CodeSQLError, "42601", adbc.StatusInvalidArgument},
		{protocol.CodeExecutionError, "42P01", adbc.StatusNotFound},
		{protocol.CodeExecutionError, "42P07", adbc.StatusAlreadyExists},
		{protocol.CodeExecutionError, "22012", adbc.StatusInvalidArgument},
		{protocol.CodeExecutionError, "23000", adbc.StatusIntegrity},
		{protocol.CodeExecutionError, "23505", adbc.StatusIntegrity},
		{protocol.CodeSQLError, "23502", adbc.StatusIntegrity},
		{protocol.CodeExecutionError, "2300", adbc.StatusInvalidArgument},
		{protocol.CodeInvalidOptionValue, "23000", adbc.StatusInvalidArgument},
		{protocol.CodePermissionDenied, "42P01", adbc.StatusUnauthorized},
		{protocol.CodeSessionNotFound, "42P07", adbc.StatusNotFound},
	} {
		if got := statusOfProblem(&protocol.Problem{Code: tc.code, SQLState: tc.sqlstate}); got != tc.want {
			t.Errorf("%s %s -> %v, want %v", tc.code, tc.sqlstate, got, tc.want)
		}
	}
}

// A code the driver doesn't know, or an error status without a problem
// document, falls back to its HTTP status (§12): 4xx the request's doing,
// 5xx the server's.
func TestErrorStatusesHTTPFallback(t *testing.T) {
	for status, want := range map[int]adbc.Status{
		http.StatusBadRequest:            adbc.StatusInvalidArgument,
		http.StatusUnauthorized:          adbc.StatusUnauthenticated,
		http.StatusForbidden:             adbc.StatusUnauthorized,
		http.StatusNotFound:              adbc.StatusNotFound,
		http.StatusMethodNotAllowed:      adbc.StatusNotImplemented,
		http.StatusRequestTimeout:        adbc.StatusIO,
		http.StatusConflict:              adbc.StatusInvalidState,
		http.StatusGone:                  adbc.StatusInvalidState,
		http.StatusRequestEntityTooLarge: adbc.StatusInvalidArgument,
		http.StatusTeapot:                adbc.StatusInvalidArgument,
		http.StatusMisdirectedRequest:    adbc.StatusIO,
		http.StatusUnprocessableEntity:   adbc.StatusInvalidArgument,
		http.StatusTooManyRequests:       adbc.StatusIO,
		http.StatusInternalServerError:   adbc.StatusInternal,
		http.StatusNotImplemented:        adbc.StatusNotImplemented,
		http.StatusBadGateway:            adbc.StatusIO,
		http.StatusServiceUnavailable:    adbc.StatusIO,
		http.StatusGatewayTimeout:        adbc.StatusTimeout,
		http.StatusInsufficientStorage:   adbc.StatusInternal,
		0:                                adbc.StatusUnknown,
	} {
		for _, code := range []string{"a_future_code", protocol.CodeClientHTTPError} {
			if got := statusOfProblem(&protocol.Problem{Code: code, Status: status}); got != want {
				t.Errorf("%s with HTTP %d -> %v, want %v", code, status, got, want)
			}
		}
	}
}

// The adbc.Error keeps the server's message, code, SQLSTATE and request id.
func TestErrorStatusesWrappedError(t *testing.T) {
	err := wrapError(&protocol.Problem{Code: protocol.CodeExecutionError, Status: 422, SQLState: "22012",
		RequestID: "r_1", Detail: "Conversion Error: division by zero"}, "ExecuteQuery")
	adbcErr, ok := errors.AsType[adbc.Error](err)
	if !ok {
		t.Fatalf("not an adbc.Error: %v", err)
	}
	if adbcErr.Code != adbc.StatusInvalidArgument || string(adbcErr.SqlState[:]) != "22012" || ErrorCode(err) != "execution_error" {
		t.Fatalf("wrapped %+v", adbcErr)
	}
	if errorDetail(err, ErrorDetailCode) != "execution_error" || errorDetail(err, ErrorDetailRequestID) != "r_1" {
		t.Errorf("details %+v", adbcErr.Details)
	}
	for _, part := range []string{"[chargewire] ExecuteQuery", "division by zero", "execution_error", "HTTP 422"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("message %q lacks %q", err, part)
		}
	}
	if strings.Count(err.Error(), "division by zero") != 1 {
		t.Errorf("message repeats itself: %s", err)
	}
	if wrapError(context.Canceled, "x").(*driverError).adbcErr.Code != adbc.StatusCancelled {
		t.Error("a cancelled context is not StatusCancelled")
	}
	if wrapError(context.DeadlineExceeded, "x").(*driverError).adbcErr.Code != adbc.StatusTimeout {
		t.Error("a context past its deadline is not StatusTimeout")
	}
	if wrapError(&protocol.TransportError{Op: "POST /statements", Err: errors.New("connection reset")}, "x").(*driverError).adbcErr.Code != adbc.StatusIO {
		t.Error("a transport failure is not StatusIO")
	}
}
