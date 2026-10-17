// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

// StatementRef names a statement: statements live in the session that ran
// them, which may since have been replaced.
type StatementRef struct {
	SessionID   string
	StatementID string
}

func (r StatementRef) path() string {
	return sessionPath(r.SessionID) + "/statements/" + url.PathEscape(r.StatementID)
}

// PreparedRef names a prepared statement in its session.
type PreparedRef struct {
	SessionID  string
	PreparedID string
}

func (r PreparedRef) path() string {
	return sessionPath(r.SessionID) + "/prepared/" + url.PathEscape(r.PreparedID)
}

// Submission is a request that creates a statement: a submit (§6.1) or a
// prepared statement's execute (§7.2). It keeps its body and Idempotency-Key,
// so the same request can be sent again to learn the statement it created.
type Submission struct {
	// Prepared, when set, executes that prepared statement; otherwise Body
	// is a submit's {"sql", "parameters"} document.
	Prepared *PreparedRef
	Body     *Body
	// Arrow asks for the streamed path (§6.1): a query's rows in the
	// response. Otherwise the answer is a statement document.
	Arrow bool
	// RespondAsync asks for an answer as soon as the statement is accepted;
	// otherwise the server may hold the answer up to Wait.
	RespondAsync bool
	Wait         time.Duration
	// Retain, when set, chooses whether the result is kept for re-reading
	// (§6.5).
	Retain *bool
	// Timeout, when positive, is the statement timeout (§6.9).
	Timeout time.Duration
	// QueryTag is a JSON object recorded with the statement.
	QueryTag string
	// Zstd accepts Arrow buffer compression (§8.2).
	Zstd bool

	key       string
	sessionID string
	sent      atomic.Bool
}

// SessionID is the session the submission went to (after Submit).
func (s *Submission) SessionID() string { return s.sessionID }

// Sent reports whether the request was written to the server in full, so
// the server may have created the statement even if no answer came back.
func (s *Submission) Sent() bool { return s.sent.Load() }

func (s *Submission) path() string {
	if s.Prepared != nil {
		return s.Prepared.path() + "/execute"
	}
	return sessionPath(s.sessionID) + "/statements"
}

func (s *Submission) request(arrow, async bool) *Request {
	header := http.Header{}
	if arrow {
		header.Set("Accept", MediaTypeArrowStream)
	}
	switch {
	case async:
		header.Set("Prefer", "respond-async")
	case !arrow && s.Wait > 0:
		header.Set("Prefer", "wait="+strconv.Itoa(int(s.Wait/time.Second)))
	}
	header.Set("Idempotency-Key", s.key)
	if s.Retain != nil {
		header.Set("Warp-Retain-Result", strconv.FormatBool(*s.Retain))
	}
	if s.Timeout > 0 {
		header.Set("Warp-Statement-Timeout", strconv.FormatInt(max(s.Timeout.Milliseconds(), 1), 10))
	}
	if s.QueryTag != "" {
		header.Set("Warp-Query-Tag", s.QueryTag)
	}
	if s.Zstd {
		header.Set("Warp-IPC-Compression", "zstd")
	}
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			s.sent.Store(true)
		}
	}}
	return &Request{Method: http.MethodPost, Path: s.path(), Header: header, Body: s.Body, Idempotent: true, Trace: trace}
}

// SubmitDocument is a submit's {"sql", "parameters"} body. Parameters is a
// JSON array (positional) or object (named), or nil.
func SubmitDocument(sql string, parameters any) (*Body, error) {
	doc := map[string]any{"sql": sql}
	if parameters != nil {
		doc["parameters"] = parameters
	}
	return JSONBody(doc)
}

// Submit sends sub. The answer is either an Arrow stream (streamed path) or a
// statement document; the caller checks Content-Type (§6.1) and closes the
// body. The statement's id is in the Warp-Statement-Id header or the
// document.
func (s *Session) Submit(ctx context.Context, sub *Submission) (*http.Response, error) {
	if sub.key == "" {
		sub.key = NewIdempotencyKey()
	}
	if sub.Prepared != nil {
		sub.sessionID = sub.Prepared.SessionID
		return s.doHandle(ctx, sub.sessionID, "prepared statement", sub.request(sub.Arrow, sub.RespondAsync))
	}
	return s.do(ctx, ScopeStatement, "", func(id string) (*Request, error) {
		sub.sessionID = id
		return sub.request(sub.Arrow, sub.RespondAsync), nil
	})
}

// ErrNotReplayable is a submission that cannot be sent again because its body
// streamed.
var ErrNotReplayable = errors.New("the request body was streamed and cannot be sent again")

// Recall sends a submission again, with the same Idempotency-Key, to learn the
// statement it created without running it twice (§6.7). This names a
// statement whose answer has not arrived, for example to cancel it.
func (s *Session) Recall(ctx context.Context, sub *Submission) (StatementRef, error) {
	if !sub.Body.Replayable() || sub.key == "" {
		return StatementRef{}, ErrNotReplayable
	}
	doc := &StatementDocument{}
	if _, err := s.client.doJSON(ctx, sub.request(false, true), doc); err != nil {
		return StatementRef{}, err
	}
	return StatementRef{SessionID: sub.sessionID, StatementID: doc.StatementID}, nil
}

// Status reads a statement document (§6.3). A positive wait asks the server
// to hold the answer until the state changes or wait passes (long poll).
func (s *Session) Status(ctx context.Context, ref StatementRef, wait time.Duration) (*StatementDocument, error) {
	header := http.Header{}
	if wait > 0 {
		header.Set("Prefer", "wait="+strconv.Itoa(int(wait/time.Second)))
	}
	resp, err := s.doHandle(ctx, ref.SessionID, "statement", &Request{Method: http.MethodGet, Path: ref.path(), Header: header, Idempotent: true})
	if err != nil {
		return nil, err
	}
	doc := &StatementDocument{}
	return doc, decodeJSON(resp, doc)
}

// WaitDone long-polls until the statement reaches a terminal state.
func (s *Session) WaitDone(ctx context.Context, ref StatementRef, doc *StatementDocument, poll time.Duration) (*StatementDocument, error) {
	for !doc.State.Terminal() {
		var err error
		if doc, err = s.Status(ctx, ref, poll); err != nil {
			return nil, err
		}
	}
	return doc, nil
}

// ResultRequest asks for a partition of a result (§6.4).
type ResultRequest struct {
	Ref       StatementRef
	Partition int
	FromBatch int64
	// Location is the partition's server, when not this one.
	Location *url.URL
	Zstd     bool
}

// Result opens a result stream. The caller closes the body.
func (s *Session) Result(ctx context.Context, rr ResultRequest) (*http.Response, error) {
	query := url.Values{}
	if rr.Partition != 0 {
		query.Set("partition", strconv.Itoa(rr.Partition))
	}
	if rr.FromBatch != 0 {
		query.Set("from_batch", strconv.FormatInt(rr.FromBatch, 10))
	}
	header := http.Header{"Accept": {MediaTypeArrowStream}}
	if rr.Zstd {
		header.Set("Warp-IPC-Compression", "zstd")
	}
	req := &Request{Method: http.MethodGet, Path: rr.Ref.path() + "/result", Query: query, Header: header,
		Idempotent: true, BaseURL: rr.Location}
	return s.doHandle(ctx, rr.Ref.SessionID, "result", req)
}

// Cancel asks the server to stop a statement (§6.8). Cancelling a finished
// statement is harmless.
func (s *Session) Cancel(ctx context.Context, ref StatementRef) error {
	_, err := s.client.doJSON(ctx, &Request{Method: http.MethodPost, Path: ref.path() + "/cancel", Idempotent: true}, nil)
	return err
}

// DeleteStatement cancels a statement if it runs and frees its result (§6.8).
func (s *Session) DeleteStatement(ctx context.Context, ref StatementRef) error {
	_, err := s.client.doJSON(ctx, &Request{Method: http.MethodDelete, Path: ref.path(), Idempotent: true}, nil)
	if HasCode(err, CodeStatementNotFound) || HasCode(err, CodeSessionNotFound) {
		return nil
	}
	return err
}

// Prepare creates a prepared statement (§7.1).
func (s *Session) Prepare(ctx context.Context, sql string) (*PreparedDocument, PreparedRef, error) {
	body, err := JSONBody(map[string]string{"sql": sql})
	if err != nil {
		return nil, PreparedRef{}, err
	}
	var sessionID string
	resp, err := s.do(ctx, ScopeSession, "", func(id string) (*Request, error) {
		sessionID = id
		return &Request{Method: http.MethodPost, Path: sessionPath(id) + "/prepared", Body: body, Idempotent: true}, nil
	})
	if err != nil {
		return nil, PreparedRef{}, err
	}
	doc := &PreparedDocument{}
	if err := decodeJSON(resp, doc); err != nil {
		return nil, PreparedRef{}, err
	}
	return doc, PreparedRef{SessionID: sessionID, PreparedID: doc.PreparedID}, nil
}

// ClosePrepared deletes a prepared statement (§7.3).
func (s *Session) ClosePrepared(ctx context.Context, ref PreparedRef) error {
	_, err := s.client.doJSON(ctx, &Request{Method: http.MethodDelete, Path: ref.path(), Idempotent: true}, nil)
	if HasCode(err, CodePreparedNotFound) || HasCode(err, CodeSessionNotFound) {
		return nil
	}
	return err
}

// IngestRequest is a bulk upload (§8.1).
type IngestRequest struct {
	Table, Catalog, Schema string
	// Mode is create, append, replace or create_append.
	Mode      string
	Temporary bool
	// Body is the Arrow IPC stream.
	Body *Body
}

// Ingest uploads rows into a table and returns how many were written.
func (s *Session) Ingest(ctx context.Context, ir IngestRequest) (int64, error) {
	query := url.Values{"table": {ir.Table}, "mode": {ir.Mode}}
	if ir.Catalog != "" {
		query.Set("catalog", ir.Catalog)
	}
	if ir.Schema != "" {
		query.Set("schema", ir.Schema)
	}
	if ir.Temporary {
		query.Set("temporary", "true")
	}
	header := http.Header{"Idempotency-Key": {NewIdempotencyKey()}}
	resp, err := s.do(ctx, ScopeStatement, "", func(id string) (*Request, error) {
		return &Request{Method: http.MethodPost, Path: sessionPath(id) + "/ingest", Query: query, Header: header,
			Body: ir.Body, Idempotent: true}, nil
	})
	if err != nil {
		return 0, err
	}
	result := &ingestResult{}
	if err := decodeJSON(resp, result); err != nil {
		return 0, err
	}
	return result.RowsIngested, nil
}

// Metadata reads a metadata endpoint (§10): an Arrow stream with Flight SQL's
// schema. The caller closes the body.
func (s *Session) Metadata(ctx context.Context, kind string, query url.Values) (*http.Response, error) {
	return s.do(ctx, ScopeSession, "", func(id string) (*Request, error) {
		return &Request{Method: http.MethodGet, Path: sessionPath(id) + "/metadata/" + kind, Query: query,
			Header: http.Header{"Accept": {MediaTypeArrowStream}}, Idempotent: true}, nil
	})
}

// ServerInfo reads the server-info document (§10).
func (s *Session) ServerInfo(ctx context.Context) (map[string]any, error) {
	resp, err := s.do(ctx, ScopeSession, "", func(id string) (*Request, error) {
		return &Request{Method: http.MethodGet, Path: sessionPath(id) + "/metadata/server-info", Idempotent: true}, nil
	})
	if err != nil {
		return nil, err
	}
	info := map[string]any{}
	return info, decodeJSON(resp, &info)
}

// DecodeStatementDocument reads a statement document from a response.
func DecodeStatementDocument(resp *http.Response) (*StatementDocument, error) {
	doc := &StatementDocument{}
	if err := decodeJSON(resp, doc); err != nil {
		return nil, err
	}
	if doc.StatementID == "" {
		return nil, fmt.Errorf("the server answered %s without a statement id", resp.Request.URL.Path)
	}
	return doc, nil
}

// ValidQueryTag checks that tag is a JSON object, as Warp-Query-Tag requires.
func ValidQueryTag(tag string) error {
	var object map[string]any
	if err := json.Unmarshal([]byte(tag), &object); err != nil {
		return fmt.Errorf("a query tag must be a JSON object: %w", err)
	}
	return nil
}
