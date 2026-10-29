// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// IngestHandle is an open partitioned ingest (§8.3). It belongs to the
// session it was opened in: its parts, its commit and its abort go there and
// are never sent to a replacement session.
type IngestHandle struct {
	SessionID string
	ID        string
}

func (h IngestHandle) path() string {
	return sessionPath(h.SessionID) + "/ingests/" + url.PathEscape(h.ID)
}

type ingestTarget struct {
	Table     string `json:"table"`
	Catalog   string `json:"catalog,omitempty"`
	Schema    string `json:"schema,omitempty"`
	Mode      string `json:"mode"`
	Temporary bool   `json:"temporary"`
}

// ingestDocument is an ingest's document (§8.3), and the answer to its open
// and its commit.
type ingestDocument struct {
	IngestID     string       `json:"ingest_id"`
	State        string       `json:"state"`
	Parts        []ingestPart `json:"parts"`
	RowsIngested int64        `json:"rows_ingested"`
	Error        *Problem     `json:"error"`
}

// ingestPart is a part whose upload finished (§8.3).
type ingestPart struct {
	Part         int   `json:"part"`
	RowsReceived int64 `json:"rows_received"`
}

// OpenIngest opens a partitioned ingest into ir's target (ir.Body is unused).
// Nothing is uploaded yet, so a lost session is replaced and the open sent
// again, as for a statement.
func (s *Session) OpenIngest(ctx context.Context, ir IngestRequest) (IngestHandle, error) {
	body, err := JSONBody(ingestTarget{Table: ir.Table, Catalog: ir.Catalog, Schema: ir.Schema,
		Mode: ir.Mode, Temporary: ir.Temporary})
	if err != nil {
		return IngestHandle{}, err
	}
	header := http.Header{"Idempotency-Key": {NewIdempotencyKey()}}
	if ir.QueryTag != "" {
		header.Set("Chargewire-Query-Tag", ir.QueryTag)
	}
	var sessionID string
	resp, err := s.do(ctx, ScopeStatement, "", func(id string) (*Request, error) {
		sessionID = id
		return &Request{Method: http.MethodPost, Path: sessionPath(id) + "/ingests", Header: header,
			Body: body, Idempotent: true}, nil
	})
	if err != nil {
		return IngestHandle{}, err
	}
	doc := &ingestDocument{}
	if err := decodeJSON(resp, doc); err != nil {
		return IngestHandle{}, err
	}
	if doc.IngestID == "" {
		return IngestHandle{}, ProtocolError("the server opened an ingest without an ingest_id")
	}
	return IngestHandle{SessionID: sessionID, ID: doc.IngestID}, nil
}

// IngestPart uploads part number part of an open ingest and returns the rows
// the server read from it. A part's Idempotency-Key is its own, so a part
// whose answer was lost is answered again rather than refused (§8.3).
func (s *Session) IngestPart(ctx context.Context, h IngestHandle, part int, body *Body) (int64, error) {
	req := &Request{Method: http.MethodPost, Path: h.path() + "/parts/" + strconv.Itoa(part),
		Header: http.Header{"Idempotency-Key": {NewIdempotencyKey()}}, Body: body, Idempotent: true}
	resp, err := s.doHandle(ctx, h.SessionID, "partitioned ingest", req)
	if err != nil {
		return 0, err
	}
	answer := &struct {
		RowsReceived int64 `json:"rows_received"`
	}{}
	return answer.RowsReceived, decodeJSON(resp, answer)
}

// CommitIngest commits parts 0..parts-1 as one and returns the rows written.
// Committing again answers with the same result, so it is safe to retry.
func (s *Session) CommitIngest(ctx context.Context, h IngestHandle, parts int) (int64, error) {
	body, err := JSONBody(map[string]int{"part_count": parts})
	if err != nil {
		return 0, err
	}
	req := &Request{Method: http.MethodPost, Path: h.path() + "/commit", Body: body, Idempotent: true}
	resp, err := s.doHandle(ctx, h.SessionID, "partitioned ingest", req)
	if err != nil {
		return 0, err
	}
	doc := &ingestDocument{}
	return doc.RowsIngested, decodeJSON(resp, doc)
}

// AbortIngest aborts an ingest: nothing it received is written. An ingest
// the server no longer knows is already gone.
func (s *Session) AbortIngest(ctx context.Context, h IngestHandle) error {
	req := &Request{Method: http.MethodDelete, Path: h.path(), Idempotent: true}
	resp, err := s.doHandle(ctx, h.SessionID, "partitioned ingest", req)
	if HasCode(err, CodeIngestNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return resp.Body.Close()
}
