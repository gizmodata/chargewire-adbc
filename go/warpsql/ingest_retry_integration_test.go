// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

const (
	retryBatches = 16
	retryRows    = retryBatches * 5_000
)

// rawClient sends requests by hand, one connection each, with a token.
type rawClient struct {
	http  *http.Client
	base  string
	token string
}

// call returns the status and body of one request, or its transport error.
func (c *rawClient) call(method, path, key, contentType string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(method, c.base+"/warp/v1"+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", contentType)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	answer, err := io.ReadAll(resp.Body)
	return resp.StatusCode, answer, err
}

// mustCall is call that fails the test unless the server answers 2xx; it
// decodes the answer into into (when not nil).
func (c *rawClient) mustCall(t *testing.T, method, path, key, contentType string, body []byte, into any) {
	t.Helper()
	status, answer, err := c.call(method, path, key, contentType, body)
	if err != nil || status >= 300 {
		t.Fatalf("%s %s: %d %s %v", method, path, status, answer, err)
	}
	if into != nil {
		if err := json.Unmarshal(answer, into); err != nil {
			t.Fatalf("%s %s answered %s: %v", method, path, answer, err)
		}
	}
}

// partsStream is partsBatches(retryBatches, 5_000) as one Arrow IPC stream:
// about 2 MB, more than the sockets buffer, so a body sent again is still
// being sent when the server answers it. (Much larger bodies lose fewer
// answers: Go's client then reads the answer before the reset arrives.)
func partsStream(t *testing.T) []byte {
	t.Helper()
	batches := partsBatches(retryBatches, 5_000)
	defer releaseAll(batches)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(partsSchema))
	for _, batch := range batches {
		if err := writer.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return stream.Bytes()
}

// resend sends a request again and again with the key it was first sent
// with; every time the answer must be the first one. The server answers
// without reading the body (§6.7, §8.3), so the answer comes back while the
// client is still sending: a server that then closes the connection with the
// body unread resets it, and the client loses the answer (Linux and macOS: a
// broken pipe or a reset; Windows discards it). The driver would retry such a
// lost answer, and an ingest whose retries are all lost fails.
func resend(t *testing.T, c *rawClient, path, key string, body, first []byte) {
	t.Helper()
	const times = 40
	lost := 0
	var lastErr error
	for range times {
		status, again, err := c.call(http.MethodPost, path, key, protocol.MediaTypeArrowStream, body)
		switch {
		case err != nil:
			lost++
			lastErr = err
		case status != http.StatusOK || !bytes.Equal(again, first):
			t.Fatalf("sent again: %d %s, want the first answer %s", status, again, first)
		}
	}
	if lost > 0 {
		t.Errorf("%d of %d answers to a request sent again were lost (the last: %v)", lost, times, lastErr)
	}
}

// Requests sent again with their Idempotency-Key are answered from the first
// one, without reading the body, and the answer arrives every time; the rows
// count once. Directly, and through the fault proxy (with no rules), which
// must not lose such answers on its own either.
func TestIntegrationIngestAnswerToARetryArrives(t *testing.T) {
	body := partsStream(t)
	for _, route := range []string{"direct", "proxy"} {
		t.Run(route, func(t *testing.T) {
			integrationTests.Add(1)
			c := &rawClient{http: &http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
				base: "http://" + shared.host, token: rawToken(t)}
			if route == "proxy" {
				c.base = newFaultProxy(t, shared).server.URL
			}
			var session struct {
				SessionID string `json:"session_id"`
			}
			c.mustCall(t, http.MethodPost, "/sessions", "", "application/json", []byte(`{}`), &session)
			sessionPath := "/sessions/" + session.SessionID
			defer c.call(http.MethodDelete, sessionPath, "", "application/json", nil) //nolint:errcheck

			t.Run("a part", func(t *testing.T) {
				table := "ing_retried_part_" + route
				var ingest struct {
					IngestID string `json:"ingest_id"`
				}
				c.mustCall(t, http.MethodPost, sessionPath+"/ingests", protocol.NewIdempotencyKey(), "application/json",
					[]byte(`{"table":"`+table+`","mode":"replace","temporary":false}`), &ingest)
				partPath := sessionPath + "/ingests/" + ingest.IngestID + "/parts/0"
				key := protocol.NewIdempotencyKey()
				status, first, err := c.call(http.MethodPost, partPath, key, protocol.MediaTypeArrowStream, body)
				if err != nil || status != http.StatusOK {
					t.Fatalf("part 0: %d %s %v", status, first, err)
				}
				resend(t, c, partPath, key, body, first)
				var committed struct {
					RowsIngested int64 `json:"rows_ingested"`
				}
				c.mustCall(t, http.MethodPost, sessionPath+"/ingests/"+ingest.IngestID+"/commit", "", "application/json",
					[]byte(`{"part_count":1}`), &committed)
				if committed.RowsIngested != retryRows {
					t.Fatalf("committed %d rows, want %d", committed.RowsIngested, retryRows)
				}
				checkPartsTable(t, openConn(t), table, retryRows)
			})
			t.Run("an upload", func(t *testing.T) {
				table := "ing_retried_upload_" + route
				path := sessionPath + "/ingest?" + url.Values{"table": {table}, "mode": {"replace"}}.Encode()
				key := protocol.NewIdempotencyKey()
				status, first, err := c.call(http.MethodPost, path, key, protocol.MediaTypeArrowStream, body)
				if err != nil || status != http.StatusOK {
					t.Fatalf("upload: %d %s %v", status, first, err)
				}
				if want := fmt.Sprintf(`"rows_ingested":%d`, retryRows); !bytes.Contains(first, []byte(want)) {
					t.Fatalf("upload answered %s, want %s", first, want)
				}
				resend(t, c, path, key, body, first)
				checkPartsTable(t, openConn(t), table, retryRows)
			})
		})
	}
}
