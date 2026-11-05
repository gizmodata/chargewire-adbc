// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock stands in for the client's clock in the resume waits: a sleep
// records its duration and moves the clock on at once.
type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	waits []time.Duration
	// sleepErr, when set, is what a sleep returns (a context that ended).
	sleepErr error
}

func installFakeClock(client *Client) *fakeClock {
	clock := &fakeClock{now: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)}
	client.now = func() time.Time {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		return clock.now
	}
	client.sleep = func(_ context.Context, d time.Duration) error {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		if clock.sleepErr != nil {
			return clock.sleepErr
		}
		clock.waits = append(clock.waits, d)
		clock.now = clock.now.Add(d)
		return nil
	}
	return clock
}

func (c *fakeClock) recorded() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.waits)
}

// resumingServer is a stub of a stopped server that is resuming: each
// route answers server_resuming (with Retry-After retryAfter, "" for none)
// the number of times set for it (-1: always), then answers normally. It
// records every request body, to show the same request is sent each time.
type resumingServer struct {
	server     *httptest.Server
	retryAfter string

	mu     sync.Mutex
	left   map[string]int // route -> server_resuming answers still to give
	calls  map[string]int
	bodies map[string][]string
}

const (
	routeToken   = "POST /chargewire/v1/auth/token"
	routeExecute = "POST /chargewire/v1/sessions/s1/statements"
)

func newResumingServer(t *testing.T, retryAfter string, resuming map[string]int) *resumingServer {
	s := &resumingServer{retryAfter: retryAfter, left: resuming, calls: map[string]int{}, bodies: map[string][]string{}}
	mux := http.NewServeMux()
	handle := func(route string, answer http.HandlerFunc) {
		mux.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			s.mu.Lock()
			s.calls[route]++
			s.bodies[route] = append(s.bodies[route], string(body))
			resume := s.left[route] != 0
			if s.left[route] > 0 {
				s.left[route]--
			}
			s.mu.Unlock()
			if resume {
				if s.retryAfter != "" {
					w.Header().Set("Retry-After", s.retryAfter)
				}
				w.Header().Set("Content-Type", MediaTypeProblem)
				w.Header().Set("Chargewire-Request-Id", "r_resume")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{"type": "urn:chargewire:error:server_resuming",
					"status": 503, "code": CodeServerResuming, "detail": "the server is resuming; retry after 5 s"})
				return
			}
			answer(w, r)
		})
	}
	handle(routeToken, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"access_token": "tok", "token_type": "Bearer", "expires_in": 3600})
	})
	handle(routeExecute, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"statement_id": "q1", "state": "succeeded"})
	})
	s.server = httptest.NewServer(mux)
	t.Cleanup(s.server.Close)
	return s
}

func (s *resumingServer) count(route string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[route]
}

// sameBodies reports whether every request to route carried the same body.
func (s *resumingServer) sameBodies(route string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	bodies := s.bodies[route]
	for _, body := range bodies {
		if body != bodies[0] {
			return false
		}
	}
	return len(bodies) > 0
}

// client is a client of s allowing attempts per request, with this resume
// timeout and a fake clock.
func (s *resumingServer) client(attempts int, resumeTimeout time.Duration) (*Client, *fakeClock) {
	base, _ := url.Parse(s.server.URL + "/chargewire")
	client := NewClient(Config{BaseURL: base, Credentials: password, MaxAttempts: attempts, ResumeTimeout: resumeTimeout})
	return client, installFakeClock(client)
}

// execute sends a statement, as a request that is not idempotent and carries
// a body.
func execute(ctx context.Context, client *Client) error {
	body, _ := JSONBody(map[string]string{"sql": "SELECT 1"})
	resp, err := client.Do(ctx, &Request{Method: http.MethodPost, Path: "/v1/sessions/s1/statements", Body: body})
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func repeat(d time.Duration, n int) []time.Duration {
	waits := make([]time.Duration, n)
	for i := range waits {
		waits[i] = d
	}
	return waits
}

// The sign-in that wakes a stopped server is sent again, unchanged, after each
// Retry-After, until the server has resumed. A client that allows one attempt
// per request still waits: a resuming server is not a failing one.
func TestServerResumingSignInIsRepeated(t *testing.T) {
	s := newResumingServer(t, "5", map[string]int{routeToken: 6})
	client, clock := s.client(1, DefaultResumeTimeout)
	if err := client.Authenticate(context.Background()); err != nil {
		t.Fatalf("sign-in through a resume: %v", err)
	}
	if got := s.count(routeToken); got != 7 {
		t.Fatalf("%d sign-in requests, want 7 (6 answered server_resuming)", got)
	}
	if !s.sameBodies(routeToken) {
		t.Fatal("the repeated sign-in was not the same request")
	}
	if got := clock.recorded(); !slices.Equal(got, repeat(5*time.Second, 6)) {
		t.Fatalf("waits %v, want six of Retry-After's 5s", got)
	}
}

// A later request (a statement, not idempotent) is repeated through a resume
// too, more often than the client's MaxAttempts: those repeats don't count.
func TestServerResumingStatementIsRepeated(t *testing.T) {
	s := newResumingServer(t, "2", map[string]int{routeExecute: 9})
	client, clock := s.client(4, DefaultResumeTimeout)
	if err := execute(context.Background(), client); err != nil {
		t.Fatalf("statement through a resume: %v", err)
	}
	if got := s.count(routeExecute); got != 10 {
		t.Fatalf("%d statement requests, want 10", got)
	}
	if !s.sameBodies(routeExecute) {
		t.Fatal("the repeated statement was not the same request")
	}
	if got := clock.recorded(); !slices.Equal(got, repeat(2*time.Second, 9)) {
		t.Fatalf("waits %v, want nine of 2s", got)
	}
}

// Once the next attempt would start after the resume deadline (counted from
// the first attempt), the server_resuming error is returned, with the
// server's message and code, and how long the client waited.
func TestServerResumingDeadlineReturnsTheError(t *testing.T) {
	s := newResumingServer(t, "10", map[string]int{routeToken: -1})
	client, clock := s.client(4, 35*time.Second)
	err := client.Authenticate(context.Background())
	p, ok := errors.AsType[*Problem](err)
	if !ok || p.Code != CodeServerResuming || p.Status != http.StatusServiceUnavailable || p.RequestID != "r_resume" {
		t.Fatalf("error %v, want the server's server_resuming problem", err)
	}
	if !strings.Contains(p.Detail, "the server is resuming; retry after 5 s") || !strings.Contains(p.Detail, "still resuming after 35s") {
		t.Fatalf("detail %q: want the server's message and the timeout", p.Detail)
	}
	// Attempts at 0, 10, 20 and 30 s; the next would start at 40 s, after 35 s.
	if got := s.count(routeToken); got != 4 {
		t.Fatalf("%d sign-in requests, want 4", got)
	}
	if got := clock.recorded(); !slices.Equal(got, repeat(10*time.Second, 3)) {
		t.Fatalf("waits %v", got)
	}
}

// Without a Retry-After this client can read the wait is 3 s.
func TestServerResumingWithoutRetryAfterWaitsThreeSeconds(t *testing.T) {
	for _, retryAfter := range []string{"", "soon"} {
		s := newResumingServer(t, retryAfter, map[string]int{routeExecute: 2})
		client, clock := s.client(4, DefaultResumeTimeout)
		if err := execute(context.Background(), client); err != nil {
			t.Fatal(err)
		}
		if got := clock.recorded(); !slices.Equal(got, repeat(3*time.Second, 2)) {
			t.Fatalf("Retry-After %q: waits %v, want two of 3s", retryAfter, got)
		}
	}
}

// Retry-After 0 waits 1 s, never sending requests back to back.
func TestServerResumingRetryAfterZeroWaitsOneSecond(t *testing.T) {
	s := newResumingServer(t, "0", map[string]int{routeExecute: 2})
	client, clock := s.client(4, DefaultResumeTimeout)
	if err := execute(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if got := clock.recorded(); !slices.Equal(got, repeat(time.Second, 2)) {
		t.Fatalf("waits %v, want two of 1s", got)
	}
}

// A longer Retry-After is capped at 60 s.
func TestServerResumingRetryAfterIsCapped(t *testing.T) {
	s := newResumingServer(t, "900", map[string]int{routeToken: 2})
	client, clock := s.client(4, DefaultResumeTimeout)
	if err := client.Authenticate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := clock.recorded(); !slices.Equal(got, repeat(time.Minute, 2)) {
		t.Fatalf("waits %v, want two of 60s", got)
	}
}

// ResumeTimeout 0 returns server_resuming at once, and the answer doesn't
// go to the retries of other 503s either: one request, no wait.
func TestServerResumingTimeoutZeroDoesNotRepeat(t *testing.T) {
	s := newResumingServer(t, "1", map[string]int{routeToken: -1})
	client, clock := s.client(4, 0)
	err := client.Authenticate(context.Background())
	if !HasCode(err, CodeServerResuming) || !strings.Contains(err.Error(), "the server is resuming; retry after 5 s") {
		t.Fatalf("error %v, want server_resuming with the server's message", err)
	}
	if strings.Contains(err.Error(), "still resuming after") {
		t.Fatalf("error %v: no wait was configured", err)
	}
	if got := s.count(routeToken); got != 1 {
		t.Fatalf("%d sign-in requests, want 1", got)
	}
	if got := clock.recorded(); len(got) != 0 {
		t.Fatalf("waits %v, want none", got)
	}
}

// A request that can't be sent the same way again (a streamed body), and a
// best-effort one with its own MaxAttempts, return server_resuming at once.
func TestServerResumingIsNotWaitedForWhenTheRequestCannotBeRepeated(t *testing.T) {
	s := newResumingServer(t, "1", map[string]int{routeExecute: -1})
	client, clock := s.client(4, DefaultResumeTimeout)
	streamed := &Request{Method: http.MethodPost, Path: "/v1/sessions/s1/statements",
		Body: StreamBody(MediaTypeJSON, strings.NewReader(`{"sql":"SELECT 1"}`)), Anonymous: true}
	bestEffort := &Request{Method: http.MethodPost, Path: "/v1/sessions/s1/statements", Idempotent: true,
		MaxAttempts: 2, Anonymous: true}
	for name, req := range map[string]*Request{"streamed": streamed, "best effort": bestEffort} {
		before := s.count(routeExecute)
		_, err := client.Do(context.Background(), req)
		if !HasCode(err, CodeServerResuming) {
			t.Fatalf("%s: error %v, want server_resuming", name, err)
		}
		if got := s.count(routeExecute) - before; got != 1 {
			t.Fatalf("%s: sent %d times, want once", name, got)
		}
	}
	if got := clock.recorded(); len(got) != 0 {
		t.Fatalf("waits %v, want none", got)
	}
}

// A context that ends during a wait ends the request with the context's
// error, which also says the server was resuming.
func TestServerResumingWaitEndsWithTheContext(t *testing.T) {
	s := newResumingServer(t, "5", map[string]int{routeToken: -1})
	client, clock := s.client(4, DefaultResumeTimeout)
	clock.sleepErr = context.Canceled
	err := client.Authenticate(context.Background())
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "the server is resuming; retry after 5 s") {
		t.Fatalf("error %v, want the context's error with the server's message", err)
	}
}

// Other 503s keep their bounded retry: MaxAttempts sends, then the error.
func TestOtherServiceUnavailableAnswersKeepTheirBoundedRetry(t *testing.T) {
	calls := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "1")
		writeProblem(w, http.StatusServiceUnavailable, CodeServerDraining, "")
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL + "/chargewire")
	client := NewClient(Config{BaseURL: base, MaxAttempts: 3, ResumeTimeout: DefaultResumeTimeout})
	installFakeClock(client) // the bounded retry sleeps for real, but only on Retry-After
	client.sleep = func(context.Context, time.Duration) error {
		t.Fatal("a server_draining answer waited as for a resume")
		return nil
	}
	start := time.Now()
	_, err := client.Do(context.Background(), &Request{Method: http.MethodGet, Path: "/v1/x", Idempotent: true, Anonymous: true})
	if !HasCode(err, CodeServerDraining) || calls.Load() != 3 {
		t.Fatalf("%d calls: %v; want 3 then server_draining", calls.Load(), err)
	}
	if time.Since(start) < 2*time.Second {
		t.Fatal("the bounded retry did not honour Retry-After")
	}
}
