// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// faultProxy sits between the driver and a server, records every request,
// and breaks the exchanges a rule picks, the way a real network does.
type faultProxy struct {
	server *httptest.Server

	mu      sync.Mutex
	rules   []*faultRule
	seen    []*http.Request
	answers []*http.Response // the server's answers (headers only), in order
}

type faultRule struct {
	match func(*http.Request) bool
	// cutAfter > 0 cuts the response body after that many bytes; 0 drops
	// the whole response after the server has answered.
	cutAfter int64
	times    int
}

// newFaultProxy proxies to server.
func newFaultProxy(t *testing.T, server *testServer) *faultProxy {
	t.Helper()
	target, _ := url.Parse("http://" + server.host)
	proxy := &faultProxy{}
	reverse := httputil.NewSingleHostReverseProxy(target)
	reverse.FlushInterval = -1
	reverse.ModifyResponse = func(resp *http.Response) error {
		proxy.mu.Lock()
		proxy.answers = append(proxy.answers, &http.Response{StatusCode: resp.StatusCode, Header: resp.Header.Clone(),
			Request: resp.Request.Clone(resp.Request.Context())})
		proxy.mu.Unlock()
		rule := proxy.take(resp.Request)
		switch {
		case rule == nil:
			return nil
		case rule.cutAfter == 0:
			resp.Body.Close()
			return errDropResponse
		}
		resp.Body = &cutBody{ReadCloser: resp.Body, left: rule.cutAfter}
		return nil
	}
	reverse.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// Close the connection without an answer: a lost response.
		panic(http.ErrAbortHandler)
	}
	proxy.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.mu.Lock()
		proxy.seen = append(proxy.seen, r.Clone(r.Context()))
		proxy.mu.Unlock()
		body := &lingeringBody{body: r.Body}
		r.Body = body
		reverse.ServeHTTP(w, r)
		body.drain()
	}))
	t.Cleanup(proxy.server.Close)
	return proxy
}

var errDropResponse = errors.New("fault proxy: response dropped")

// lingeringBody is a request body the proxy reads to its end even when the
// server answered without reading it all (a retried part, answered from its
// first upload): leaving the rest unread would reset the client's connection
// and lose the answer the proxy passed on, a fault no rule asked for. The
// reverse proxy's transport may still be sending it when the answer is back,
// so reads are serialized, and Close waits for drain.
type lingeringBody struct {
	mu   sync.Mutex
	body io.ReadCloser
}

func (b *lingeringBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.body.Read(p)
}

func (b *lingeringBody) Close() error { return nil }

func (b *lingeringBody) drain() {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, _ = io.Copy(io.Discard, b.body)
	_ = b.body.Close()
}

// cutBody fails after a number of bytes, which makes the reverse proxy
// abort the connection mid-body.
type cutBody struct {
	io.ReadCloser
	left int64
}

func (b *cutBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, errors.New("fault proxy: body cut")
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= int64(n)
	return n, err
}

func (p *faultProxy) add(rule *faultRule) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, rule)
}

func (p *faultProxy) take(r *http.Request) *faultRule {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, rule := range p.rules {
		if rule.times > 0 && rule.match(r) {
			rule.times--
			return rule
		}
	}
	return nil
}

// requests returns the recorded requests that match.
func (p *faultProxy) requests(match func(*http.Request) bool) []*http.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*http.Request
	for _, r := range p.seen {
		if match(r) {
			out = append(out, r)
		}
	}
	return out
}

// responses returns the recorded answers to requests that match.
func (p *faultProxy) responses(match func(*http.Request) bool) []*http.Response {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*http.Response
	for _, r := range p.answers {
		if match(r.Request) {
			out = append(out, r)
		}
	}
	return out
}

func (p *faultProxy) uri() string {
	return "warp://" + strings.TrimPrefix(p.server.URL, "http://") + "?tls=false"
}

func isPost(suffix string) func(*http.Request) bool {
	return func(r *http.Request) bool {
		return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, suffix)
	}
}

func isResultFetch(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/result")
}
