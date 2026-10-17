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

	mu    sync.Mutex
	rules []*faultRule
	seen  []*http.Request
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
		reverse.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.server.Close)
	return proxy
}

var errDropResponse = errors.New("fault proxy: response dropped")

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
