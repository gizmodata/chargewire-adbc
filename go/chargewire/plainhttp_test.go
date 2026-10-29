// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// plainHTTPListener is a server that speaks only plain HTTP: whatever a
// connection sends, it answers 400 and closes, as an HTTP server answers a TLS
// hello. It records every byte it received.
type plainHTTPListener struct {
	listener net.Listener
	accepted atomic.Int64
	mu       sync.Mutex
	received bytes.Buffer
}

func newPlainHTTPListener(t *testing.T) *plainHTTPListener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &plainHTTPListener{listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			p.accepted.Add(1)
			go p.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return p
}

func (p *plainHTTPListener) serve(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	p.mu.Lock()
	p.received.Write(buf[:n])
	p.mu.Unlock()
	_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 11\r\nConnection: close\r\n\r\nBad Request")
	// Read on until the client hangs up, to record anything else it sends.
	for {
		n, err := conn.Read(buf)
		p.mu.Lock()
		p.received.Write(buf[:n])
		p.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (p *plainHTTPListener) bytes() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return bytes.Clone(p.received.Bytes())
}

// TLS (the default for chargewire://) pointed at a plain-HTTP server fails at once
// with what to do, sends nothing but the TLS hello (no credentials), and never
// falls back to plaintext.
func TestPlainHTTPServerGetsAHintAndNoCredentials(t *testing.T) {
	for _, tc := range []struct {
		uri    func(addr string) string
		advice string
	}{
		{func(addr string) string { return "chargewire://" + addr }, "Add ?tls=false to the URI"},
		{func(addr string) string { return "chargewire://" + addr + "?tls=true" }, "Add ?tls=false to the URI"},
		{func(addr string) string { return "https://" + addr + "/custom" }, "Use an http:// URI instead of https://"},
	} {
		server := newPlainHTTPListener(t)
		addr := server.listener.Addr().String()
		const password = "s3cret:p@ss-never-sent"
		db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
			adbc.OptionKeyURI: tc.uri(addr), adbc.OptionKeyUsername: "plain_user", adbc.OptionKeyPassword: password,
		})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		cnxn, err := db.Open(context.Background())
		if err == nil {
			_ = cnxn.Close(context.Background())
			t.Fatalf("%s: a TLS connection to a plain-HTTP server opened", tc.uri(addr))
		}
		want := "[chargewire] The server at " + addr + " speaks plain HTTP, not TLS. " + tc.advice +
			" to connect without encryption."
		var adbcErr adbc.Error
		if !errors.As(err, &adbcErr) || adbcErr.Code != adbc.StatusIO || adbcErr.Msg != want {
			t.Fatalf("%s: error %q (status %v), want %q", tc.uri(addr), err, adbcErr.Code, want)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("%s: took %s: a plain-HTTP answer is not retried", tc.uri(addr), elapsed)
		}
		_ = db.Close(context.Background())
		if got := server.accepted.Load(); got != 1 {
			t.Errorf("%s: %d connections, want 1 (no retries, no plaintext fallback)", tc.uri(addr), got)
		}
		received := server.bytes()
		if len(received) < 3 || received[0] != 0x16 || received[1] != 0x03 {
			t.Errorf("%s: the server got %q, want only a TLS hello", tc.uri(addr), received)
		}
		for _, secret := range []string{password, "plain_user", "/v1/auth/token", "HTTP/1.1"} {
			if bytes.Contains(received, []byte(secret)) {
				t.Errorf("%s: %q reached the plain-HTTP server", tc.uri(addr), secret)
			}
		}
	}
}

// The twin: the same server reached with ?tls=false gets plain HTTP (and so
// answers it), proving the listener above would have seen a request.
func TestPlainHTTPServerWithTLSFalseIsSpokenTo(t *testing.T) {
	server := newPlainHTTPListener(t)
	addr := server.listener.Addr().String()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
		adbc.OptionKeyURI: "chargewire://" + addr + "?tls=false", adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p",
		OptionRetryMaxAttempts: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	_, err = db.Open(context.Background())
	if err == nil || bytes.Contains([]byte(err.Error()), []byte("speaks plain HTTP")) {
		t.Fatalf("error %v: want the server's 400, not the TLS hint", err)
	}
	if !bytes.HasPrefix(server.bytes(), []byte("GET /chargewire HTTP/1.1")) {
		t.Fatalf("the server got %q", server.bytes())
	}
}
