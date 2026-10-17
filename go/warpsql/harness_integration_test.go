// Licensed under the Apache License, Version 2.0.

//go:build integration

// Live-server tests. They start the server binary named by
// WARP_SQL_ADBC_TEST_SERVER_BINARY (a server with warp-duckdb's interface:
// --host, --port, --username, --tls-*, password in WARP_DUCKDB_PASSWORD) on
// a free port. Without it they fail; they never skip, and a run in which no
// integration test ran fails too.
//
//	WARP_SQL_ADBC_TEST_SERVER_BINARY=/path/to/warp-duckdb go test -tags integration ./...

package warpsql

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	serverBinaryEnv = "WARP_SQL_ADBC_TEST_SERVER_BINARY"
	testUsername    = "warp_test"
)

// integrationTests counts tests that reached a server, so that a run where
// none did fails instead of passing vacuously.
var integrationTests atomic.Int64

// shared is the server most tests use, started once by TestMain.
var shared *testServer

func TestMain(m *testing.M) {
	binary := os.Getenv(serverBinaryEnv)
	if binary == "" {
		fmt.Fprintf(os.Stderr, "FAIL: set %s to a warp-duckdb binary to run the integration tests\n", serverBinaryEnv)
		os.Exit(1)
	}
	server, err := launchServer(binary, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: starting %s: %v\n", binary, err)
		os.Exit(1)
	}
	shared = server
	code := m.Run()
	server.stop()
	if code == 0 && integrationTests.Load() == 0 {
		fmt.Fprintln(os.Stderr, "FAIL: no integration test ran")
		code = 1
	}
	os.Exit(code)
}

// testServer is a running server process.
type testServer struct {
	cmd      *exec.Cmd
	host     string // host:port
	password string
	scheme   string
	log      *syncBuffer
}

// syncBuffer collects the server's output for failure messages.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// launchServer starts the binary with a fresh password that contains ':' and
// '@' (credentials travel as JSON fields, so nothing may split them, §4.1).
func launchServer(binary string, extraArgs []string) (*testServer, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	secret := make([]byte, 9)
	_, _ = rand.Read(secret)
	server := &testServer{
		host:     fmt.Sprintf("127.0.0.1:%d", port),
		password: hex.EncodeToString(secret) + ":p@ss:word",
		scheme:   "http",
		log:      &syncBuffer{},
	}
	for _, arg := range extraArgs {
		if arg == "--tls-cert" {
			server.scheme = "https"
		}
	}
	args := append([]string{"--host", "127.0.0.1", "--port", fmt.Sprint(port), "--username", testUsername}, extraArgs...)
	server.cmd = exec.Command(binary, args...)
	server.cmd.Env = append(os.Environ(), "WARP_DUCKDB_PASSWORD="+server.password)
	server.cmd.Stdout, server.cmd.Stderr = server.log, server.log
	if err := server.cmd.Start(); err != nil {
		return nil, err
	}
	if err := server.waitUp(); err != nil {
		server.stop()
		return nil, fmt.Errorf("%w\nserver output:\n%s", err, server.log.String())
	}
	return server, nil
}

func (s *testServer) waitUp() error {
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: insecureTLS()}}
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(s.scheme + "://" + s.host + "/warp")
		if err != nil && strings.Contains(err.Error(), "certificate required") {
			return nil // a server that requires client certificates answered the handshake
		}
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("GET /warp: %s", resp.Status)
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("no Warp SQL server answered at %s within 20s: %v", s.host, lastErr)
}

func (s *testServer) stop() {
	if s.cmd.Process == nil {
		return
	}
	// The first SIGTERM drains, the second stops at once.
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = s.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	}
}

// startServer launches a server of the test's own, for configurations the
// shared one does not have (TLS, client certificates).
func startServer(t *testing.T, extraArgs ...string) *testServer {
	t.Helper()
	integrationTests.Add(1)
	server, err := launchServer(os.Getenv(serverBinaryEnv), extraArgs)
	if err != nil {
		t.Fatalf("starting a server: %v", err)
	}
	t.Cleanup(server.stop)
	return server
}

// uri is the server's warp:// URI (plain HTTP unless it serves TLS).
func (s *testServer) uri() string {
	if s.scheme == "https" {
		return "warp://" + s.host
	}
	return "warp://" + s.host + "?tls=false"
}

func (s *testServer) options() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI:      s.uri(),
		adbc.OptionKeyUsername: testUsername,
		adbc.OptionKeyPassword: s.password,
	}
}

// openDB opens a database on server with extra options.
func openDB(t *testing.T, server *testServer, extra map[string]string) adbc.DatabaseWithContext {
	t.Helper()
	integrationTests.Add(1)
	options := server.options()
	for key, value := range extra {
		options[key] = value
	}
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db
}

// openConn opens a connection to the shared server.
func openConn(t *testing.T) adbc.ConnectionWithContext {
	t.Helper()
	return connect(t, openDB(t, shared, nil))
}

func connect(t *testing.T, db adbc.DatabaseWithContext) adbc.ConnectionWithContext {
	t.Helper()
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cnxn.Close(context.Background()) })
	return cnxn
}
