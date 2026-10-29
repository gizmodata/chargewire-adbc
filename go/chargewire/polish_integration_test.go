// Licensed under the Apache License, Version 2.0.

//go:build integration

package chargewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/goleak"
)

// tcpProxy forwards a loopback port to a server, records what clients send,
// and can freeze: from then on it forwards nothing and answers nothing, as a
// server that hangs or a network that drops packets.
type tcpProxy struct {
	listener net.Listener
	target   string
	frozen   atomic.Bool
	mu       sync.Mutex
	sent     bytes.Buffer
	conns    []net.Conn
	wg       sync.WaitGroup
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{listener: listener, target: target}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.track(client)
			if p.frozen.Load() {
				continue // accepted, never answered
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			p.track(server)
			p.wg.Add(2)
			go p.pipe(server, client, true)
			go p.pipe(client, server, false)
		}
	}()
	t.Cleanup(p.close)
	return p
}

func (p *tcpProxy) track(conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns = append(p.conns, conn)
}

// pipe copies from src to dst until either side ends or the proxy freezes.
func (p *tcpProxy) pipe(dst, src net.Conn, fromClient bool) {
	defer p.wg.Done()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.frozen.Load() {
			if fromClient {
				p.mu.Lock()
				p.sent.Write(buf[:n])
				p.mu.Unlock()
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if !p.frozen.Load() {
				_ = dst.Close()
			}
			return
		}
	}
}

func (p *tcpProxy) addr() string { return p.listener.Addr().String() }

func (p *tcpProxy) received() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return bytes.Clone(p.sent.Bytes())
}

func (p *tcpProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// --- a TLS connection to a plain-HTTP server --------------------------------

func TestIntegrationTLSToAPlainHTTPServerSaysToTurnTLSOff(t *testing.T) {
	proxy := newTCPProxy(t, shared.host)
	options := shared.options()
	options[adbc.OptionKeyURI] = "chargewire://" + proxy.addr() // TLS: the default
	integrationTests.Add(1)
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	_, err = db.Open(context.Background())
	want := "[chargewire] The server at " + proxy.addr() + " speaks plain HTTP, not TLS. Add ?tls=false to the URI to connect without encryption."
	var adbcErr adbc.Error
	if !errors.As(err, &adbcErr) || adbcErr.Code != adbc.StatusIO || adbcErr.Msg != want {
		t.Fatalf("error %v, want %q", err, want)
	}
	sent := proxy.received()
	if len(sent) == 0 || sent[0] != 0x16 {
		t.Fatalf("the server got %q, want a TLS hello only", sent)
	}
	for _, secret := range []string{shared.password, testUsername, "auth/token"} {
		if bytes.Contains(sent, []byte(secret)) {
			t.Errorf("%q reached the server", secret)
		}
	}
}

// The twin: through the same proxy with ?tls=false the driver connects, and
// the proxy sees the password (so the check above would catch one sent).
func TestIntegrationPlainHTTPThroughTheRecordingProxyTwin(t *testing.T) {
	proxy := newTCPProxy(t, shared.host)
	options := shared.options()
	options[adbc.OptionKeyURI] = "chargewire://" + proxy.addr() + "?tls=false"
	integrationTests.Add(1)
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := scalarInt(t, cnxn, "SELECT 42"); got != 42 {
		t.Fatalf("got %d", got)
	}
	_ = cnxn.Close(context.Background())
	if !bytes.Contains(proxy.received(), []byte(shared.password)) {
		t.Fatal("the recording proxy did not see the password of a plain-HTTP sign-in")
	}
}

// --- closing after the server stopped ---------------------------------------

// openWithStatements opens a connection on db with a prepared statement and
// one whose result was read.
func openWithStatements(t *testing.T, db adbc.DatabaseWithContext) (adbc.ConnectionWithContext, []adbc.StatementWithContext) {
	t.Helper()
	ctx := context.Background()
	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prepared, _ := cnxn.NewStatement(ctx)
	if err := prepared.SetSqlQuery(ctx, "SELECT 1 + 1"); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	queried, _ := cnxn.NewStatement(ctx)
	if err := queried.SetSqlQuery(ctx, "SELECT * FROM range(100000)"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := queried.ExecuteQuery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader.Next()
	reader.Release()
	return cnxn, []adbc.StatementWithContext{prepared, queried}
}

// closeEverything closes statements, connections and the database; each Close
// must succeed. It returns how long that took.
func closeEverything(t *testing.T, db adbc.DatabaseWithContext, cnxns []adbc.ConnectionWithContext, stmts []adbc.StatementWithContext) time.Duration {
	t.Helper()
	ctx := context.Background()
	start := time.Now()
	for i, stmt := range stmts {
		if err := stmt.Close(ctx); err != nil {
			t.Errorf("statement %d: Close: %v", i, err)
		}
	}
	for i, cnxn := range cnxns {
		if err := cnxn.Close(ctx); err != nil {
			t.Errorf("connection %d: Close: %v", i, err)
		}
	}
	if err := db.Close(ctx); err != nil {
		t.Errorf("database: Close: %v", err)
	}
	return time.Since(start)
}

func newCheckedDB(t *testing.T, options map[string]string) (adbc.DatabaseWithContext, *memory.CheckedAllocator) {
	t.Helper()
	integrationTests.Add(1)
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	db, err := NewDriver(alloc).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	return db, alloc
}

func TestIntegrationCloseAfterTheServerStopped(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	server := startServer(t)
	db, alloc := newCheckedDB(t, server.options())
	var cnxns []adbc.ConnectionWithContext
	var stmts []adbc.StatementWithContext
	for range 3 {
		cnxn, s := openWithStatements(t, db)
		cnxns, stmts = append(cnxns, cnxn), append(stmts, s...)
	}
	server.stop()
	if elapsed := closeEverything(t, db, cnxns, stmts); elapsed > 3*time.Second {
		t.Errorf("closing took %s against a stopped server", elapsed)
	}
	alloc.AssertSize(t, 0)
	goleak.VerifyNone(t, ignore)
}

func TestIntegrationCloseAgainstAServerThatHangs(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	proxy := newTCPProxy(t, shared.host)
	options := shared.options()
	options[adbc.OptionKeyURI] = "chargewire://" + proxy.addr() + "?tls=false"
	db, alloc := newCheckedDB(t, options)
	cnxn, stmts := openWithStatements(t, db)
	proxy.frozen.Store(true)
	elapsed := closeEverything(t, db, []adbc.ConnectionWithContext{cnxn}, stmts)
	if elapsed > closeTimeout+2*time.Second {
		t.Errorf("closing took %s, want at most about closeTimeout (%s)", elapsed, closeTimeout)
	}
	alloc.AssertSize(t, 0)
	proxy.close()
	goleak.VerifyNone(t, ignore)
}

// The twin: with the server up, closing ends the session there.
func TestIntegrationCloseEndsTheSessionTwin(t *testing.T) {
	db, alloc := newCheckedDB(t, shared.options())
	cnxn, stmts := openWithStatements(t, db)
	id, err := cnxn.(optionConn).GetOption(context.Background(), OptionSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if status := sessionStatus(t, id); status != http.StatusOK {
		t.Fatalf("GET session %s before closing: HTTP %d", id, status)
	}
	closeEverything(t, db, []adbc.ConnectionWithContext{cnxn}, stmts)
	alloc.AssertSize(t, 0)
	if status := sessionStatus(t, id); status != http.StatusNotFound {
		t.Errorf("GET session %s after closing: HTTP %d, want 404", id, status)
	}
}

// sessionStatus signs in to the shared server over plain HTTP and returns the
// HTTP status of GET on session id.
func sessionStatus(t *testing.T, id string) int {
	t.Helper()
	base := "http://" + shared.host + "/chargewire"
	body, _ := json.Marshal(map[string]string{"grant_type": "password", "username": testUsername, "password": shared.password})
	resp, err := http.Post(base+"/v1/auth/token", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&token)
	_ = resp.Body.Close()
	if err != nil || token.AccessToken == "" {
		t.Fatalf("signing in: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, base+"/v1/sessions/"+id, nil)
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}
