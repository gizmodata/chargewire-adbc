// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	discoveryRequest = "GET /chargewire"
	signInRequest    = "POST /chargewire/v1/auth/token"
	openRequest      = "POST /chargewire/v1/sessions"
)

// A stopped server (its stand-in's discovery says "state": "stopped"):
// connecting sends the discovery request only, no sign-in and no session.
// What a pool does with an idle connection before handing it out (reading
// its options and the driver's info) and closing it send nothing, so a
// pool's refills don't start the server.
func TestLazyConnectToAStoppedServerSendsNothing(t *testing.T) {
	f := newFakeChargewire(t)
	f.setState("stopped")
	db := openFakeDatabase(t, f, nil)
	ctx := context.Background()
	for range 3 {
		cnxn, err := db.Open(ctx)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if _, err := cnxn.(adbc.GetSetOptionsWithContext).GetOption(ctx, adbc.OptionKeyAutoCommit); err != nil {
			t.Fatalf("GetOption(autocommit): %v", err)
		}
		info, err := cnxn.GetInfo(ctx, []adbc.InfoCode{adbc.InfoVendorName, adbc.InfoDriverName})
		if err != nil {
			t.Fatalf("GetInfo: %v", err)
		}
		info.Release()
		if err := cnxn.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	// One discovery read per Open: the server may have started since.
	if sent := f.sent(); !slices.Equal(sent, []string{discoveryRequest, discoveryRequest, discoveryRequest}) {
		t.Fatalf("requests %v, want a discovery request per Open only", sent)
	}
}

// The first statement signs in (the request that starts the server: here it
// answers server_resuming once), opens the session and runs the query.
func TestLazyConnectFirstStatementSignsInAndRuns(t *testing.T) {
	f := newFakeChargewire(t)
	f.setState("stopped")
	signIn, signIns := resumingFor(1)
	f.setToken(signIn)
	cnxn := f.connect(t, nil)
	if sent := f.sent(); !slices.Equal(sent, []string{discoveryRequest}) {
		t.Fatalf("requests after connecting %v, want the discovery request only", sent)
	}
	values, err := f.query(t, cnxn)
	if err != nil {
		t.Fatalf("first statement: %v", err)
	}
	wantSequence(t, values, 1, f.batches)
	sent := f.sent()
	want := []string{discoveryRequest, signInRequest, signInRequest, openRequest, "POST /chargewire/v1/sessions/s1/statements"}
	if !slices.Equal(sent[:min(len(sent), len(want))], want) {
		t.Fatalf("requests %v, want them to begin %v", sent, want)
	}
	if signIns.Load() != 2 {
		t.Fatalf("%d sign-ins, want 2 (one answered server_resuming)", signIns.Load())
	}
}

// A wrong password surfaces at the first statement, with the code a connect
// would have given (unauthenticated); connecting succeeded.
func TestLazyConnectWrongPasswordFailsTheFirstStatement(t *testing.T) {
	f := newFakeChargewire(t)
	f.setState("stopped")
	f.setToken(func(w http.ResponseWriter, r *http.Request) bool {
		fakeProblem(w, http.StatusUnauthorized, "invalid_credentials")
		return true
	})
	cnxn := f.connect(t, nil)
	_, err := f.query(t, cnxn)
	if adbcErr, ok := errors.AsType[adbc.Error](err); !ok || adbcErr.Code != adbc.StatusUnauthenticated {
		t.Fatalf("first statement: %v, want StatusUnauthenticated", err)
	}
	for _, request := range f.sent() {
		if request == openRequest {
			t.Fatal("a session was opened with a wrong password")
		}
	}
}

// The positive twin: without a state (a serving server), connecting signs in
// and opens the session, as before.
func TestConnectToAServingServerSignsInAtOnce(t *testing.T) {
	f := newFakeChargewire(t)
	f.connect(t, nil)
	want := []string{discoveryRequest, signInRequest, openRequest}
	if sent := f.sent(); !slices.Equal(sent, want) {
		t.Fatalf("requests %v, want %v", sent, want)
	}
}

// requestsSince returns the requests the fake got after the first n.
func (f *fakeChargewire) requestsSince(n int) []string {
	return f.sent()[n:]
}

// One database whose server stops between connections, as a pool sees it:
// the first Open signs in and opens a session; after the server stopped (its
// stand-in answers discovery with "state": "stopped"), the next Open reads
// discovery again and sends nothing else, and closing it sends nothing.
func TestLazyConnectAfterTheServerStoppedReadsDiscoveryAgain(t *testing.T) {
	f := newFakeChargewire(t)
	db := openFakeDatabase(t, f, nil)
	ctx := context.Background()
	first, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.query(t, first); err != nil {
		t.Fatal(err)
	}
	_ = first.Close(ctx)
	if sent := f.sent(); !slices.Contains(sent, signInRequest) || !slices.Contains(sent, openRequest) {
		t.Fatalf("requests %v, want a sign-in and a session for the serving server", sent)
	}
	f.setState("stopped")
	before := len(f.sent())
	for range 2 {
		cnxn, err := db.Open(ctx)
		if err != nil {
			t.Fatalf("Open after the server stopped: %v", err)
		}
		if err := cnxn.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if sent := f.requestsSince(before); !slices.Equal(sent, []string{discoveryRequest, discoveryRequest}) {
		t.Fatalf("requests for two Opens and Closes after the server stopped %v, want one discovery each", sent)
	}
}

// The twin: a database whose server was stopped at its first connection
// (which therefore sent nothing) and is serving at the next one signs in and
// opens that connection's session at Open, as against any serving server.
func TestConnectAfterTheServerStartedSignsInAtOpen(t *testing.T) {
	f := newFakeChargewire(t)
	f.setState("stopped")
	db := openFakeDatabase(t, f, nil)
	ctx := context.Background()
	first, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Close(ctx)
	f.setState("")
	before := len(f.sent())
	second, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	if sent := f.requestsSince(before); !slices.Equal(sent, []string{discoveryRequest, signInRequest, openRequest}) {
		t.Fatalf("requests for an Open after the server started %v, want discovery, a sign-in and a session", sent)
	}
}

// A person stopped the server, which ended the tokens issued until then (the
// stand-in refuses them with token_revoked). A connection opened after that
// is a new one: its first statement signs in afresh with the database's
// credentials, instead of sending the database's old token, and runs.
func TestLazyConnectionAfterAStopSignsInAfresh(t *testing.T) {
	f := newFakeChargewire(t)
	db := openFakeDatabase(t, f, nil)
	ctx := context.Background()
	first, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.query(t, first); err != nil {
		t.Fatal(err)
	}
	_ = first.Close(ctx)
	if n := f.tokens.Load(); n != 1 {
		t.Fatalf("%d sign-ins while serving, want 1 (tok1)", n)
	}
	f.setState("stopped")
	f.revoke("tok1")
	second, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	values, err := f.query(t, second)
	if err != nil {
		t.Fatalf("first statement of a connection opened after the stop: %v", err)
	}
	wantSequence(t, values, 1, f.batches)
	if n := f.tokens.Load(); n != 2 {
		t.Fatalf("%d sign-ins, want 2 (a fresh tok2 for the new connection)", n)
	}
}

// The twin: a connection opened before the stop, whose token the stand-in
// refuses with token_revoked, does not sign in again on its own (§4.2): its
// next statement fails with the refusal.
func TestConnectionFromBeforeAStopDoesNotSignInAgain(t *testing.T) {
	f := newFakeChargewire(t)
	cnxn := f.connect(t, nil)
	if _, err := f.query(t, cnxn); err != nil {
		t.Fatal(err)
	}
	f.setState("stopped")
	f.revoke("tok1")
	_, err := f.query(t, cnxn)
	if adbcErr, ok := errors.AsType[adbc.Error](err); !ok || adbcErr.Code != adbc.StatusUnauthenticated ||
		!strings.Contains(err.Error(), "token_revoked") {
		t.Fatalf("statement after the stop: %v, want the token_revoked refusal", err)
	}
	if n := f.tokens.Load(); n != 1 {
		t.Fatalf("%d sign-ins, want 1: the old connection signed in again", n)
	}
}

// openWithToken opens a database that signs in with an access token issued
// elsewhere (as a browser sign-in's), and a connection with ctx.
func openWithToken(t *testing.T, f *fakeChargewire, ctx context.Context) (adbc.DatabaseWithContext, adbc.ConnectionWithContext) {
	t.Helper()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
		adbc.OptionKeyURI: f.server.URL, OptionAccessToken: "elsewhere1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	cnxn, err := db.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cnxn.Close(context.Background()) })
	return db, cnxn
}

// A token issued elsewhere can't be renewed by the driver: when a connection
// opened after a stop finds it revoked at its first request, the error says
// the token is likely from before the stop and to supply a new one.
func TestLazyConnectionWithARevokedTokenFromElsewhereSaysSupplyANewOne(t *testing.T) {
	f := newFakeChargewire(t)
	f.setState("stopped")
	f.revoke("elsewhere1")
	_, cnxn := openWithToken(t, f, context.Background())
	_, err := f.query(t, cnxn)
	if adbcErr, ok := errors.AsType[adbc.Error](err); !ok || adbcErr.Code != adbc.StatusUnauthenticated ||
		!strings.Contains(err.Error(), "token_revoked") ||
		!strings.Contains(err.Error(), "issued before the server was stopped") || !strings.Contains(err.Error(), "supply a new one") {
		t.Fatalf("first statement: %v, want the token_revoked refusal saying to supply a new token", err)
	}
}

// With WithReauthenticate (a wrapping driver's browser sign-in), that refusal
// gets the database a new token, once, and the request runs with it.
func TestLazyConnectionReauthenticatesOnceAfterAStop(t *testing.T) {
	f := newFakeChargewire(t)
	f.setState("stopped")
	f.revoke("elsewhere1")
	var db adbc.DatabaseWithContext
	var refused []string
	ctx := WithReauthenticate(context.Background(), func(ctx context.Context, token string) error {
		refused = append(refused, token)
		return db.(adbc.GetSetOptionsWithContext).SetOption(ctx, OptionAccessToken, "elsewhere2")
	})
	db, cnxn := openWithToken(t, f, ctx)
	if _, err := f.query(t, cnxn); err != nil {
		t.Fatalf("first statement after reauthenticating: %v", err)
	}
	if !slices.Equal(refused, []string{"elsewhere1"}) {
		t.Fatalf("reauthenticated for %v, want once, for elsewhere1", refused)
	}
}

// A second refusal, after reauthenticating, is the error: no loop.
func TestLazyConnectionReportsARefusalAfterReauthenticating(t *testing.T) {
	f := newFakeChargewire(t)
	f.setState("stopped")
	f.revoke("elsewhere1")
	f.revoke("elsewhere2")
	var db adbc.DatabaseWithContext
	calls := 0
	ctx := WithReauthenticate(context.Background(), func(ctx context.Context, token string) error {
		calls++
		return db.(adbc.GetSetOptionsWithContext).SetOption(ctx, OptionAccessToken, "elsewhere2")
	})
	db, cnxn := openWithToken(t, f, ctx)
	_, err := f.query(t, cnxn)
	if !strings.Contains(fmt.Sprint(err), "token_revoked") || calls != 1 {
		t.Fatalf("first statement: %v after %d reauthentications, want the second refusal after one", err, calls)
	}
}

// The twin: a connection opened while the server was serving never
// reauthenticates; its refusal after the stop is the error (§4.2).
func TestConnectionFromBeforeAStopDoesNotReauthenticate(t *testing.T) {
	f := newFakeChargewire(t)
	calls := 0
	ctx := WithReauthenticate(context.Background(), func(context.Context, string) error {
		calls++
		return nil
	})
	_, cnxn := openWithToken(t, f, ctx)
	if _, err := f.query(t, cnxn); err != nil {
		t.Fatal(err)
	}
	f.setState("stopped")
	f.revoke("elsewhere1")
	_, err := f.query(t, cnxn)
	if !strings.Contains(fmt.Sprint(err), "token_revoked") || calls != 0 {
		t.Fatalf("statement after the stop: %v after %d reauthentications, want the refusal and none", err, calls)
	}
}
