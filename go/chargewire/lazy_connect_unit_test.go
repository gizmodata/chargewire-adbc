// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
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
