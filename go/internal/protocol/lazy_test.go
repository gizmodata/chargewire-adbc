// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"testing"
	"time"
)

// lazyConnect discovers f and returns a session as the driver opens one:
// lazily when discovery says the server is stopped, at once otherwise.
func lazyConnect(t *testing.T, f *fake, client *Client) *Session {
	t.Helper()
	ctx := context.Background()
	discovery, err := client.Discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if discovery.Stopped() {
		return client.NewLazySession(map[string]any{"catalog": "lake"}, true, nil)
	}
	if err := client.Authenticate(ctx); err != nil {
		t.Fatal(err)
	}
	session, err := client.OpenSession(ctx, map[string]any{"catalog": "lake"}, true)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

// A stopped server's discovery document (a resume holder's) has state
// "stopped": connecting sends that GET only, no sign-in and no session.
// Closing such a connection, or cancelling its work, sends nothing either.
func TestLazySessionSendsNothingUntilWork(t *testing.T) {
	f := newFake(t).withSessions().withCloses()
	f.state = ServerStateStopped
	session := lazyConnect(t, f, f.client(password, 1))
	if !session.Pending() || session.ID() != "" {
		t.Fatalf("pending %v, id %q: want a session that is not open yet", session.Pending(), session.ID())
	}
	if _, err := session.CancelAll(context.Background()); err != nil {
		t.Fatalf("CancelAll: %v", err)
	}
	if err := session.SetOptions(context.Background(), map[string]any{"schema": "main"}); err != nil {
		t.Fatalf("SetOptions: %v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if sent := f.sent(); !slices.Equal(sent, []string{"GET /chargewire"}) {
		t.Fatalf("requests %v, want the discovery request only", sent)
	}
}

// The first statement signs in, opens the session (with the options set
// before it, through a server_resuming wait while the server starts) and
// runs there. The connection then behaves as one opened at once.
func TestLazySessionOpensAtTheFirstStatement(t *testing.T) {
	f := newFake(t).withSessions().withCloses()
	f.state = ServerStateStopped
	f.resuming = map[string]int{"POST /chargewire/v1/auth/token": 1}
	base, _ := url.Parse(f.server.URL + "/chargewire")
	client := NewClient(Config{BaseURL: base, Credentials: password, MaxAttempts: 1, ResumeTimeout: time.Minute})
	clock := installFakeClock(client)
	session := lazyConnect(t, f, client)
	if err := session.SetOptions(context.Background(), map[string]any{"schema": "main"}); err != nil {
		t.Fatal(err)
	}
	if err := submitUpdate(t, session); err != nil {
		t.Fatalf("first statement: %v", err)
	}
	want := []string{"GET /chargewire", "POST /chargewire/v1/auth/token", "POST /chargewire/v1/auth/token",
		"POST /chargewire/v1/sessions", "POST /chargewire/v1/sessions/s1/statements"}
	if sent := f.sent(); !slices.Equal(sent, want) {
		t.Fatalf("requests\n%v\nwant\n%v", sent, want)
	}
	if waits := clock.recorded(); len(waits) != 1 {
		t.Fatalf("resume waits %v, want one", waits)
	}
	if got := f.created[0]; got["catalog"] != "lake" || got["schema"] != "main" {
		t.Fatalf("session opened with options %v, want catalog lake and schema main", got)
	}
	if session.Pending() || session.ID() != "s1" || f.statementsRun("s1") != 1 {
		t.Fatalf("pending %v, id %q, %d statements run", session.Pending(), session.ID(), f.statementsRun("s1"))
	}
	if !client.Used() {
		t.Fatal("the client does not know the server served it")
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sent := f.sent(); sent[len(sent)-1] != "DELETE /chargewire/v1/sessions/s1" {
		t.Fatalf("last request %s, want the session's close", sent[len(sent)-1])
	}
}

// A wrong password surfaces at the first statement, with the sign-in's own
// error; nothing is opened, and the next statement signs in again.
func TestLazySessionWrongPasswordFailsTheFirstStatement(t *testing.T) {
	f := newFake(t).withSessions()
	f.state = ServerStateStopped
	session := lazyConnect(t, f, f.client(Credentials{Username: "joe", Password: "wrong"}, 1))
	for range 2 {
		err := submitUpdate(t, session)
		if !HasCode(err, CodeInvalidCredentials) {
			t.Fatalf("first statement: %v, want invalid_credentials", err)
		}
	}
	if len(f.created) != 0 || f.signIns() != 2 || !session.Pending() {
		t.Fatalf("%d sessions created, %d sign-ins, pending %v", len(f.created), f.signIns(), session.Pending())
	}
}

// The positive twin: without a state (a serving server), connecting signs in
// and opens the session as before.
func TestServingServerConnectsAtOnce(t *testing.T) {
	f := newFake(t).withSessions()
	session := lazyConnect(t, f, f.client(password, 1))
	want := []string{"GET /chargewire", "POST /chargewire/v1/auth/token", "POST /chargewire/v1/sessions"}
	if sent := f.sent(); !slices.Equal(sent, want) || session.Pending() {
		t.Fatalf("requests %v (pending %v), want %v", sent, session.Pending(), want)
	}
}

// A closed lazy session is never opened.
func TestClosedLazySessionIsNotOpened(t *testing.T) {
	f := newFake(t).withSessions()
	f.state = ServerStateStopped
	session := lazyConnect(t, f, f.client(password, 1))
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := submitUpdate(t, session); !errors.Is(err, errSessionClosed) {
		t.Fatalf("statement after close: %v", err)
	}
	if sent := f.sent(); len(sent) != 1 {
		t.Fatalf("requests %v, want the discovery request only", sent)
	}
}
