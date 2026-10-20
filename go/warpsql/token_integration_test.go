// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

func readOption(t *testing.T, of any, key string) string {
	t.Helper()
	value, err := of.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), key)
	if err != nil {
		t.Fatalf("GetOption(%s): %v", key, err)
	}
	return value
}

// The token a password sign-in got is a working bearer token, and its expiry
// is the server's token lifetime from now.
func TestIntegrationAccessTokenIsReadable(t *testing.T) {
	server := startServer(t, "--token-lifetime-seconds", "600")
	cnxn := connect(t, openDB(t, server, nil))
	token := readOption(t, cnxn, OptionAccessToken)
	if token == "" {
		t.Fatal("no access token after a password sign-in")
	}
	at, err := time.Parse(time.RFC3339, readOption(t, cnxn, OptionAccessTokenExpiresAt))
	if err != nil {
		t.Fatal(err)
	}
	if left := time.Until(at); left < 590*time.Second || left > 601*time.Second {
		t.Fatalf("the token expires in %v, want about 600 s (--token-lifetime-seconds 600)", left)
	}
	// A raw request with it as the bearer token reads the connection's session.
	session := readOption(t, cnxn, OptionSessionID)
	for _, bearer := range []string{token, token + "x"} {
		req, _ := http.NewRequest(http.MethodGet, server.scheme+"://"+server.host+"/warp/v1/sessions/"+session, nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		want := http.StatusOK
		if bearer != token {
			want = http.StatusUnauthorized // the twin: a token that isn't the server's
		}
		if resp.StatusCode != want {
			t.Fatalf("GET the session with bearer %s…: HTTP %d, want %d", bearer[:min(8, len(bearer))], resp.StatusCode, want)
		}
	}
}

// Past its renewal time the getter returns a new token, which works.
func TestIntegrationAccessTokenAfterRenewal(t *testing.T) {
	server := startServer(t, "--token-lifetime-seconds", "2")
	db := openDB(t, server, nil)
	cnxn := connect(t, db)
	first := readOption(t, cnxn, OptionAccessToken)
	time.Sleep(2500 * time.Millisecond)
	second := readOption(t, cnxn, OptionAccessToken)
	if second == "" || second == first {
		t.Fatal("the token read after its lifetime is not a new one")
	}
	if readOption(t, db, OptionAccessToken) != second {
		t.Fatal("the database and its connection report different tokens")
	}
	if got := scalarInt(t, cnxn, "SELECT 7"); got != 7 {
		t.Fatalf("SELECT 7 = %d", got)
	}
	// Handed to another database, it signs in there (and is the caller's own).
	other := connect(t, openDB(t, server, map[string]string{adbc.OptionKeyUsername: "", adbc.OptionKeyPassword: "",
		OptionAccessToken: second}))
	if got := readOption(t, other, OptionAccessToken); got != second {
		t.Fatal("a caller's own token does not read back as it is")
	}
	// Its expiry comes from the token itself when the server's tokens are JWTs.
	expiry := readOption(t, other, OptionAccessTokenExpiresAt)
	if strings.Count(second, ".") == 2 && expiry == "" {
		t.Fatal("no expiry for a caller's JWT")
	}
}
