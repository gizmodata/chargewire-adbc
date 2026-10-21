// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func fakeDatabase(t *testing.T, f *fakeWarp, options map[string]string) adbc.DatabaseWithContext {
	t.Helper()
	all := map[string]string{adbc.OptionKeyURI: f.server.URL}
	for key, value := range options {
		all[key] = value
	}
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), all)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db
}

func getOption(t *testing.T, of any, key string) string {
	t.Helper()
	value, err := of.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), key)
	if err != nil {
		t.Fatalf("GetOption(%s): %v", key, err)
	}
	return value
}

// expiresIn parses OptionAccessTokenExpiresAt and returns how far ahead it is.
func expiresIn(t *testing.T, of any) time.Duration {
	t.Helper()
	text := getOption(t, of, OptionAccessTokenExpiresAt)
	at, err := time.Parse(time.RFC3339, text)
	if err != nil || !at.Equal(at.UTC()) || text[len(text)-1] != 'Z' {
		t.Fatalf("expires_at %q is not RFC 3339 UTC (%v)", text, err)
	}
	return time.Until(at)
}

// After a password sign-in the token the driver got is readable, on the
// database and the connection, with its expiry from expires_in.
func TestFakeAccessTokenIsReadable(t *testing.T) {
	f := newFakeWarp(t)
	db := fakeDatabase(t, f, map[string]string{adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"})
	if got := getOption(t, db, OptionAccessToken); got != "" {
		t.Fatalf("before signing in the token is %q, want empty", got)
	}
	if got := getOption(t, db, OptionAccessTokenExpiresAt); got != "" {
		t.Fatalf("before signing in the expiry is %q, want empty", got)
	}
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cnxn.Close(context.Background())
	for _, of := range []any{cnxn, db} {
		if got := getOption(t, of, OptionAccessToken); got != "tok1" {
			t.Fatalf("token %q, want tok1", got)
		}
		if left := expiresIn(t, of); left < 3590*time.Second || left > 3601*time.Second {
			t.Fatalf("the token expires in %v, want about an hour (expires_in 3600)", left)
		}
	}
}

// The getter returns the live token: once it is due for renewal, a new one.
func TestFakeAccessTokenAfterRenewal(t *testing.T) {
	f := newFakeWarp(t)
	f.tokenTTL = 1
	cnxn := f.connect(t, nil)
	if got := getOption(t, cnxn, OptionAccessToken); got != "tok1" {
		t.Fatalf("token %q, want tok1", got)
	}
	first := expiresIn(t, cnxn)
	time.Sleep(1100 * time.Millisecond)
	if got := getOption(t, cnxn, OptionAccessToken); got != "tok2" {
		t.Fatalf("after its lifetime the token is %q, want the renewed tok2", got)
	}
	if second := expiresIn(t, cnxn); second <= first-1100*time.Millisecond {
		t.Fatalf("the renewed token's expiry did not move (%v, then %v)", first, second)
	}
}

func unsignedJWT(claims map[string]any) string {
	encode := func(v any) string {
		data, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(data)
	}
	return encode(map[string]string{"alg": "none"}) + "." + encode(claims) + ".sig"
}

// A caller's own token reads back as it is, with its exp claim when it is a
// JWT, before and after the first connection.
func TestFakeCallersAccessToken(t *testing.T) {
	f := newFakeWarp(t)
	exp := time.Now().Add(42 * time.Minute).Truncate(time.Second)
	for _, tc := range []struct {
		token, expiresAt string
	}{
		{unsignedJWT(map[string]any{"sub": "u", "exp": exp.Unix()}), exp.UTC().Format(time.RFC3339)},
		{"opaque-token", ""},
	} {
		db := fakeDatabase(t, f, map[string]string{OptionAccessToken: tc.token})
		check := func(of any) {
			t.Helper()
			if got := getOption(t, of, OptionAccessToken); got != tc.token {
				t.Fatalf("token %q, want the caller's %q", got, tc.token)
			}
			if got := getOption(t, of, OptionAccessTokenExpiresAt); got != tc.expiresAt {
				t.Fatalf("expiry %q, want %q", got, tc.expiresAt)
			}
		}
		check(db)
		cnxn, err := db.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		check(cnxn)
		check(db)
		_ = cnxn.Close(context.Background())
	}
}

func TestFakeAccessTokenExpiryIsReadOnly(t *testing.T) {
	f := newFakeWarp(t)
	db := fakeDatabase(t, f, map[string]string{adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"})
	if err := db.(adbc.GetSetOptionsWithContext).SetOption(context.Background(), OptionAccessTokenExpiresAt, "x"); err == nil {
		t.Fatal("the database took an expiry")
	}
	cnxn, err := db.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cnxn.Close(context.Background())
	if err := cnxn.(adbc.GetSetOptionsWithContext).SetOption(context.Background(), OptionAccessTokenExpiresAt, "x"); err == nil {
		t.Fatal("the connection took an expiry")
	}
}
