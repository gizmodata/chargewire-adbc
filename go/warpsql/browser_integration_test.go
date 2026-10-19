// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// Browser sign-in (spec §4.3) against a live server and a mock OpenID Connect
// provider that approves at once. The server's flags are a template
// (WARP_SQL_ADBC_TEST_SERVER_OAUTH_ARGS, default warp-duckdb's) with {issuer},
// {client_id} and {identity}, and {base_url}; the client secret goes in the
// variable WARP_SQL_ADBC_TEST_SERVER_OAUTH_SECRET_ENV names (default
// WARP_DUCKDB_OAUTH_CLIENT_SECRET).

const (
	oidcClientID = "warp-sql-adbc-test"
	oidcSecret   = "warp-sql-adbc-test-secret+/="
	oidcIdentity = "analyst@example.com"
)

// mockOIDC is an OpenID Connect provider: discovery, a JWKS, an authorization
// endpoint that redirects straight back with a code (or with error when deny
// is set), and a token endpoint that checks the client, the redirect URI and
// PKCE (S256) and puts the request's nonce in a signed ID token.
type mockOIDC struct {
	server *httptest.Server
	key    *rsa.PrivateKey

	mu    sync.Mutex
	email string
	deny  bool
	codes map[string]url.Values // code -> its authorization request
}

func newMockOIDC(t *testing.T) *mockOIDC {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &mockOIDC{key: key, email: oidcIdentity, codes: map[string]url.Values{}}
	idp.server = httptest.NewUnstartedServer(http.HandlerFunc(idp.serve))
	idp.server.Start()
	t.Cleanup(idp.server.Close)
	return idp
}

func (idp *mockOIDC) serve(w http.ResponseWriter, r *http.Request) {
	reply := func(status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
	issuer := idp.server.URL
	b64 := base64.RawURLEncoding.EncodeToString
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		reply(200, map[string]string{"issuer": issuer, "jwks_uri": issuer + "/jwks",
			"authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token"})
	case "/jwks":
		reply(200, map[string]any{"keys": []map[string]string{{"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
			"n": b64(idp.key.N.Bytes()), "e": b64(big.NewInt(int64(idp.key.E)).Bytes())}}})
	case "/authorize":
		q := r.URL.Query()
		if q.Get("client_id") != oidcClientID || q.Get("code_challenge_method") != "S256" {
			reply(400, map[string]string{"error": "invalid_request"})
			return
		}
		idp.mu.Lock()
		deny := idp.deny
		code := b64([]byte(time.Now().String()))
		idp.codes[code] = q
		idp.mu.Unlock()
		back := url.Values{"state": {q.Get("state")}}
		if deny {
			back.Set("error", "access_denied")
			back.Set("error_description", "the user said no")
		} else {
			back.Set("code", code)
		}
		http.Redirect(w, r, q.Get("redirect_uri")+"?"+back.Encode(), http.StatusFound)
	case "/token":
		_ = r.ParseForm()
		idp.mu.Lock()
		request, ok := idp.codes[r.Form.Get("code")]
		delete(idp.codes, r.Form.Get("code"))
		email := idp.email
		idp.mu.Unlock()
		verifier := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		switch {
		case r.Form.Get("client_id") != oidcClientID || r.Form.Get("client_secret") != oidcSecret:
			reply(401, map[string]string{"error": "invalid_client"})
		case !ok || request.Get("redirect_uri") != r.Form.Get("redirect_uri"):
			reply(400, map[string]string{"error": "invalid_grant"})
		case b64(verifier[:]) != request.Get("code_challenge"):
			reply(400, map[string]string{"error": "invalid_grant", "error_description": "PKCE"})
		default:
			reply(200, map[string]string{"access_token": "unused", "token_type": "Bearer",
				"id_token": idp.idToken(email, request.Get("nonce"))})
		}
	default:
		reply(404, map[string]string{"error": "not_found"})
	}
}

func (idp *mockOIDC) idToken(email, nonce string) string {
	b64 := func(v any) string { data, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(data) }
	now := time.Now().Unix()
	unsigned := b64(map[string]string{"alg": "RS256", "kid": "k1", "typ": "JWT"}) + "." + b64(map[string]any{
		"iss": idp.server.URL, "aud": oidcClientID, "sub": email, "email": email, "nonce": nonce,
		"iat": now, "exp": now + 300})
	digest := sha256.Sum256([]byte(unsigned))
	signature, _ := rsa.SignPKCS1v15(rand.Reader, idp.key, crypto.SHA256, digest[:])
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func startBrowserSignInServer(t *testing.T, idp *mockOIDC) *testServer {
	t.Helper()
	t.Setenv(envOr("WARP_SQL_ADBC_TEST_SERVER_OAUTH_SECRET_ENV", "WARP_DUCKDB_OAUTH_CLIENT_SECRET"), oidcSecret)
	template := envOr("WARP_SQL_ADBC_TEST_SERVER_OAUTH_ARGS",
		"--oauth-issuer {issuer} --oauth-client-id {client_id} --oauth-identities {identity} --oauth-base-url {base_url}")
	var args []string
	for _, arg := range strings.Fields(template) {
		arg = strings.NewReplacer("{issuer}", idp.server.URL, "{client_id}", oidcClientID,
			"{identity}", oidcIdentity).Replace(arg)
		args = append(args, arg)
	}
	return startServer(t, args...)
}

// aBrowser stands in for the person's browser: it opens the authorize URL
// and follows the identity provider back to the server's callback.
func aBrowser(t *testing.T) *[]int {
	t.Helper()
	statuses := &[]int{}
	saved := openBrowser
	openBrowser = func(string) BrowserOpener {
		return func(ctx context.Context, authorizeURL string) error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, authorizeURL, nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			*statuses = append(*statuses, resp.StatusCode)
			return resp.Body.Close()
		}
	}
	t.Cleanup(func() { openBrowser = saved })
	return statuses
}

func browserDB(t *testing.T, server *testServer) adbc.DatabaseWithContext {
	t.Helper()
	// No user name or password: the browser is the credential.
	return openDB(t, server, map[string]string{OptionAuthType: AuthTypeOAuthBrowser,
		OptionOAuthTimeoutSeconds: "30", adbc.OptionKeyUsername: "", adbc.OptionKeyPassword: ""})
}

func TestIntegrationBrowserSignIn(t *testing.T) {
	idp := newMockOIDC(t)
	server := startBrowserSignInServer(t, idp)
	statuses := aBrowser(t)
	cnxn := connect(t, browserDB(t, server))
	if got := scalarInt(t, cnxn, "SELECT 42"); got != 42 {
		t.Fatalf("got %d", got)
	}
	if len(*statuses) != 1 || (*statuses)[0] != http.StatusOK {
		t.Fatalf("the browser saw %v", *statuses)
	}

	// The exported API returns the server's token, which opens sessions.
	result, err := BrowserSignIn(context.Background(), map[string]string{adbc.OptionKeyURI: server.uri()},
		openBrowser(""), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.AccessToken == "" || result.ExpiresIn <= 0 {
		t.Fatalf("got %+v", result)
	}
	db := openDB(t, server, map[string]string{OptionAccessToken: result.AccessToken,
		adbc.OptionKeyUsername: "", adbc.OptionKeyPassword: ""})
	if got := scalarInt(t, connect(t, db), "SELECT 7"); got != 7 {
		t.Fatalf("got %d", got)
	}
}

func TestIntegrationBrowserSignInRefused(t *testing.T) {
	idp := newMockOIDC(t)
	server := startBrowserSignInServer(t, idp)
	aBrowser(t)
	for _, c := range []struct {
		setup func()
		code  string
	}{
		{func() { idp.mu.Lock(); idp.deny = true; idp.mu.Unlock() }, "access_denied"},
		{func() { idp.mu.Lock(); idp.deny, idp.email = false, "mallory@example.org"; idp.mu.Unlock() }, "invalid_credentials"},
	} {
		c.setup()
		_, err := browserDB(t, server).Open(context.Background())
		if adbcStatus(err) != adbc.StatusUnauthenticated || ErrorCode(err) != c.code {
			t.Fatalf("%s: got %v (code %q)", c.code, err, ErrorCode(err))
		}
	}
}

func TestIntegrationBrowserSignInNeedsTheCapability(t *testing.T) {
	aBrowser(t)
	_, err := browserDB(t, shared).Open(context.Background())
	if adbcStatus(err) != adbc.StatusUnauthenticated || !strings.Contains(err.Error(), "does not offer browser sign-in") {
		t.Fatalf("got %v", err)
	}
}
