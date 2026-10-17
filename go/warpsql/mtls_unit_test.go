// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// fakeMTLS is a TLS server that offers the client_certificate grant (§4.4):
// it signs in the e-mail identity of the client certificate a connection
// presents, if that identity is allowed.
type fakeMTLS struct {
	server  *httptest.Server
	ca      *testCA
	allowed string
	ttl     int64

	mu     sync.Mutex
	grants []map[string]any
	peers  []string // the certificate e-mail each grant arrived with ("" for none)
}

func newFakeMTLS(t *testing.T, offer bool) *fakeMTLS {
	ca := newTestCA(t)
	f := &fakeMTLS{ca: ca, allowed: testClientEmail, ttl: 3600}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /warp", func(w http.ResponseWriter, r *http.Request) {
		capabilities, grants := []string{}, []string{"password"}
		if offer {
			capabilities, grants = []string{protocol.CapabilityMTLSIdentity}, []string{"password", protocol.GrantClientCertificate}
		}
		fakeJSON(w, http.StatusOK, map[string]any{"protocol": "warp-sql", "protocol_versions": []string{"1"},
			"server": map[string]string{"name": "fake-mtls", "version": "1"}, "capabilities": capabilities,
			"auth": map[string]any{"grant_types": grants}})
	})
	mux.HandleFunc("POST /warp/v1/auth/token", func(w http.ResponseWriter, r *http.Request) {
		var grant map[string]any
		_ = json.NewDecoder(r.Body).Decode(&grant)
		peer := ""
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 && len(r.TLS.PeerCertificates[0].EmailAddresses) == 1 {
			peer = r.TLS.PeerCertificates[0].EmailAddresses[0]
		}
		f.mu.Lock()
		f.grants = append(f.grants, grant)
		f.peers = append(f.peers, peer)
		f.mu.Unlock()
		switch {
		case peer == "":
			fakeProblem(w, http.StatusUnauthorized, protocol.CodeClientCertificateRequired)
		case peer != f.allowed:
			fakeProblem(w, http.StatusUnauthorized, protocol.CodeUnknownCertificateIdentity)
		default:
			fakeJSON(w, http.StatusOK, map[string]any{"access_token": "bound-to-" + peer, "token_type": "Bearer", "expires_in": f.ttl})
		}
	})
	mux.HandleFunc("POST /warp/v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		fakeJSON(w, http.StatusCreated, map[string]any{"session_id": "s1", "options": map[string]any{}})
	})
	mux.HandleFunc("DELETE /warp/v1/sessions/{s}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	f.server = httptest.NewUnstartedServer(mux)
	pair, err := tls.X509KeyPair([]byte(ca.serverCert), []byte(ca.serverKey))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(ca.caPEM))
	f.server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.VerifyClientCertIfGiven}
	f.server.StartTLS()
	t.Cleanup(f.server.Close)
	return f
}

// options are a certificate sign-in's: the CA, the client certificate, and
// auth type value.
func (f *fakeMTLS) options(authType string) map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: f.server.URL, OptionAuthType: authType, OptionTLSRootCerts: f.ca.caPEM,
		OptionTLSClientCert: f.ca.clientCertPEM, OptionTLSClientKey: f.ca.clientKeyPEM,
	}
}

func openWith(t *testing.T, options map[string]string) (adbc.DatabaseWithContext, adbc.ConnectionWithContext, error) {
	t.Helper()
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	cnxn, err := db.Open(context.Background())
	if err == nil {
		t.Cleanup(func() { _ = cnxn.Close(context.Background()) })
	}
	return db, cnxn, err
}

func adbcStatus(err error) adbc.Status {
	if e, ok := errors.AsType[adbc.Error](err); ok {
		return e.Code
	}
	return adbc.StatusUnknown
}

// The grant carries no secret, arrives over the client certificate, and is
// repeated when the token nears its expiry.
func TestCertificateSignIn(t *testing.T) {
	for _, authType := range []string{AuthTypeClientCertificate, "mtls", "MTLS"} {
		f := newFakeMTLS(t, true)
		f.ttl = 1
		db, _, err := openWith(t, f.options(authType))
		if err != nil {
			t.Fatalf("%s: %v", authType, err)
		}
		if got, _ := db.(adbc.GetSetOptionsWithContext).GetOption(context.Background(), OptionAuthType); got != AuthTypeClientCertificate {
			t.Errorf("auth type reads back %q", got)
		}
		time.Sleep(1100 * time.Millisecond)
		if _, err := db.Open(context.Background()); err != nil {
			t.Fatalf("second Open after the token expired: %v", err)
		}
		f.mu.Lock()
		if len(f.grants) != 2 {
			t.Errorf("%d grants sent, want 2 (sign-in and renewal)", len(f.grants))
		}
		for i, grant := range f.grants {
			if len(grant) != 1 || grant["grant_type"] != protocol.GrantClientCertificate || f.peers[i] != testClientEmail {
				t.Errorf("grant %v over certificate %q", grant, f.peers[i])
			}
		}
		f.mu.Unlock()
	}
}

// The server's refusal comes back with its code and as Unauthenticated; the
// twin with an allowed identity is TestCertificateSignIn.
func TestCertificateSignInRefusedIdentity(t *testing.T) {
	f := newFakeMTLS(t, true)
	f.allowed = "someone-else@example.com"
	_, _, err := openWith(t, f.options(AuthTypeClientCertificate))
	if adbcStatus(err) != adbc.StatusUnauthenticated || ErrorCode(err) != protocol.CodeUnknownCertificateIdentity {
		t.Fatalf("error %v (code %q)", err, ErrorCode(err))
	}
}

// A server without the capability is not asked.
func TestCertificateSignInNeedsTheCapability(t *testing.T) {
	f := newFakeMTLS(t, false)
	_, _, err := openWith(t, f.options(AuthTypeClientCertificate))
	if adbcStatus(err) != adbc.StatusUnauthenticated || !strings.Contains(err.Error(), protocol.CapabilityMTLSIdentity) {
		t.Fatalf("error %v", err)
	}
	if len(f.grants) != 0 {
		t.Fatalf("%d grants sent to a server that does not offer the grant", len(f.grants))
	}
}

// Contradicting or incomplete certificate sign-ins are refused before anything
// is sent.
func TestCertificateSignInOptionsAreChecked(t *testing.T) {
	f := newFakeMTLS(t, true)
	for name, change := range map[string]map[string]string{
		"with a username":         {adbc.OptionKeyUsername: "joe"},
		"with a password":         {adbc.OptionKeyPassword: "secret"},
		"with a subject token":    {OptionSubjectToken: "eyJ"},
		"with an access token":    {OptionAccessToken: "tok"},
		"without a certificate":   {OptionTLSClientCert: "", OptionTLSClientKey: ""},
		"over plain HTTP":         {adbc.OptionKeyURI: "warp://127.0.0.1:1?tls=false", OptionTLSRootCerts: "", OptionTLSClientCert: "", OptionTLSClientKey: ""},
		"with an unknown type":    {OptionAuthType: "kerberos"},
		"certificate without key": {OptionTLSClientKey: ""},
	} {
		options := f.options(AuthTypeClientCertificate)
		for key, value := range change {
			options[key] = value
		}
		_, _, err := openWith(t, options)
		if adbcStatus(err) != adbc.StatusInvalidArgument {
			t.Errorf("%s: error %v, want InvalidArgument", name, err)
		}
	}
	if len(f.grants) != 0 {
		t.Fatalf("%d grants sent for refused options", len(f.grants))
	}
	// The password default is unchanged: auth type password with a username.
	options := f.options(AuthTypePassword)
	options[adbc.OptionKeyUsername] = "joe"
	if _, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), options); err != nil {
		t.Fatalf("password auth type: %v", err)
	}
}
