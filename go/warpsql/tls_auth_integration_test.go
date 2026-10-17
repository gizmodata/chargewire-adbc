// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// openErr is the error opening a connection with options gives.
func openErr(t *testing.T, options map[string]string) error {
	t.Helper()
	integrationTests.Add(1)
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), options)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	cnxn, err := db.Open(context.Background())
	if err == nil {
		cnxn.Close(context.Background())
	}
	return err
}

func TestIntegrationTLS(t *testing.T) {
	ca := newTestCA(t)
	server := startServer(t, "--tls-cert", ca.serverCertFile, "--tls-key", ca.serverKeyFile)

	t.Run("custom CA", func(t *testing.T) {
		cnxn := connect(t, openDB(t, server, map[string]string{OptionTLSRootCerts: ca.caPEM}))
		if got := scalarInt(t, cnxn, "SELECT 1"); got != 1 {
			t.Fatal(got)
		}
	})
	t.Run("https URI and server name", func(t *testing.T) {
		cnxn := connect(t, openDB(t, server, map[string]string{
			adbc.OptionKeyURI:   "https://" + strings.Replace(server.host, "127.0.0.1", "localhost", 1),
			OptionTLSRootCerts:  ca.caPEM,
			OptionTLSServerName: "localhost",
		}))
		if got := scalarInt(t, cnxn, "SELECT 2"); got != 2 {
			t.Fatal(got)
		}
	})
	t.Run("verified by default", func(t *testing.T) {
		err := openErr(t, server.options())
		assertStatus(t, err, adbc.StatusIO, "")
		if !strings.Contains(err.Error(), "certificate") {
			t.Errorf("error %v does not mention the certificate", err)
		}
	})
	t.Run("wrong server name", func(t *testing.T) {
		options := server.options()
		options[OptionTLSRootCerts] = ca.caPEM
		options[OptionTLSServerName] = "wrong.example"
		assertStatus(t, openErr(t, options), adbc.StatusIO, "")
	})
	t.Run("skip verify is explicit", func(t *testing.T) {
		cnxn := connect(t, openDB(t, server, map[string]string{OptionTLSSkipVerify: "true"}))
		if got := scalarInt(t, cnxn, "SELECT 3"); got != 3 {
			t.Fatal(got)
		}
	})
	t.Run("plain HTTP to a TLS server", func(t *testing.T) {
		options := server.options()
		options[adbc.OptionKeyURI] = "warp://" + server.host + "?tls=false"
		if err := openErr(t, options); err == nil {
			t.Fatal("plain HTTP to a TLS port connected")
		}
	})
	t.Run("TLS options on a plain-HTTP URI", func(t *testing.T) {
		options := shared.options()
		options[OptionTLSRootCerts] = ca.caPEM
		assertStatus(t, openErr(t, options), adbc.StatusInvalidArgument, "")
	})
	t.Run("bad root certificates", func(t *testing.T) {
		options := server.options()
		options[OptionTLSRootCerts] = "not a certificate"
		assertStatus(t, openErr(t, options), adbc.StatusInvalidArgument, "")
	})
}

func TestIntegrationMutualTLS(t *testing.T) {
	ca := newTestCA(t)
	server := startServer(t, "--tls-cert", ca.serverCertFile, "--tls-key", ca.serverKeyFile, "--tls-client-ca", ca.caFile)
	cnxn := connect(t, openDB(t, server, map[string]string{
		OptionTLSRootCerts: ca.caPEM, OptionTLSClientCert: ca.clientCertPEM, OptionTLSClientKey: ca.clientKeyPEM,
	}))
	if got := scalarInt(t, cnxn, "SELECT 1"); got != 1 {
		t.Fatal(got)
	}
	options := server.options()
	options[OptionTLSRootCerts] = ca.caPEM
	assertStatus(t, openErr(t, options), adbc.StatusIO, "")
	options[OptionTLSClientCert] = ca.clientCertPEM
	assertStatus(t, openErr(t, options), adbc.StatusInvalidArgument, "") // a certificate without its key
}

func TestIntegrationWrongPassword(t *testing.T) {
	for _, credentials := range [][2]string{{testUsername, shared.password + "x"}, {"nobody", shared.password}, {"", ""}} {
		options := shared.options()
		options[adbc.OptionKeyUsername], options[adbc.OptionKeyPassword] = credentials[0], credentials[1]
		assertStatus(t, openErr(t, options), adbc.StatusUnauthenticated, "invalid_credentials")
	}
}

// A token obtained elsewhere is used as is; a bad one is refused.
func TestIntegrationAccessToken(t *testing.T) {
	cnxn := connect(t, openDB(t, shared, map[string]string{
		adbc.OptionKeyUsername: "", adbc.OptionKeyPassword: "", OptionAccessToken: rawToken(t),
	}))
	if got := scalarInt(t, cnxn, "SELECT 1"); got != 1 {
		t.Fatal(got)
	}
	options := map[string]string{adbc.OptionKeyURI: shared.uri(), OptionAccessToken: "forged"}
	assertStatus(t, openErr(t, options), adbc.StatusUnauthenticated, "invalid_token")
}

// The driver does not try a grant the server does not offer (§3.1).
func TestIntegrationUnsupportedGrant(t *testing.T) {
	options := map[string]string{adbc.OptionKeyURI: shared.uri(), OptionSubjectToken: "eyJ.fake.jwt"}
	err := openErr(t, options)
	assertStatus(t, err, adbc.StatusUnauthenticated, "")
	if !strings.Contains(err.Error(), "token_exchange") {
		t.Errorf("error %v does not name the grant", err)
	}
}

func TestIntegrationUnreachableServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close() // nothing listens there now
	options := shared.options()
	options[adbc.OptionKeyURI] = "warp://" + address + "?tls=false"
	options[OptionRetryMaxAttempts] = "2"
	assertStatus(t, openErr(t, options), adbc.StatusIO, "")
}

// An HTTP server that is not a Warp SQL server is told apart.
func TestIntegrationNotAWarpServer(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"hello": "world"}`))
	}))
	defer other.Close()
	options := shared.options()
	options[adbc.OptionKeyURI] = other.URL
	err := openErr(t, options)
	if err == nil || !strings.Contains(err.Error(), "does not describe a Warp SQL server") {
		t.Fatalf("error %v", err)
	}
}

func TestIntegrationBadURIs(t *testing.T) {
	for _, uri := range []string{"", "grpc://host:1", "warp://", "warp://host?transport=tcp", "warp://user:pw@host"} {
		options := shared.options()
		options[adbc.OptionKeyURI] = uri
		assertStatus(t, openErr(t, options), adbc.StatusInvalidArgument, "")
	}
}

// certificateToken signs in with the client_certificate grant over a plain
// HTTP client presenting cert/key, the way another tool would get a token.
func certificateToken(t *testing.T, server *testServer, ca *testCA, cert, key string) string {
	t.Helper()
	pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(ca.caPEM))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair}}}}
	resp, err := client.Post("https://"+server.host+"/warp/v1/auth/token", "application/json",
		strings.NewReader(`{"grant_type":"client_certificate"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var answer struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil || answer.AccessToken == "" {
		t.Fatalf("certificate sign-in: HTTP %d, %v", resp.StatusCode, err)
	}
	return answer.AccessToken
}

// The client_certificate grant (§4.4): the certificate is the credential, and
// the token it gets is bound to it.
func TestIntegrationClientCertificate(t *testing.T) {
	ca := newTestCA(t)
	server := startServer(t, "--tls-cert", ca.serverCertFile, "--tls-key", ca.serverKeyFile, "--tls-client-ca", ca.caFile,
		"--mtls-admin-identities", testClientEmail)
	strangerCert, strangerKey := ca.issue(t, "stranger", []string{"stranger@example.com"}, false)
	certificate := func(cert, key string) map[string]string {
		return map[string]string{
			adbc.OptionKeyUsername: "", adbc.OptionKeyPassword: "", OptionAuthType: "mtls",
			OptionTLSRootCerts: ca.caPEM, OptionTLSClientCert: cert, OptionTLSClientKey: key,
		}
	}

	t.Run("signs in without a password", func(t *testing.T) {
		cnxn := connect(t, openDB(t, server, certificate(ca.clientCertPEM, ca.clientKeyPEM)))
		if got := scalarInt(t, cnxn, "SELECT 7"); got != 7 {
			t.Fatal(got)
		}
	})
	t.Run("a split result is read with the bound token", func(t *testing.T) {
		cnxn := splitDB(t, server, "4", "preserve", certificate(ca.clientCertPEM, ca.clientKeyPEM))
		table := queryTable(t, cnxn, "SELECT i FROM range(1000000) t(i) ORDER BY i DESC")
		defer table.Release()
		if got := int64Column(t, table, "i"); !slices.Equal(got, descending(1000000)) {
			t.Fatal("the split result over a certificate sign-in is wrong")
		}
	})
	t.Run("an unknown identity is refused", func(t *testing.T) {
		assertStatus(t, openErr(t, mergeOptions(server.options(), certificate(strangerCert, strangerKey))),
			adbc.StatusUnauthenticated, "unknown_certificate_identity")
	})
	t.Run("a bound token over another certificate is refused", func(t *testing.T) {
		token := certificateToken(t, server, ca, ca.clientCertPEM, ca.clientKeyPEM)
		stolen := certificate(strangerCert, strangerKey)
		stolen[OptionAuthType], stolen[OptionAccessToken] = "password", token
		assertStatus(t, openErr(t, mergeOptions(server.options(), stolen)), adbc.StatusUnauthenticated, "certificate_mismatch")
		// The twin: the same token over its own certificate works.
		own := certificate(ca.clientCertPEM, ca.clientKeyPEM)
		own[OptionAuthType], own[OptionAccessToken] = "password", token
		cnxn := connect(t, openDB(t, server, own))
		if got := scalarInt(t, cnxn, "SELECT 8"); got != 8 {
			t.Fatal(got)
		}
	})
	t.Run("a server without client CAs does not offer it", func(t *testing.T) {
		plain := startServer(t, "--tls-cert", ca.serverCertFile, "--tls-key", ca.serverKeyFile)
		err := openErr(t, mergeOptions(plain.options(), certificate(ca.clientCertPEM, ca.clientKeyPEM)))
		assertStatus(t, err, adbc.StatusUnauthenticated, "")
		if !strings.Contains(err.Error(), "mtls_identity") {
			t.Errorf("error %v does not say what the server lacks", err)
		}
		// The twin: a password sign-in over the same TLS works.
		cnxn := connect(t, openDB(t, plain, map[string]string{OptionTLSRootCerts: ca.caPEM}))
		if got := scalarInt(t, cnxn, "SELECT 9"); got != 9 {
			t.Fatal(got)
		}
	})
}
