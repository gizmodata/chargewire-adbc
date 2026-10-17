// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
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
