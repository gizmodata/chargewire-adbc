// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// testClientEmail is the identity (rfc822Name SAN) of testCA's client
// certificate, for the client_certificate grant (§4.4).
const testClientEmail = "warp-test-client@example.com"

// testCA is a throwaway certificate authority with a server and a client
// certificate, written to PEM files.
type testCA struct {
	caPEM, serverCert, serverKey, clientCertPEM, clientKeyPEM string
	caFile, serverCertFile, serverKeyFile                     string

	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	serial atomic.Int64
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "warp-sql-adbc test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	ca := &testCA{caPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), cert: caCert, key: caKey}
	ca.serial.Store(1)
	ca.serverCert, ca.serverKey = ca.issue(t, "localhost", nil, true)
	ca.clientCertPEM, ca.clientKeyPEM = ca.issue(t, "warp-test-client", []string{testClientEmail}, false)
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	ca.caFile = write("ca.pem", ca.caPEM)
	ca.serverCertFile = write("server.pem", ca.serverCert)
	ca.serverKeyFile = write("server-key.pem", ca.serverKey)
	return ca
}

// issue returns a certificate (PEM) and its key signed by the CA: a server
// certificate for localhost, or a client certificate with these e-mail SANs.
func (ca *testCA) issue(t *testing.T, cn string, emails []string, server bool) (string, string) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	usage := x509.ExtKeyUsageClientAuth
	if server {
		usage = x509.ExtKeyUsageServerAuth
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(ca.serial.Add(1)), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		EmailAddresses: emails,
	}
	if server {
		template.DNSNames = []string{"localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}
