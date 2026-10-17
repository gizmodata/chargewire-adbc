// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func insecureTLS() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true}
}

func newStmt(t *testing.T, cnxn adbc.ConnectionWithContext) *statementImpl {
	t.Helper()
	stmt, err := cnxn.NewStatement(context.Background())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	t.Cleanup(func() { _ = stmt.Close(context.Background()) })
	return stmt.(*statementImpl)
}

// execUpdate runs sql with ExecuteUpdate and returns the rows affected.
func execUpdate(t *testing.T, cnxn adbc.ConnectionWithContext, sql string) int64 {
	t.Helper()
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
	n, err := stmt.ExecuteUpdate(context.Background())
	if err != nil {
		t.Fatalf("ExecuteUpdate(%q): %v", sql, err)
	}
	return n
}

// queryTable runs sql and reads its whole result.
func queryTable(t *testing.T, cnxn adbc.ConnectionWithContext, sql string) arrow.Table {
	t.Helper()
	stmt := newStmt(t, cnxn)
	if err := stmt.SetSqlQuery(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery(%q): %v", sql, err)
	}
	return readAll(t, reader)
}

// readAll drains and releases a reader.
func readAll(t *testing.T, reader array.RecordReader) arrow.Table {
	t.Helper()
	defer reader.Release()
	var batches []arrow.RecordBatch
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		batches = append(batches, rec)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading the result: %v", err)
	}
	table := array.NewTableFromRecords(reader.Schema(), batches)
	for _, rec := range batches {
		rec.Release()
	}
	t.Cleanup(table.Release)
	return table
}

// int64s reads an integer column of any width.
func int64s(t *testing.T, table arrow.Table, column int) []int64 {
	t.Helper()
	var out []int64
	for _, chunk := range table.Column(column).Data().Chunks() {
		for i := range chunk.Len() {
			switch c := chunk.(type) {
			case *array.Int64:
				out = append(out, c.Value(i))
			case *array.Int32:
				out = append(out, int64(c.Value(i)))
			case *array.Int16:
				out = append(out, int64(c.Value(i)))
			case *array.Int8:
				out = append(out, int64(c.Value(i)))
			default:
				t.Fatalf("column %d is %s, not an integer", column, chunk.DataType())
			}
		}
	}
	return out
}

// strings reads a utf8 column.
func stringsOf(t *testing.T, table arrow.Table, column int) []string {
	t.Helper()
	var out []string
	for _, chunk := range table.Column(column).Data().Chunks() {
		c, ok := chunk.(*array.String)
		if !ok {
			t.Fatalf("column %d is %s, not utf8", column, chunk.DataType())
		}
		for i := range c.Len() {
			out = append(out, c.Value(i))
		}
	}
	return out
}

func scalarInt(t *testing.T, cnxn adbc.ConnectionWithContext, sql string) int64 {
	t.Helper()
	values := int64s(t, queryTable(t, cnxn, sql), 0)
	if len(values) != 1 {
		t.Fatalf("%q returned %d rows, want 1", sql, len(values))
	}
	return values[0]
}

func int32Batch(name string, values ...int32) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues(values, nil)
	return builder.NewRecordBatch()
}

// assertStatus checks an error's ADBC status and Warp SQL code ("" for an
// error that did not come from the server).
func assertStatus(t *testing.T, err error, status adbc.Status, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error with status %v and code %q, got none", status, code)
	}
	adbcErr, ok := errors.AsType[adbc.Error](err)
	if !ok {
		t.Fatalf("error is not an adbc.Error: %v", err)
	}
	if adbcErr.Code != status {
		t.Errorf("status = %v, want %v (%v)", adbcErr.Code, status, err)
	}
	if got := ErrorCode(err); got != code {
		t.Errorf("Warp SQL code = %q, want %q (%v)", got, code, err)
	}
}

// assertSessionUsable proves the session is free again: a statement still
// running in it would hold this one up.
func assertSessionUsable(t *testing.T, cnxn adbc.ConnectionWithContext) {
	t.Helper()
	done := make(chan int64, 1)
	go func() { done <- scalarInt(t, cnxn, "SELECT 42") }()
	select {
	case got := <-done:
		if got != 42 {
			t.Fatalf("SELECT 42 = %d", got)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the session did not answer SELECT 42 within 30s: an earlier statement is still running")
	}
}

// testCA is a throwaway certificate authority with a server and a client
// certificate, written to PEM files.
type testCA struct {
	caPEM, serverCert, serverKey, clientCertPEM, clientKeyPEM string
	caFile, serverCertFile, serverKeyFile                    string
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
	issue := func(serial int64, cn string, usage x509.ExtKeyUsage, server bool) (string, string) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		}
		if server {
			template.DNSNames = []string{"localhost"}
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, _ := x509.MarshalECPrivateKey(key)
		return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
			string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	}
	ca := &testCA{caPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))}
	ca.serverCert, ca.serverKey = issue(2, "localhost", x509.ExtKeyUsageServerAuth, true)
	ca.clientCertPEM, ca.clientKeyPEM = issue(3, testUsername, x509.ExtKeyUsageClientAuth, false)
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
