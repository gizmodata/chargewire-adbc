// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

func TestParseURI(t *testing.T) {
	for raw, want := range map[string]string{
		"warp://db.example.com:31337":               "https://db.example.com:31337/warp",
		"warp://db.example.com":                     "https://db.example.com/warp",
		"warp://db.example.com:31337/":              "https://db.example.com:31337/warp",
		"warp://db.example.com:31337/custom/prefix": "https://db.example.com:31337/custom/prefix",
		"warp://127.0.0.1:8080?tls=false":           "http://127.0.0.1:8080/warp",
		"warp://127.0.0.1:8080?tls=true":            "https://127.0.0.1:8080/warp",
		"WARP://h:1":                                "https://h:1/warp",
		"https://h:1/x/":                            "https://h:1/x",
		"http://[::1]:8080":                         "http://[::1]:8080/warp",
	} {
		got, err := parseURI(raw)
		if err != nil || got.String() != want {
			t.Errorf("parseURI(%q) = %v, %v; want %s", raw, got, err, want)
		}
	}
	for _, raw := range []string{"", "grpc+tls://h:1", "warp://", "warp://h?transport=tcp", "warp://h?tls=maybe", "warp://joe:pw@h", "h:1"} {
		if got, err := parseURI(raw); err == nil {
			t.Errorf("parseURI(%q) = %v, want an error", raw, got)
		}
	}
	// Credentials in a URI are never echoed back in the error.
	if _, err := parseURI("warp://joe:secret@h"); err == nil || strings.Contains(err.Error(), "secret") {
		t.Errorf("error %v", err)
	}
}

func TestExecOptions(t *testing.T) {
	var o execOptions
	for key, value := range map[string]string{
		OptionAsync: "true", OptionRetainResult: "false", OptionStatementTimeout: "1.5",
		OptionQueryTag: `{"a":1}`, OptionIPCCompression: "ZSTD",
	} {
		if handled, err := o.set(key, value); !handled || err != nil {
			t.Fatalf("set(%s=%s): %v %v", key, value, handled, err)
		}
	}
	if !o.async || o.retain == nil || *o.retain || o.timeout != 1500*time.Millisecond || o.compression != "zstd" {
		t.Fatalf("options %+v", o)
	}
	if got, _ := o.get(OptionStatementTimeout); got != "1.5" {
		t.Errorf("timeout reads back as %q", got)
	}
	for key, value := range map[string]string{
		OptionAsync: "yes", OptionStatementTimeout: "-1", OptionQueryTag: "[1]", OptionIPCCompression: "gzip",
	} {
		_, err := o.set(key, value)
		if adbcErr, ok := errors.AsType[adbc.Error](err); !ok || adbcErr.Code != adbc.StatusInvalidArgument {
			t.Errorf("set(%s=%s): %v", key, value, err)
		}
	}
	if handled, _ := o.set("adbc.something.else", "x"); handled {
		t.Error("an unknown key was handled")
	}
}

func TestLikePatterns(t *testing.T) {
	cases := []struct {
		pattern string
		match   []string
		nomatch []string
	}{
		{"orders", []string{"orders"}, []string{"Orders", "orders2"}},
		{"ord%", []string{"ord", "orders", "ord\nx"}, []string{"xord"}},
		{"order_lines", []string{"order_lines", "orderxlines"}, []string{"order__lines"}},
		{`order\_lines`, []string{"order_lines"}, []string{"orderxlines"}},
		{`100\%`, []string{"100%"}, []string{"1000"}},
		{`a.b*c`, []string{"a.b*c"}, []string{"axbbc"}},
		{`tail\`, []string{`tail\`}, []string{"tail"}},
	}
	for _, c := range cases {
		p := compileLike(&c.pattern)
		for _, name := range c.match {
			if !p.match(name) {
				t.Errorf("%q should match %q", c.pattern, name)
			}
		}
		for _, name := range c.nomatch {
			if p.match(name) {
				t.Errorf("%q should not match %q", c.pattern, name)
			}
		}
	}
	if !compileLike(nil).match("anything") {
		t.Error("a nil pattern must match everything")
	}
	for _, name := range []string{"order_lines", `back\slash`, "100%", "plain"} {
		escaped := escapeLike(name)
		if p := compileLike(&escaped); !p.match(name) || (name != "plain" && p.match(strings.NewReplacer("_", "x", "%", "y").Replace(name)+"z")) {
			t.Errorf("escapeLike(%q) = %q does not match exactly", name, escaped)
		}
	}
}

func ipcStream(t *testing.T, batches int, withEOS bool) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	for b := range batches {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{int64(b)}, nil)
		rec := builder.NewRecordBatch()
		if err := writer.Write(rec); err != nil {
			t.Fatal(err)
		}
		rec.Release()
		builder.Release()
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	if !withEOS {
		data = data[:len(data)-len(eosMarker)]
	}
	return data
}

// readBatches reads a stream with newIPCReader and returns how many batches it
// got and the error that ended it.
func readBatches(data []byte) (int, error) {
	reader, err := newIPCReader(bytes.NewReader(data), memory.DefaultAllocator)
	if err != nil {
		return 0, err
	}
	defer reader.Release()
	n := 0
	for reader.Next() {
		n++
	}
	return n, reader.Err()
}

// A stream is complete only with its end-of-stream marker (§6.6).
func TestEndOfStreamDetection(t *testing.T) {
	if n, err := readBatches(ipcStream(t, 3, true)); n != 3 || err != nil {
		t.Errorf("complete stream: %d batches, %v", n, err)
	}
	if n, err := readBatches(ipcStream(t, 3, false)); n != 3 || !errors.Is(err, errIncompleteStream) {
		t.Errorf("stream cut between messages: %d batches, %v", n, err)
	}
	cut := ipcStream(t, 3, true)
	if _, err := readBatches(cut[:len(cut)-20]); err == nil {
		t.Error("stream cut inside a message passed")
	}
	if _, err := readBatches(nil); err == nil {
		t.Error("an empty body passed")
	}
	// The pre-0.15 end-of-stream marker is four zero bytes.
	legacy := append(ipcStream(t, 2, false), 0, 0, 0, 0)
	if n, err := readBatches(legacy); n != 2 || err != nil {
		t.Errorf("legacy end-of-stream: %d batches, %v", n, err)
	}
}

func int64Reader(t *testing.T, batches, rows int) array.RecordReader {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var records []arrow.RecordBatch
	for b := range batches {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for r := range rows {
			builder.Field(0).(*array.Int64Builder).Append(int64(b*rows + r))
		}
		records = append(records, builder.NewRecordBatch())
		builder.Release()
	}
	reader, err := array.NewRecordReader(schema, records)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		rec.Release()
	}
	return reader
}

// Small uploads stay in memory (and can be sent again); large ones stream.
func TestUploadSpillsWhenLarge(t *testing.T) {
	small := int64Reader(t, 2, 100)
	defer small.Release()
	up, err := encodeUpload(small, memory.DefaultAllocator, false)
	if err != nil {
		t.Fatal(err)
	}
	if !up.body.Replayable() {
		t.Fatal("a small upload streams")
	}
	if n, err := readBatches(up.body.Bytes()); n != 2 || err != nil || up.finish(nil) != nil {
		t.Fatalf("in-memory body: %d batches, %v", n, err)
	}

	large := int64Reader(t, 40, 100_000) // 32 MB
	defer large.Release()
	up, err = encodeUpload(large, memory.DefaultAllocator, false)
	if err != nil {
		t.Fatal(err)
	}
	if up.body.Replayable() {
		t.Fatal("a 32 MB upload was held in memory")
	}
	// The streamed body, read to the end, is one valid Arrow stream.
	var buf bytes.Buffer
	stream, _ := up.body.Open()
	if _, err := io.Copy(&buf, stream); err != nil {
		t.Fatal(err)
	}
	if err := up.finish(nil); err != nil {
		t.Fatal(err)
	}
	if n, err := readBatches(buf.Bytes()); n != 40 || err != nil {
		t.Fatalf("streamed body: %d batches, %v", n, err)
	}
}

// Every Warp SQL code maps to the ADBC status that says what to do about it;
// a code the driver does not know falls back to its HTTP status (§12).
func TestErrorStatuses(t *testing.T) {
	for code, want := range map[string]adbc.Status{
		protocol.CodeSQLError: adbc.StatusInvalidArgument, protocol.CodeInvalidCredentials: adbc.StatusUnauthenticated,
		protocol.CodePermissionDenied: adbc.StatusUnauthorized, protocol.CodeSessionNotFound: adbc.StatusNotFound,
		protocol.CodeStatementTimeout: adbc.StatusTimeout, protocol.CodeStatementCancelled: adbc.StatusCancelled,
		protocol.CodeUnsupportedArrowType: adbc.StatusNotImplemented, protocol.CodeResultExpired: adbc.StatusInvalidState,
		protocol.CodeServerDraining: adbc.StatusIO, protocol.CodeExecutionError: adbc.StatusInternal,
	} {
		if got := statusOfProblem(&protocol.Problem{Code: code}); got != want {
			t.Errorf("%s -> %v, want %v", code, got, want)
		}
	}
	for sqlstate, want := range map[string]adbc.Status{"42P01": adbc.StatusNotFound, "42P07": adbc.StatusAlreadyExists, "42601": adbc.StatusInvalidArgument} {
		if got := statusOfProblem(&protocol.Problem{Code: protocol.CodeSQLError, SQLState: sqlstate}); got != want {
			t.Errorf("sql_error %s -> %v, want %v", sqlstate, got, want)
		}
	}
	for status, want := range map[int]adbc.Status{
		http.StatusForbidden: adbc.StatusUnauthorized, http.StatusGone: adbc.StatusInvalidState,
		http.StatusTeapot: adbc.StatusInvalidArgument, http.StatusBadGateway: adbc.StatusInternal,
	} {
		if got := statusOfProblem(&protocol.Problem{Code: "a_future_code", Status: status}); got != want {
			t.Errorf("unknown code with HTTP %d -> %v, want %v", status, got, want)
		}
	}
	err := wrapError(&protocol.Problem{Code: protocol.CodeSQLError, Status: 400, SQLState: "42P01", RequestID: "r_1", Detail: "no table t"}, "ExecuteQuery")
	adbcErr, ok := errors.AsType[adbc.Error](err)
	if !ok || string(adbcErr.SqlState[:]) != "42P01" || len(adbcErr.Details) != 2 || ErrorCode(err) != "sql_error" {
		t.Fatalf("wrapped %+v", adbcErr)
	}
	if strings.Count(err.Error(), "no table t") != 1 {
		t.Errorf("message repeats itself: %s", err)
	}
	if wrapError(context.Canceled, "x").(*driverError).adbcErr.Code != adbc.StatusCancelled {
		t.Error("a cancelled context is not StatusCancelled")
	}
}

func TestDatabaseOptions(t *testing.T) {
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
		adbc.OptionKeyURI: "warp://h:1", adbc.OptionKeyUsername: "joe", adbc.OptionKeyPassword: "secret",
		OptionStatementTimeout: "2",
	})
	if err != nil {
		t.Fatal(err)
	}
	options := db.(adbc.GetSetOptionsWithContext)
	if got, _ := options.GetOption(context.Background(), adbc.OptionKeyUsername); got != "joe" {
		t.Errorf("username %q", got)
	}
	if got, err := options.GetOption(context.Background(), adbc.OptionKeyPassword); err == nil || got != "" {
		t.Errorf("the password can be read back: %q", got)
	}
	if got, _ := options.GetOption(context.Background(), OptionStatementTimeout); got != "2" {
		t.Errorf("default statement timeout %q", got)
	}
	for options, wantStatus := range map[string]map[string]string{
		"bad uri":        {adbc.OptionKeyURI: "grpc://h"},
		"bad attempts":   {OptionRetryMaxAttempts: "0"},
		"bad bool":       {OptionTLSSkipVerify: "sure"},
		"unknown option": {"adbc.warp.nonsense": "1"},
	} {
		_, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), wantStatus)
		if err == nil {
			t.Errorf("%s: accepted", options)
		}
	}
}

func TestTLSConfig(t *testing.T) {
	if _, err := (tlsOptions{clientCert: "x"}).config(); err == nil {
		t.Error("a client certificate without its key was accepted")
	}
	if _, err := (tlsOptions{rootCerts: "nonsense"}).config(); err == nil {
		t.Error("root certificates without a PEM block were accepted")
	}
	config, err := (tlsOptions{}).config()
	if err != nil || config.InsecureSkipVerify || config.MinVersion < 0x0303 {
		t.Errorf("default TLS config %+v, %v", config, err)
	}
}

func TestNewDriverWithNameReportsTheName(t *testing.T) {
	fake := newFakeWarp(t)
	for drv, want := range map[driverbase.Driver]string{
		NewDriver(memory.DefaultAllocator):                                     "ADBC Warp SQL Driver - Go",
		NewDriverWithName(memory.DefaultAllocator, "ADBC Example Driver - Go"): "ADBC Example Driver - Go",
	} {
		db, err := drv.NewDatabaseWithContext(context.Background(), map[string]string{
			adbc.OptionKeyURI: fake.server.URL, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"})
		if err != nil {
			t.Fatal(err)
		}
		cnxn, err := db.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		reader, err := cnxn.GetInfo(context.Background(), []adbc.InfoCode{adbc.InfoDriverName})
		if err != nil {
			t.Fatal(err)
		}
		var got string
		for reader.Next() {
			rec := reader.RecordBatch()
			union := rec.Column(1).(*array.DenseUnion)
			got = union.Field(union.ChildID(0)).(*array.String).Value(int(union.ValueOffset(0)))
		}
		reader.Release()
		if got != want {
			t.Errorf("driver name %q, want %q", got, want)
		}
		_ = cnxn.Close(context.Background())
		_ = db.Close(context.Background())
	}
}
