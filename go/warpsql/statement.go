// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

type statementImpl struct {
	driverbase.StatementImplBase

	cnxn  *connectionImpl
	alloc memory.Allocator
	opts  execOptions

	query      string
	prepared   *preparedStatement // created on the server only when needed
	params     *boundData
	ingest     ingestTarget
	bindByName bool

	mu     sync.Mutex
	active *execution // the running statement or open result, for Cancel
	lastID string
}

// preparedStatement is a server-side prepared statement (§7).
type preparedStatement struct {
	ref             protocol.PreparedRef
	sql             string
	parameterSchema *arrow.Schema
	resultSchema    *arrow.Schema // nil when the server did not say
}

// ingestTarget is where bulk ingest writes; table is empty unless the
// statement is in ingest mode.
type ingestTarget struct {
	table, catalog, schema string
	mode                   string
	temporary              bool
}

// boundData is what Bind or BindStream supplied: parameter rows for a query,
// or rows to ingest.
type boundData struct {
	record arrow.RecordBatch  // Bind: used by every execution
	stream array.RecordReader // BindStream: read by the next execution
}

// rows returns a reader over the bound data and a function that releases it.
func (b *boundData) rows() (array.RecordReader, error) {
	if b.record != nil {
		return array.NewRecordReader(b.record.Schema(), []arrow.RecordBatch{b.record})
	}
	if b.stream == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: fmt.Sprintf("[%s] the bound stream was already read by an earlier execution; bind again", driverName)}
	}
	stream := b.stream
	b.stream = nil
	return stream, nil
}

func (b *boundData) release() {
	if b.record != nil {
		b.record.Release()
	}
	if b.stream != nil {
		b.stream.Release()
	}
}

// execution is one statement while it runs and while its result is open.
type execution struct {
	sub       *protocol.Submission
	cancel    context.CancelFunc
	cancelled atomic.Bool // the application cancelled it

	mu    sync.Mutex
	ref   protocol.StatementRef
	known bool
}

func (e *execution) setRef(ref protocol.StatementRef) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ref, e.known = ref, ref.StatementID != ""
}

func (e *execution) statementRef() (protocol.StatementRef, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ref, e.known
}

func newStatement(c *connectionImpl) *statementImpl {
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		cnxn:              c,
		alloc:             c.Alloc,
		opts:              c.defaults,
		ingest:            ingestTarget{mode: "create"},
	}
}

func (st *statementImpl) session() *protocol.Session { return st.cnxn.session }

func (st *statementImpl) zstd() bool {
	return st.opts.compression == "zstd" && st.cnxn.discovery.Has(protocol.CapabilityIPCCompressionZstd)
}

func (st *statementImpl) SetOption(ctx context.Context, key, value string) error {
	if handled, err := st.opts.set(key, value); handled {
		return err
	}
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		st.dropPrepared()
		st.query = ""
		st.ingest.table = value
	case adbc.OptionKeyIngestMode:
		switch value {
		case adbc.OptionValueIngestModeCreate:
			st.ingest.mode = "create"
		case adbc.OptionValueIngestModeAppend:
			st.ingest.mode = "append"
		case adbc.OptionValueIngestModeReplace:
			st.ingest.mode = "replace"
		case adbc.OptionValueIngestModeCreateAppend:
			st.ingest.mode = "create_append"
		default:
			return invalidOption(key, value, "an adbc.ingest.mode.* value")
		}
	case adbc.OptionValueIngestTargetCatalog:
		st.ingest.catalog = value
	case adbc.OptionValueIngestTargetDBSchema:
		st.ingest.schema = value
	case adbc.OptionValueIngestTemporary:
		b, err := parseBool(key, value)
		if err != nil {
			return err
		}
		st.ingest.temporary = b
	case optionBindByName:
		b, err := parseBool(key, value)
		if err != nil {
			return err
		}
		st.bindByName = b
	case OptionStatementID:
		return st.ErrorHelper.InvalidArgument("option %s is read-only", key)
	case adbc.OptionKeyIncremental:
		return st.ErrorHelper.NotImplemented("incremental execution is not supported")
	default:
		return st.StatementImplBase.SetOption(ctx, key, value)
	}
	return nil
}

func (st *statementImpl) GetOption(ctx context.Context, key string) (string, error) {
	if value, ok := st.opts.get(key); ok {
		return value, nil
	}
	switch key {
	case OptionStatementID:
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.lastID, nil
	case adbc.OptionKeyIngestTargetTable:
		return st.ingest.table, nil
	case adbc.OptionValueIngestTargetCatalog:
		return st.ingest.catalog, nil
	case adbc.OptionValueIngestTargetDBSchema:
		return st.ingest.schema, nil
	case optionBindByName:
		return fmt.Sprint(st.bindByName), nil
	}
	return st.StatementImplBase.GetOption(ctx, key)
}

func (st *statementImpl) SetSqlQuery(ctx context.Context, query string) error {
	if st.prepared != nil && st.prepared.sql != query {
		st.dropPrepared()
	}
	st.query = query
	st.ingest.table = ""
	return nil
}

func (st *statementImpl) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	return st.ErrorHelper.NotImplemented("Warp SQL takes SQL, not Substrait plans")
}

func (st *statementImpl) Bind(ctx context.Context, values arrow.RecordBatch) error {
	st.unbind()
	if values != nil {
		values.Retain()
		st.params = &boundData{record: values}
	}
	return nil
}

func (st *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	st.unbind()
	if stream != nil {
		stream.Retain()
		st.params = &boundData{stream: stream}
	}
	return nil
}

func (st *statementImpl) unbind() {
	if st.params != nil {
		st.params.release()
		st.params = nil
	}
}

// Prepare marks the query as prepared. The server-side prepared statement is
// created only when it is needed (to bind parameters, or to describe them),
// which spares a plain query run through a DB-API cursor, which prepares
// every query, two extra round trips.
func (st *statementImpl) Prepare(ctx context.Context) error {
	if st.query == "" {
		return st.ErrorHelper.InvalidState("Prepare needs a query (SetSqlQuery)")
	}
	return nil
}

func (st *statementImpl) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	prepared, err := st.ensurePrepared(ctx)
	if err != nil {
		return nil, err
	}
	return prepared.parameterSchema, nil
}

// ExecuteSchema returns the result schema without running the query.
func (st *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	prepared, err := st.ensurePrepared(ctx)
	if err != nil {
		return nil, err
	}
	if prepared.resultSchema == nil {
		return nil, st.ErrorHelper.NotImplemented("the server did not report the query's result schema when preparing it")
	}
	return prepared.resultSchema, nil
}

// ensurePrepared creates the server-side prepared statement for the query.
func (st *statementImpl) ensurePrepared(ctx context.Context) (*preparedStatement, error) {
	if st.query == "" {
		return nil, st.ErrorHelper.InvalidState("no query set (SetSqlQuery)")
	}
	if st.prepared != nil && st.prepared.sql == st.query && st.prepared.ref.SessionID == st.session().ID() {
		return st.prepared, nil
	}
	st.dropPrepared()
	if err := st.cnxn.requireCapability(protocol.CapabilityPreparedStatements, "binding parameters"); err != nil {
		return nil, err
	}
	doc, ref, err := st.session().Prepare(ctx, st.query)
	if err != nil {
		return nil, wrapError(err, "Prepare")
	}
	prepared := &preparedStatement{ref: ref, sql: st.query}
	if prepared.parameterSchema, err = decodeSchema(doc.ParameterSchema, st.alloc); err != nil {
		return nil, wrapError(err, "Prepare")
	}
	if doc.ResultSchema != nil {
		if prepared.resultSchema, err = decodeSchema(*doc.ResultSchema, st.alloc); err != nil {
			return nil, wrapError(err, "Prepare")
		}
	}
	st.prepared = prepared
	return prepared, nil
}

// dropPrepared deletes the server-side prepared statement, off the caller's
// path.
func (st *statementImpl) dropPrepared() {
	if st.prepared == nil {
		return
	}
	ref, session := st.prepared.ref, st.session()
	st.prepared = nil
	st.cnxn.janitor.submit("close prepared statement", func(ctx context.Context) error {
		return session.ClosePrepared(ctx, ref)
	})
}

// newSubmission builds the request that runs the query: a submit, or, with
// bound parameters, an execute of the prepared statement whose Arrow body
// holds one parameter row per execution (§7.2).
func (st *statementImpl) newSubmission(ctx context.Context, arrowResult bool) (*protocol.Submission, *upload, error) {
	if st.ingest.table != "" {
		return nil, nil, st.ErrorHelper.InvalidState("the statement is set up for bulk ingest, which runs with ExecuteUpdate")
	}
	if st.query == "" {
		return nil, nil, st.ErrorHelper.InvalidState("no query set (SetSqlQuery)")
	}
	sub := &protocol.Submission{
		Arrow: arrowResult, Wait: longPoll, Retain: st.opts.retain, Timeout: st.opts.timeout,
		QueryTag: st.opts.queryTag, Zstd: st.zstd(),
	}
	if st.params == nil {
		body, err := protocol.SubmitDocument(st.query, nil)
		if err != nil {
			return nil, nil, err
		}
		sub.Body = body
		return sub, nil, nil
	}
	prepared, err := st.ensurePrepared(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows, err := st.params.rows()
	if err != nil {
		return nil, nil, err
	}
	up, err := encodeUpload(rows, st.alloc, st.zstd())
	if err != nil {
		rows.Release()
		return nil, nil, wrapError(err, "encoding the parameters")
	}
	up.release = rows.Release
	sub.Prepared = &prepared.ref
	sub.Body = up.body
	return sub, up, nil
}

// begin registers a new execution so that Cancel can reach it.
func (st *statementImpl) begin(ctx context.Context, sub *protocol.Submission) (context.Context, *execution) {
	execCtx, cancel := context.WithCancel(ctx)
	exec := &execution{sub: sub, cancel: cancel}
	st.mu.Lock()
	st.active = exec
	st.mu.Unlock()
	return execCtx, exec
}

// end deregisters an execution.
func (st *statementImpl) end(exec *execution) {
	st.mu.Lock()
	if st.active == exec {
		st.active = nil
	}
	if ref, ok := exec.statementRef(); ok {
		st.lastID = ref.StatementID
	}
	st.mu.Unlock()
	exec.cancel()
}

// submit sends a submission. A prepared statement lost with its session (or
// dropped by the server) is prepared again and the execution retried once,
// when that is safe: outside a transaction and with the body in memory.
func (st *statementImpl) submit(ctx context.Context, sub *protocol.Submission, up *upload) (*http.Response, error) {
	resp, err := st.session().Submit(ctx, sub)
	if sub.Prepared != nil && sub.Body.Replayable() && !st.session().InTransaction() && preparedGone(err) {
		st.prepared = nil
		prepared, prepErr := st.ensurePrepared(ctx)
		if prepErr != nil {
			err = errors.Join(err, prepErr)
		} else {
			sub.Prepared = &prepared.ref
			resp, err = st.session().Submit(ctx, sub)
		}
	}
	if up != nil {
		err = up.finish(err)
	}
	return resp, err
}

// preparedGone reports an execute refused because its prepared statement no
// longer exists on the server.
func preparedGone(err error) bool {
	if lost, ok := errors.AsType[*protocol.SessionLostError](err); ok {
		return lost.NewID != ""
	}
	return protocol.HasCode(err, protocol.CodePreparedNotFound)
}

// fail ends an execution that went wrong and returns its error. A statement
// that may still run on the server is stopped.
func (st *statementImpl) fail(exec *execution, err error, op string) error {
	st.end(exec)
	st.stop(exec)
	if exec.cancelled.Load() {
		return adbc.Error{Code: adbc.StatusCancelled, Msg: fmt.Sprintf("[%s] %s: the statement was cancelled", driverName, op)}
	}
	return wrapError(err, op)
}

// stop frees the statement on the server (deleting a running statement
// cancels it), off the caller's path. A statement whose answer never arrived
// is first named by sending its request again with its Idempotency-Key, which
// does not run it twice (§6.7) — but only when the request was sent in full;
// one that never reached the server must not be started now.
func (st *statementImpl) stop(exec *execution) {
	session := st.session()
	ref, known := exec.statementRef()
	switch {
	case known:
		st.cnxn.janitor.submit("free statement", func(ctx context.Context) error {
			return session.DeleteStatement(ctx, ref)
		})
	case exec.sub != nil && exec.sub.Sent():
		sub := exec.sub
		st.cnxn.janitor.submit("stop unanswered statement", func(ctx context.Context) error {
			ref, err := session.Recall(ctx, sub)
			if err != nil {
				return err
			}
			return session.DeleteStatement(ctx, ref)
		})
	}
}

// abandonActive stops a statement still running or an unread result: a new
// execution or Close invalidates it.
func (st *statementImpl) abandonActive() {
	st.mu.Lock()
	exec := st.active
	st.active = nil
	st.mu.Unlock()
	if exec != nil {
		exec.cancelled.Store(true)
		exec.cancel()
		st.stop(exec)
	}
}

// Cancel stops the running statement on the server and stops waiting for it.
// When the statement's id is not known yet (its answer is still on the way),
// it is learned by sending the same request again with its Idempotency-Key.
func (st *statementImpl) Cancel(ctx context.Context) error {
	st.mu.Lock()
	exec := st.active
	st.mu.Unlock()
	if exec == nil {
		return st.ErrorHelper.InvalidState("no statement is running")
	}
	exec.cancelled.Store(true)
	defer exec.cancel()
	ref, known := exec.statementRef()
	if !known {
		if exec.sub == nil || !exec.sub.Sent() {
			return nil // nothing reached the server; stopping the request is enough
		}
		var err error
		if ref, err = st.session().Recall(ctx, exec.sub); err != nil {
			if errors.Is(err, protocol.ErrNotReplayable) {
				return nil
			}
			return wrapError(err, "Cancel")
		}
		exec.setRef(ref)
	}
	return wrapError(st.session().Cancel(ctx, ref), "Cancel")
}

func (st *statementImpl) Close(ctx context.Context) error {
	st.abandonActive()
	st.dropPrepared()
	st.unbind()
	return nil
}

func isArrow(resp *http.Response) bool {
	return strings.HasPrefix(resp.Header.Get("Content-Type"), protocol.MediaTypeArrowStream)
}

// ExecuteQuery runs the query and returns its rows. By default the rows
// stream in the response to the submit (one round trip); with the async
// option the statement is submitted, awaited, and its retained result read.
func (st *statementImpl) ExecuteQuery(ctx context.Context) (rdr array.RecordReader, affected int64, err error) {
	ctx, span := st.StartSpan(ctx, "Statement.ExecuteQuery")
	defer driverbase.EndSpanWithError(span, &err)
	st.abandonActive()

	if st.opts.async {
		retain := true
		if st.opts.retain != nil {
			retain = *st.opts.retain
		}
		execCtx, exec, doc, err := st.runToCompletion(ctx, &retain, "ExecuteQuery")
		if err != nil {
			return nil, -1, err
		}
		return st.queryResult(execCtx, exec, doc)
	}

	sub, up, err := st.newSubmission(ctx, true)
	if err != nil {
		return nil, -1, err
	}
	execCtx, exec := st.begin(ctx, sub)
	resp, err := st.submit(execCtx, sub, up)
	if err != nil {
		return nil, -1, st.fail(exec, err, "ExecuteQuery")
	}
	if !isArrow(resp) {
		// A statement without rows answers with its document (§6.1).
		doc, err := st.awaitDocument(execCtx, exec, sub, resp)
		if err != nil {
			return nil, -1, st.fail(exec, err, "ExecuteQuery")
		}
		return st.queryResult(execCtx, exec, doc)
	}
	exec.setRef(protocol.StatementRef{SessionID: sub.SessionID(), StatementID: resp.Header.Get("Warp-Statement-Id")})
	reader, err := newResultReader(st.readerConfig(execCtx, exec, []resultPart{{index: 0}}), resp.Body)
	if err != nil {
		return nil, -1, err
	}
	return reader, -1, nil
}

// awaitDocument reads a statement document from resp and waits for the
// statement to finish.
func (st *statementImpl) awaitDocument(ctx context.Context, exec *execution, sub *protocol.Submission, resp *http.Response) (*protocol.StatementDocument, error) {
	doc, err := protocol.DecodeStatementDocument(resp)
	if err != nil {
		return nil, err
	}
	ref := protocol.StatementRef{SessionID: sub.SessionID(), StatementID: doc.StatementID}
	exec.setRef(ref)
	return st.session().WaitDone(ctx, ref, doc, longPoll)
}

// runToCompletion submits the query asynchronously and waits until it ends
// (§6.1, §6.3).
func (st *statementImpl) runToCompletion(ctx context.Context, retain *bool, op string) (context.Context, *execution, *protocol.StatementDocument, error) {
	sub, up, err := st.newSubmission(ctx, false)
	if err != nil {
		return nil, nil, nil, err
	}
	sub.RespondAsync = true
	sub.Retain = retain
	execCtx, exec := st.begin(ctx, sub)
	resp, err := st.submit(execCtx, sub, up)
	if err != nil {
		return nil, nil, nil, st.fail(exec, err, op)
	}
	doc, err := st.awaitDocument(execCtx, exec, sub, resp)
	if err != nil {
		return nil, nil, nil, st.fail(exec, err, op)
	}
	return execCtx, exec, doc, nil
}

// queryResult turns a finished statement into ExecuteQuery's answer.
func (st *statementImpl) queryResult(ctx context.Context, exec *execution, doc *protocol.StatementDocument) (array.RecordReader, int64, error) {
	if err := st.checkSucceeded(exec, doc, "ExecuteQuery"); err != nil {
		return nil, -1, err
	}
	if doc.Result.Kind != protocol.ResultRows {
		st.end(exec)
		st.stop(exec)
		reader, err := array.NewRecordReader(arrow.NewSchema(nil, nil), nil)
		return reader, rowsAffected(doc.Result), err
	}
	parts, err := resultParts(doc.Result)
	if err != nil {
		return nil, -1, st.fail(exec, err, "ExecuteQuery")
	}
	reader, err := newResultReader(st.readerConfig(ctx, exec, parts), nil)
	if err != nil {
		return nil, -1, err
	}
	return reader, partitionRows(doc.Result), nil
}

// checkSucceeded turns a failed or cancelled statement into its error.
func (st *statementImpl) checkSucceeded(exec *execution, doc *protocol.StatementDocument, op string) error {
	switch {
	case doc.State == protocol.StateSucceeded && doc.Result != nil:
		return nil
	case doc.State == protocol.StateSucceeded:
		return st.fail(exec, fmt.Errorf("statement %s succeeded without a result", doc.StatementID), op)
	}
	return st.fail(exec, statementError(doc), op)
}

// readerConfig sets up a result reader for an execution: when the result
// ends, the execution ends, and the statement is freed on the server (which
// also stops it, if the result was abandoned).
func (st *statementImpl) readerConfig(ctx context.Context, exec *execution, parts []resultPart) resultReaderConfig {
	ref, _ := exec.statementRef()
	return resultReaderConfig{
		ctx: ctx, alloc: st.alloc, session: st.session(), ref: ref, parts: parts, zstd: st.zstd(),
		cancelled: exec.cancelled.Load,
		onEnd: func(bool) {
			st.end(exec)
			st.stop(exec)
		},
	}
}

func rowsAffected(result *protocol.Result) int64 {
	if result.RowsAffected == nil {
		return -1
	}
	return *result.RowsAffected
}

// partitionRows is the row count of a result whose partitions all know
// theirs, else -1.
func partitionRows(result *protocol.Result) int64 {
	total := int64(0)
	for _, partition := range result.Partitions {
		if partition.Rows == nil {
			return -1
		}
		total += *partition.Rows
	}
	return total
}

// ExecuteUpdate runs a statement without reading rows and returns the rows it
// affected, or bulk-ingests the bound data.
func (st *statementImpl) ExecuteUpdate(ctx context.Context) (affected int64, err error) {
	ctx, span := st.StartSpan(ctx, "Statement.ExecuteUpdate")
	defer driverbase.EndSpanWithError(span, &err)
	st.abandonActive()
	if st.ingest.table != "" {
		return st.executeIngest(ctx)
	}

	sub, up, err := st.newSubmission(ctx, false)
	if err != nil {
		return -1, err
	}
	execCtx, exec := st.begin(ctx, sub)
	resp, err := st.submit(execCtx, sub, up)
	if err != nil {
		return -1, st.fail(exec, err, "ExecuteUpdate")
	}
	doc, err := st.awaitDocument(execCtx, exec, sub, resp)
	if err != nil {
		return -1, st.fail(exec, err, "ExecuteUpdate")
	}
	if err := st.checkSucceeded(exec, doc, "ExecuteUpdate"); err != nil {
		return -1, err
	}
	st.end(exec)
	st.stop(exec)
	if doc.Result.Kind != protocol.ResultUpdate {
		return -1, nil
	}
	return rowsAffected(doc.Result), nil
}

// executeIngest uploads the bound rows into the target table (§8).
func (st *statementImpl) executeIngest(ctx context.Context) (int64, error) {
	if err := st.cnxn.requireCapability(protocol.CapabilityIngest, "bulk ingest"); err != nil {
		return -1, err
	}
	if st.params == nil {
		return -1, st.ErrorHelper.InvalidState("bulk ingest needs data: Bind or BindStream the rows")
	}
	rows, err := st.params.rows()
	if err != nil {
		return -1, err
	}
	defer rows.Release()
	up, err := encodeUpload(rows, st.alloc, st.zstd())
	if err != nil {
		return -1, wrapError(err, "encoding the rows to ingest")
	}
	n, err := st.session().Ingest(ctx, protocol.IngestRequest{
		Table: st.ingest.table, Catalog: st.ingest.catalog, Schema: st.ingest.schema,
		Mode: st.ingest.mode, Temporary: st.ingest.temporary, Body: up.body,
	})
	if err := up.finish(err); err != nil {
		return -1, wrapError(err, "bulk ingest into "+st.ingest.table)
	}
	return n, nil
}

// ExecutePartitions runs the query and returns its partitions without reading
// them; ReadPartition reads one, on any connection of the same principal,
// while the result is retained.
func (st *statementImpl) ExecutePartitions(ctx context.Context) (schema *arrow.Schema, partitions adbc.Partitions, affected int64, err error) {
	ctx, span := st.StartSpan(ctx, "Statement.ExecutePartitions")
	defer driverbase.EndSpanWithError(span, &err)
	st.abandonActive()
	retain := true
	_, exec, doc, err := st.runToCompletion(ctx, &retain, "ExecutePartitions")
	if err != nil {
		return nil, adbc.Partitions{}, -1, err
	}
	if err := st.checkSucceeded(exec, doc, "ExecutePartitions"); err != nil {
		return nil, adbc.Partitions{}, -1, err
	}
	// The result must outlive the statement: partitions are read later.
	st.end(exec)
	if doc.Result.Kind != protocol.ResultRows {
		st.stop(exec)
		return arrow.NewSchema(nil, nil), adbc.Partitions{}, rowsAffected(doc.Result), nil
	}
	schemaBytes, err := doc.Result.SchemaBytes()
	if err != nil {
		return nil, adbc.Partitions{}, -1, wrapError(err, "ExecutePartitions")
	}
	if schema, err = readSchema(schemaBytes, st.alloc); err != nil {
		return nil, adbc.Partitions{}, -1, wrapError(err, "ExecutePartitions")
	}
	ref, _ := exec.statementRef()
	for _, partition := range doc.Result.Partitions {
		descriptor, err := encodePartition(ref, partition)
		if err != nil {
			return nil, adbc.Partitions{}, -1, wrapError(err, "ExecutePartitions")
		}
		partitions.PartitionIDs = append(partitions.PartitionIDs, descriptor)
	}
	partitions.NumPartitions = uint64(len(partitions.PartitionIDs))
	return schema, partitions, partitionRows(doc.Result), nil
}
