"""Re-apply this repository's changes to the generated cgo entry points (`make -C go regenerate`).

go/pkg/chargewire/driver.go comes from driverbase-go's template, with changes of ours.
Each must match the template's text exactly once: when the template changes, this script
stops, and the change has to be looked at again.

1. Options set before AdbcConnectionInit. The template keeps them in a map and applies them by
   ranging over it, in the order Go randomizes. The current schema is looked up in the current
   catalog, so it has to be applied after the catalog: chargewire.InitOptionOrder gives that
   order (go/chargewire/init_options.go says what went wrong without it).

2. AdbcStatementCancel. The template cancels the blocked call's context before asking the
   server to stop the statement; the server has to be asked first (the comment in the
   replacement says why).

3. Read-only is one-way before initialization too, and must be carried into Database.Open
   so the session is created read-only, before hooks or other connection options are applied.
"""

from pathlib import Path

CHANGES = [
    (
        "one-way read-only before connection initialization",
        "\t\tconn.initArgs[k] = v\n",
        """		if e := chargewire.SetInitOption(conn.initArgs, k, v); e != nil {
			return C.AdbcStatusCode(errToAdbcErr(err, e))
		}
""",
    ),
    (
        "read-only in the creation request",
        "\tc, e := cdb.db.Open(context.Background())\n",
        """	openCtx, e := chargewire.ConnectionInitContext(context.Background(), conn.initArgs)
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	c, e := cdb.db.Open(openCtx)
""",
    ),
    (
        "the options set before AdbcConnectionInit (chargewire.InitOptionOrder)",
        """		for k, v := range conn.initArgs {
			rawCode := errToAdbcErr(err, conn.cnxn.SetOption(ctx, k, v))
""",
        """		for _, k := range chargewire.InitOptionOrder(conn.initArgs) {
			if k == adbc.OptionKeyReadOnly {
				continue // already carried in the session's creation request
			}
			rawCode := errToAdbcErr(err, conn.cnxn.SetOption(ctx, k, conn.initArgs[k]))
""",
    ),
    (
        "AdbcStatementCancel (the server first, then the blocked call's context)",
        """	active := st.executionContext.CancelContext()
	canceler, ok := st.stmt.(driverbase.StatementCanceler)
	if !ok {
		if active {
			return C.ADBC_STATUS_OK
		}
		setErr(err, "AdbcStatementCancel: no active query to cancel")
		return C.ADBC_STATUS_INVALID_STATE
	}

	if e := canceler.Cancel(context.Background()); e != nil {
""",
        """	canceler, ok := st.stmt.(driverbase.StatementCanceler)
	if !ok {
		if st.executionContext.CancelContext() {
			return C.ADBC_STATUS_OK
		}
		setErr(err, "AdbcStatementCancel: no active query to cancel")
		return C.ADBC_STATUS_INVALID_STATE
	}

	// The server first, then the blocked call's context. Cancelling the context first
	// unblocks the caller (Python's SIGINT handler runs this on another thread) before the
	// server has been asked to stop anything; the caller's next statement then reaches the
	// server ahead of a session-wide cancel (used while this statement's id is still
	// unknown) and is the one cancelled.
	e := canceler.Cancel(context.Background())
	active := st.executionContext.CancelContext()
	if e != nil {
""",
    ),
]

driver = Path(__file__).resolve().parents[1] / "go/pkg/chargewire/driver.go"
source = driver.read_text()
for what, original, replacement in CHANGES:
    if source.count(original) != 1:
        raise SystemExit(f"driverbase's template changed: review {what}")
    source = source.replace(original, replacement)
driver.write_text(source)
