# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **CI**: a change that touches only documentation (`*.md`, `docs/`, `LICENSE`, `NOTICE`) skips the build and test jobs, and one job, `CI result`, reports on every run and is the only required check. The glibc-2.28 install check retries `dnf install` with fresh metadata, so a mirror serving a broken `repomd.xml` can't fail a build.
- **Python 3.10, pyarrow 16 and adbc-driver-manager 1.8 are supported**: the Python package
  requires Python 3.10 or newer (was 3.11), `pyarrow>=16.0.0` (was 26.0.1) and
  `adbc-driver-manager>=1.8.0` (was 1.13.0). These are the floors of
  [dlt](https://github.com/dlt-hub/dlt), which supports Python 3.10 to 3.14, so the driver installs
  wherever dlt does and doesn't force its locked versions up. The wheels were always `py3-none`;
  only the declared floors kept the package off Python 3.10. Two things in the DB-API layer
  needed adbc-driver-manager 1.9.0 and 1.10.0 and now work with 1.8.0 too: creating a cursor, and
  `conn.close()` closing the cursors the connection still has open. Connection profiles
  (`connect(profile=...)`, `profile://`) are the driver manager's own feature from 1.11.0: with an
  older one the option reaches the driver, which now says so (`connection profiles are resolved
  by the ADBC driver manager, from its version 1.11.0 …`) instead of `Unknown database option
  'profile'`. Python 3.13 needs pyarrow 18, 3.14 needs pyarrow 22, and 3.15 needs pyarrow 26.0.1
  and adbc-driver-manager 1.13.0 (their first wheels for it); pip picks those by itself.
- **CI** runs the whole Python suite, unit and live, at both ends: Python 3.10 with exactly the
  lowest versions `pyproject.toml` allows (the job fails if what got installed is not the floors),
  and Python 3.15 with the latest. Both are part of the release gate.

### Fixed

- **A catalog and a schema given together when connecting name that catalog's schema**: Python
  `connect(catalog="lake", db_schema="reports")`, and any ADBC client that sets
  `adbc.connection.catalog` and `adbc.connection.db_schema` before `AdbcConnectionInit`. The
  library applied the options set before a connection opens in the order of a Go map, which is
  random, so most connects set the schema first (33 of 40 in a run against 2.2.10). The schema was
  then looked up in the server's default catalog: one that only the requested catalog has failed
  the connect (`NOT_FOUND … No catalog + schema named "memory.reports" found`), and one that the
  default catalog has too was set there and lost again when the catalog was set, so the connection
  opened in the requested catalog's `main` without an error. Every release since 1.0.0 did this;
  either option alone was not affected. The options are now applied in a fixed order: the catalog,
  the schema, then the others by name (`chargewire.InitOptionOrder`, for drivers built on this one
  to call from their own generated entry points). `make -C go regenerate` keeps the change
  (`scripts/patch_generated_exports.py`), and a unit test fails when the generated file loses it.
  The server was never at fault: it applies one options document that carries both in the right
  order, whichever key comes first.
- **`db_schema="lake.reports"` says what to do instead**. The current schema is one schema name
  in the current catalog and is never split at a dot, because a dot can be part of a schema's name
  (`CREATE SCHEMA "sales.emea"`), so this names a schema called `lake.reports`. When there is none
  but `lake` is a catalog, the `NOT_FOUND` error now adds: set the current catalog to `lake` and
  the current schema to `reports`.
- **A session that replaces a lost one opens where the lost one was** after the current schema
  and then the current catalog were set on a connection (`conn.adbc_current_db_schema = "raw"`,
  then `conn.adbc_current_catalog = "lake"`). Setting the catalog moves the session to that
  catalog's default schema, as SQL `USE` does, but the driver still remembered the schema and
  replayed both: the replacement opened in `lake.raw` when `lake` has a schema of that name, so
  later statements ran in another schema than before the loss, without an error; when it has
  none, every request from then on failed (`No catalog + schema named "lake.raw" found`) and the
  connection was unusable. The driver now forgets the schema when the catalog is set without one,
  on an open session and on one that isn't open yet (a stopped server's lazy connection), so both
  keep the order of what was set on them.

## [2.2.10] - 2026-11-09

### Changed

- **README**: the PyPI page shows the ADBC conformance badge: the adbc-drivers.org validation suite runs in CI, and every release waits for it.

No code changes.

## [2.2.9] - 2026-11-09

Brings in the fixes hardcharger-adbc (the Hardcharger driver built on this one) made in its 2.4.6
and 2.4.7, so every Chargewire driver gets them. The driver now passes the ADBC validation
(conformance) suite.

### Fixed

- **The shared library exports only the ADBC entrypoints**, as the
  [ADBC shared-library requirements](https://adbc-drivers.org/building-drivers/shared-library-requirements.html)
  ask: `AdbcDriverInit`, `AdbcDriverChargewireInit` and the other `Adbc*` functions, 57 in all.
  2.0.0 through 2.2.8 also exported every Go `//export` function and the cgo glue
  (`ChargewireDatabaseNew`, `ChargewireArrayStreamGetNext`, `_cgo_*`, `crosscall2`, ...): 71 to
  255 extra names depending on the platform, which could clash with another library's in the
  same process. The export lists are `go/pkg/chargewire/exports_linux.map`, `exports_darwin.txt`
  and `exports_windows.def`, passed by `scripts/build_lib.sh` and `make -C go lib`. Nothing that
  loads the driver changes: the entrypoints the driver manager and the manifest use
  (`AdbcDriverChargewireInit`, `AdbcDriverInit`) are still there.
- **The macOS wheels said they run on macOS 11**: they are tagged `macosx_13_0` and the library's
  minimum macOS is 13.0, because the Go 1.27 runtime it is built with requires macOS 13 Ventura.
  2.0.0 through 2.2.8 were tagged `macosx_11_0`, so pip would install them on macOS 11 and 12,
  where the Go runtime isn't supported. The release now checks that the library's minimum macOS
  matches the wheel's tag, and `scripts/build_lib.sh` passes the target in the cgo flags, so Go's
  build cache (which ignores `MACOSX_DEPLOYMENT_TARGET`) can't reuse a build for another target.
- **Setting an option the driver doesn't know on a database that has opened a connection** is
  `NOT_IMPLEMENTED`, as before the first connection; it was `INVALID_STATE` ("must be set before
  the first connection is opened"), which said the option exists. A known option is still
  `INVALID_STATE` then.
- **Making a catalog or schema that doesn't exist current** (`adbc.connection.catalog`,
  `adbc.connection.db_schema`) is `NOT_FOUND`, the ADBC status for it; it was `INVALID_ARGUMENT`.
  The driver asks the metadata endpoints whether the name exists (exactly, not as a pattern), only
  after the server refused it.
- **Appending (or `create_append`) data with a column the existing table doesn't have** is
  `ALREADY_EXISTS`, ADBC's status for a table that exists with another schema; it was
  `INVALID_ARGUMENT`. The driver compares the data's columns with the table's (case insensitively,
  as DuckDB binds them), only after the server refused the append, for an ingest in one stream and
  one in parts alike. A value that doesn't convert is still the data's error (`INVALID_ARGUMENT`).
- **The library's `GetInfo` driver version** in builds from a checkout (`make -C go lib`,
  `scripts/build_lib.sh`) could say `go/vX.Y.Z`, the Go module's tag on the same commit, or, in a
  clone without tags (CI), a bare commit hash, neither a version; it now comes from the release
  tag (`vX.Y.Z-N-gHASH`), or is `v0.0.0-HASH` without one. Released libraries were not affected.

### Added

- **`go/internal/checkexports`**: checks a built library (ELF, Mach-O or PE) for `AdbcDriverInit`,
  the entrypoint its file name implies, and no export without the `Adbc` prefix. CI and the
  release run it on every library they build, on every platform, Windows arm64 included.
- **`validation/`**: the [ADBC validation suite](https://github.com/adbc-drivers/validation)
  (pinned, a test dependency only) configured for this driver against a Hardcharger server, with
  DuckDB's versions of its type cases, and a CI job that runs it on every push and before every
  release (it fails, never skips, without its server). Each skip and expected failure says why in
  `validation/README.md`.

### Changed

- **Dependencies**: apache arrow-adbc Go 1.13.0, driverbase-go of 2026-11-09 (the generated cgo
  exports are unchanged), grpc 1.85.0 and the golang.org/x modules; Go toolchain 1.27.3 (the
  module still needs Go 1.27.2). Python: `adbc-driver-manager>=1.13.0`, `pyarrow>=26.0.1`; test
  and dev extras `pytest>=9.2.0`, `ruff>=0.17.4`.

## [2.2.8] - 2026-11-07

### Fixed

- **A browser sign-in database's connections opened after a Stop could never start the server.**
  With `adbc.chargewire.auth.type` `oauth_browser`, the database kept its browser sign-in's token,
  which a person's Stop revokes (`invalid_token`, reason `token_revoked`), and 2.2.6's fresh
  sign-in needs a password, so every connection opened after the Stop failed at its first
  request. Such a connection now runs the database's browser sign-in again, once (usually silent
  with single sign-on), and the request runs with the new token, which starts the server; a
  second refusal is the error. Connections that find the token revoked together share one
  sign-in. Connections opened before the Stop never run it. (A `WithReauthenticate` given to
  `Open` takes its place.)

## [2.2.7] - 2026-11-07

### Added

- **`chargewire.WithReauthenticate` (Go): a new token for a connection opened after a stop.** A
  database that signs in with a token issued elsewhere (`adbc.chargewire.auth.access_token`, a
  wrapping driver's browser sign-in) can't sign in afresh as 2.2.6 does with a password. When a
  connection opened while the server was stopped finds that token revoked at its first request
  (`invalid_token`, reason `token_revoked`), the function given to `Open` with
  `WithReauthenticate` is called once to give the database a new token (setting
  `adbc.chargewire.auth.access_token` again); the request is then sent again, and a second refusal
  is the error. Connections opened before the stop never call it.

### Changed

- **That refusal says what to do.** Without `WithReauthenticate`, the `token_revoked` refusal at a
  new connection's first request now adds that the token was likely issued before the server was
  stopped, and to supply a new one.

## [2.2.6] - 2026-11-07

### Fixed

- **A connection opened after a person stopped the server could never start it again.** Stopping
  a server ends the tokens issued until then, and its stand-in refuses them with
  `invalid_token` and reason `token_revoked`. A database that had signed in while the server was
  serving kept that token, so a connection opened lazily after the stop sent it at its first
  request, got `token_revoked`, and (rightly, spec §4.2) didn't sign in again: every new
  connection of that database failed until the database was made again. A lazily opened
  connection's first request that does work now signs in afresh with the database's credentials
  unless the database already signed in after this connection opened, so connecting again after a
  stop starts the server, as it should. Connections opened before the stop, and connections opened
  at once (to a serving server), still never sign in again after `token_revoked`. A token issued
  elsewhere (`auth.access_token`) is sent as it is.

## [2.2.5] - 2026-11-07

### Fixed

- **A pool that kept one database still started a stopped server at its refills.** 2.2.4
  decided whether to connect lazily from the database's first discovery, so a Go app that keeps
  one database and pools its connections (`database/sql` style) went on signing in and opening
  sessions at every refill after the server stopped, starting it again. Every Open after the
  database's first connection now reads the discovery document again (one small `GET`, which
  never wakes a server) and connects lazily when it says `"state": "stopped"`, or at once when
  the server is serving again. A Python `connect()` makes its own database each time, so it was
  not affected.

## [2.2.4] - 2026-11-07

### Added

- **Connecting to a stopped server is lazy, so a pool's refills don't start it.** A connection
  pool opens connections on a timer, and every new connection signed in and opened a session.
  Against a server that a stand-in answers for while it is stopped and starts when someone signs
  in (a GizmoData Cloud cluster with auto-resume), a pooled app started an idle-stopped server
  every time the pool refilled, and right after a person stopped it. When the discovery document
  says `"state": "stopped"`, opening a connection (`AdbcConnectionInit`, Python's `connect()`)
  now sends nothing more. The connection signs in and opens its session at its first request
  that does work: a statement (executing or preparing), metadata, an ingest, a commit or
  rollback, or reading the session (its id, transaction, current catalog or schema). Before
  that, reading the connection's own options and the driver's info, closing it and cancelling
  send nothing. Session options set before it (current catalog or schema, autocommit) go with the
  request that opens the session. A wrong password or a refused option fails that first request
  instead of the connect, with the same error (`StatusUnauthenticated` for the password). Once a
  request has run, the database's next connections open as before. Without `state` (a serving
  server) nothing changes. Browser sign-in and attaching to a session still happen at connect.

## [2.2.3] - 2026-11-06

### Fixed

- **Closing a connection late in its token's life signed in again, which woke a stopped server.**
  The driver renews its access token with the saved credentials shortly before it expires, on
  whatever request comes next; a close or cancel was such a request. Against a server that
  resumes when someone signs in (a stopped GizmoData Cloud cluster with auto-resume), closing a
  connection two minutes before the token expired started the server for nobody, or undid a
  person's Stop. Closes and cancels (ending a session, statement, prepared statement or ingest;
  cancelling a statement or a session's work, and the request that learns a statement's id to
  cancel it) now never sign in, before sending or after an `invalid_token` refusal: they carry the
  token the driver has. A close the server refuses for its token, or whose session ended
  (`session_ended`), is done, like one for a session the server no longer has: the session is
  gone or idles out. A cancel reports the refusal. Requests that do work renew the token as
  before.
- **Aborting a partitioned ingest of a lost session opened a replacement session**, and after
  `session_ended` with `token_expired` signed in first. It now counts as done
  (`session_not_found`), opening nothing; the next request that does work replaces the session.
- **Closes and cancels waited for a resuming server** (`server_resuming`) for up to the resume
  timeout (10 minutes by default): freeing a statement, closing a prepared statement, aborting an
  ingest or cancelling held on. They now return `server_resuming` at once, as closing a session
  already did: a resuming server holds nothing they would end.

## [2.2.2] - 2026-11-05

### Fixed

- **A request ran again without the lost session's `USE` and settings.** When the server lost a
  session (an idle eviction, a restart) and no transaction was open, the driver ran the request
  again on the replacement session, which has none of what SQL had set in the old one: after
  `USE other_schema`, an `INSERT INTO t` landed in the connection's own schema's `t`, and after
  `SET …` a statement ran with other settings, without any error. After a statement whose first
  word is `USE`, `SET`, `RESET`, `PRAGMA` or `EXECUTE` (`UPDATE … SET` doesn't count; every
  statement of a multi-statement text counts), no request runs again on a replacement: it fails
  (`StatusIO`, `session_not_found`) with a message that the session's settings and `USE` are gone
  and the request did not run again, and the next request runs in the replacement. A prepared
  statement is not prepared and run again either. Statements a new-session hook runs don't
  count.

### Added

- `chargewire.WithSessionStateRestored(ctx)` (Go): execute a statement that sets session state
  in SQL with this context when the connection's new-session hook sets the same state up in every
  new session; the statement then doesn't keep a request from running again on a replacement.

## [2.2.1] - 2026-11-05

### Fixed

- **A statement could run again, in autocommit, after its transaction was lost** when the
  session document was read too early. A Chargewire server up to 2.2.0 (every Hardcharger
  through 2.7.0) answers a statement without rows on the streamed path as soon as its outcome is
  known and records the session's transaction in its document slightly after, so a read right
  after `BEGIN` could say `"none"`. 2.1.0 and 2.2.0 believed it: after `BEGIN; INSERT 1;` and a
  lost session (a server restart), `INSERT 2` ran again on the replacement and committed on its
  own (seen on a busy CI runner). The driver now assumes a transaction is open after any statement
  with a word that can begin one or change how one begins (`BEGIN`, `START`, `TRANSACTION`,
  `AUTOCOMMIT`, `EXECUTE`), also in a text that ends with `COMMIT`, and doesn't read the
  document then. Only a later statement with a word that can end a transaction (`COMMIT`,
  `ROLLBACK`, `END`, `ABORT`) and none that can begin one, and a read after it that says
  `"none"`, clear that; a stale `"open"` there only means a request is refused rather than run
  again. The same holds for statements a new-session hook runs. This holds against servers
  that keep the race (Chargewire 2.2.1 fixes it on the server).

## [2.2.0] - 2026-11-05

For Chargewire 2.2.0 (`server_resuming`, spec §11.2: GizmoData Cloud's auto-resume) and Hardcharger 2.7.0.

### Added

- **A new-session hook** (Go): `chargewire.WithNewSessionHook(ctx, hook)` on `Database.Open`'s
  context runs `hook` on every session the driver creates for that connection, the first inside
  `Open` and every replacement of a lost one (an idle eviction, `token_expired`,
  `idle_in_transaction`), before any request of the application runs on it, even one then refused
  because a transaction was lost. The hook gets a `NewSession` (`ID`, `Replaces`, `Exec`), whose
  `Exec` runs SQL in that session only. It is for state the session options can't carry: a
  replacement had the options but not what SQL had set (a driver built on this one lost its
  settings and tags). When the hook fails, the new session is ended instead of used without its
  setup: `Open` fails, or the request that found the session lost fails (`StatusIO`, the message
  saying the setup failed and why) and the next request opens and sets up another.
- Waiting for a resuming server. A stopped server that is starting again because a client
  connected answers `server_resuming` (HTTP 503 with `Retry-After`), and may hold a request,
  the sign-in above all, for up to 25 s before it answers. The driver now sends the same
  request again after each `Retry-After` (at least 1 s and at most 60 s; 3 s when there is none
  or it isn't a whole number), for every request including the sign-in, until the new option
  `adbc.chargewire.resume_timeout` (seconds, default 600, `0`: don't wait) has passed; then it
  returns the server's error (`server_resuming`, `StatusIO`) with its message. These repeats don't count against
  `adbc.chargewire.retry.max_attempts`, and other 503s keep their bounded retry. Python's
  `connect()` takes it as `resume_timeout`; `DatabaseOptions.RESUME_TIMEOUT` names the option.
- `server_resuming` in the error mapping (`StatusIO`).

## [2.1.0] - 2026-11-05

For Chargewire 2.1.0 (Hardcharger 2.7.0).

### Fixed

- **A statement never runs again in a replacement session when its transaction was lost.** A
  session lost while a transaction begun with SQL `BEGIN` was open (`session_not_found` after an
  idle eviction or a server restart) was replaced and the statement that found it gone was run
  again in the new session, in autocommit, because the driver knew of a transaction only when
  autocommit was turned off through it. That committed the statement on its own while the
  transaction's earlier statements had been rolled back with the session; a prepared statement
  was prepared again and run the same way. Now the driver runs a request again only when it
  knows no transaction was open: the server said so in the session document (`transaction`), and
  no statement that could begin one has run since. After a statement with a transaction word in
  it (`BEGIN`, `COMMIT`, …) it reads the session document once, when nothing else of the
  connection is running, so a committed transaction doesn't keep later statements from running
  again; until it has, it assumes one may be open. Otherwise the request fails with the
  transaction named as lost (`SessionLostError.Transaction`), and the next one runs in the
  replacement.
- Metadata, option and prepare calls that found the session lost inside a transaction (autocommit
  off, or SQL `BEGIN`) were run again quietly in the replacement session, so the caller never
  learned that its transaction was gone and its next statements ran outside it. They now fail the
  same way.

### Added

- `session_ended` with reason `idle_in_transaction` (Chargewire 2.1.0, §5.6): the server ended a
  session whose transaction sat idle past its `idle_in_transaction_timeout_s` and rolled the
  transaction back. With `adbc.chargewire.session.reconnect` on (the default) the driver replaces
  the session with the token it has, and the statement that found it ended fails rather than run
  again; the next statement runs in the replacement. Before, the error was reported and the
  connection stayed on the ended session, so every later call failed too. The session document's
  `idle_in_transaction_timeout_s` is read (`protocol.SessionDocument`).
- Tests for nested schemas (Chargewire 2.1.0, §10): GetObjects lists `sales.payroll` and a
  top-level `"sales.payroll"` as the server names them, its schema filter matches the path, and
  GetTableSchema finds a table in either. The driver passes schema names through as text and
  needed no change.

## [2.0.1] - 2026-10-29

### Fixed

- The README's status line still described 1.0.0 and Hardcharger 2.0.0. It now names no version,
  and points at the latest release and this changelog instead.

### Added

- `legacy/adbc-driver-warpsql` and the `warpsql-shim` workflow (run by hand): the PyPI project from before the
  rename, whose releases install this driver and forward its old import name, published with provenance through
  trusted publishing.

## [2.0.0] - 2026-10-29

The driver for the protocol under its new name, Chargewire: `adbc-driver-chargewire` on PyPI and the Go
module `github.com/gizmodata/chargewire-adbc/go/v2`. It works with servers on either side of the rename.

### Changed

- The protocol is now called Chargewire, and the driver speaks it under the new names: the
  `adbc-driver-chargewire` package (module `adbc_driver_chargewire`), the
  `libadbc_driver_chargewire` library (entry point `AdbcDriverChargewireInit`, driver manifest
  `chargewire.toml`), the Go module `github.com/gizmodata/chargewire-adbc/go/v2` (package
  `chargewire`), the `chargewire://` URI scheme, `adbc.chargewire.*` options and
  `chargewire.*` error details. Connection strings saved before the rename keep working: their
  URI scheme and option keys are read as the new ones.
- The driver also works with servers from before the rename. At the default base path, a server
  that doesn't answer discovery there but describes itself in the protocol's earlier spelling is
  spoken to in that spelling (its base path and header names) for the whole database and every
  connection opened from it; discovery runs once per database. A custom base path never falls
  back, and a server that answers neither way gets the original error.

## [1.0.0] - 2026-10-20

The first release: the ADBC driver for Chargewire protocol 1.0.0, for Python (`adbc-driver-chargewire`
on PyPI), Go (`github.com/gizmodata/chargewire-adbc/go`) and every other ADBC language through the
shared library. Earlier builds (0.1.0.dev0) were never published; the sections below are the
whole history, newest first, for anyone who installed one from git.
<!-- Set the date above at tag time. -->

### Added

- The Go module `github.com/gizmodata/chargewire-adbc/go` is versioned by `go/vX.Y.Z` tags on each
  release's commit (`go-module.yml` checks the pair and warms the module proxy); `RELEASING.md`
  has the release order.

- The access token is readable: `GetOption("adbc.chargewire.auth.access_token")` on a database or
  connection returns the token in use (renewed first when due, so a renewal shows; the
  caller's own token as given), and `adbc.chargewire.auth.access_token_expires_at` (read-only) when
  it expires, in RFC 3339 UTC, from `expires_in` or the token's `exp` (`""` when unknown). It is
  a bearer secret: the README says how to handle it, and how it pairs with attaching to a
  session. Other secrets (password, subject token, keys) stay write-only.
- Attaching to an existing session: `adbc.chargewire.session.id` set on the database makes its
  connections use that session instead of creating one, inside its open transaction if it has
  one. Opening checks it with `GET /chargewire/v1/sessions/{id}`; an id the server doesn't have, or
  another user's session, fails with `session_not_found` (`NotFound`) and no session is
  created. An attached session is never replaced when the server loses it.
  `adbc.chargewire.session.close_on_release` (database and connection option) says whether closing
  the connection ends the session: by default `true` for a session the connection created and
  `false` for an attached one, so a transaction can be handed from one process to another. A
  session lives on one server instance: behind a load balancer, attaching works only when
  routing reaches that instance (the README says more).
- Browser sign-in (Chargewire spec §4.3, servers with capability `oauth_browser`):
  `adbc.chargewire.auth.type=oauth_browser` signs in through a browser with the server's identity
  provider when the first connection opens. The driver creates a flow, opens its
  `authorize_url` (`adbc.chargewire.oauth.browser`: a command, or `none` to print the URL to stderr;
  default the system's browser), polls it with its secret at the server's `poll_interval_s`
  and uses the server's access token it gets once (`adbc.chargewire.oauth.timeout_seconds`, default
  300). A refusal fails with status `UNAUTHENTICATED` and the server's code (`access_denied`,
  `invalid_credentials`, `flow_not_found`) and detail; a server without the capability is
  refused before any flow is created; a username, password or token beside it is refused.
  The poll secret and the token are never logged. For drivers built on this one,
  `chargewire.BrowserSignIn(ctx, options, opener, timeout)` runs the flow and returns the access
  token, its lifetime and the provider's name, and `chargewire.SystemBrowser(command)` is the
  default opener. Python: `connect(auth_type="oauth_browser", open_browser=False|"cmd",
  oauth_timeout_seconds=...)`, `DatabaseOptions.OAUTH_BROWSER`, `OAUTH_TIMEOUT_SECONDS`.
  Tests: unit tests against a fake server (the token used, the capability required, each
  failure's code and detail, timeout and cancellation, conflicting credentials, the exported
  function) and live ones against chargewire-duckdb with a mock OpenID Connect provider that checks
  PKCE (a sign-in, the exported token opening a session, a refusal at the provider, an
  unauthorized user, a server without the capability); Python unit and live tests.
- `adbc.chargewire.auth.access_token` can be set again on an open database that signs in with an
  access token or through the browser: its connections send the new token from their next
  request on (a wrapper that renews or shares tokens needs no new database). On a database
  that signs in another way it is refused (`INVALID_STATE`).
- `adbc.chargewire.session.transaction` (read-only, connection; Python
  `ConnectionOptions.SESSION_TRANSACTION`): the session's transaction as the server reports
  it (`none`, `open`, `aborted`, or `unknown`), which sees SQL `BEGIN` and `COMMIT` too.
- The live-test servers (Go and Python) run with a `HOME` of their own.
- LZ4_FRAME Arrow buffer compression (Chargewire §8.2, capability
  `ipc_compression_lz4_frame`), for results and uploads: `adbc.chargewire.ipc_compression=lz4_frame`.
  Every request that returns Arrow sends `Chargewire-IPC-Compression`, each partition's included.
- `adbc.chargewire.ipc_compression=auto`, the new default: `lz4_frame` on a link with a round
  trip under 10 ms, `zstd` on a slower one. The round trip is the discovery request's,
  timed from its last byte sent to the answer's first.
- `adbc.chargewire.ingest.parts=auto`, the new default: 16 parts on a near link, 32 on a far one.

### Changed

- **Chargewire protocol 1.0.0.** The driver follows the protocol's first stable version (spec
  1.0.0), with no fallbacks for the draft's names:
  - Discovery has no `draft` field and no `result_retention` / `partitioned_results`
    capabilities (retention is core: the driver may ask, and reads `retained`). Unknown
    capabilities are ignored.
  - Enumerations are open (§12): a statement or ingest state the driver doesn't know is not
    terminal (it keeps polling), an unknown `reason` / `cancel_reason` is a generic one, an
    unknown code is handled by its HTTP status. The code lists now hold every 1.0 code
    (`unsupported_grant_type`, `partition_not_found`, `internal_error`, …);
    `client_certificate_required` is now `certificate_required`.
  - Codes the driver makes up start with `client_`: `client_protocol_error` (was
    `protocol_error`), `client_http_error` for an error status without a problem document (a
    proxy's page; it used to borrow a server code from the status, such as
    `payload_too_large` or `execution_error`), and `client_statement_failed` /
    `client_statement_cancelled` for a failed or cancelled document without its error.
  - A statement cancelled or failed by the time the server answers its submit is an error
    response (409 `statement_cancelled` with `cancel_reason`, 422 `execution_error`, …) naming
    the statement in `Chargewire-Statement-Id`: the driver reports it (`StatusCancelled` for a
    cancel) and frees that statement. A `failed`/`cancelled` document is still an error
    whatever the status. 502 and 421 without a known code are now `StatusIO`.
  - A split result must state its order: `Chargewire-Result-Partitions` above 1 without a
    known `Chargewire-Result-Order` (or a document's `partition_order`) is `client_protocol_error`,
    no longer read as `any`.
  - Partitions on other servers: the driver sends `Chargewire-Result-Locations: remote` only to a
    server listing `result_locations`, refuses a location it did not ask for or one that
    would drop TLS (the access token goes along), and reads remote partitions at once with
    the others (they were read one after another). On the streamed path it finds them in the
    statement document (§6.2).
  - Browser sign-in follows the same rules: an unknown flow `state` keeps polling; a failed
    flow's `error.code` (`access_denied`, `invalid_credentials`, `identity_provider_error`,
    or an unknown one, which is a generic failure) is `StatusUnauthenticated` with no HTTP
    status attached (it arrives in a 200 poll answer); a failed flow without an error is
    `client_sign_in_failed`, and a malformed flow or token answer `client_protocol_error`.
  - Partitioned ingest commits with `{"part_count": n}` (was `{"parts": n}`).
  - `adbc.chargewire.statement.query_tag` must be a JSON object (`null` was accepted); it is sent
    compact and ASCII-escaped (`\uXXXX`), refused over 4,096 bytes, and now also labels
    ingests.
  - `invalid_token` is answered by signing in again (once) only without a reason, with
    `token_expired` or with an unknown reason; after `token_revoked`, `credentials_changed`
    or `principal_dropped` the error comes back with its reason (error detail
    `chargewire.reason`; `chargewire.cancel_reason` for cancels). A session ended with `token_expired`
    is replaced only after a new token; with a token issued elsewhere it is not replaced.
  - A TLS alert `certificate_required` / `certificate_revoked` at the handshake is reported
    as that refusal (`StatusUnauthenticated`, code `certificate_required` /
    `certificate_revoked`), not as an unreachable server (`StatusIO`).
  - `Retry-After` is read as delta-seconds only.
- Compressed record batches are decompressed by the driver, not Arrow's reader: every
  buffer of a batch at once, straight into one new body, while a goroutine keeps reading
  the stream's next messages; buffers sent as they are (length -1) are copied. Bodies come
  from a recycler over the C library's allocator (calloc), which faulted in fresh pages
  for every batch. Over a 100 Gbit/s LAN, SF10 lineitem in 32 partitions with lz4_frame:
  0.99 s -> 0.89 s through Python; one stream: 5.8 s -> 3.7 s on a laptop.
- Results may be split into up to 32 partitions by default (was 8); the server chooses
  how many by the result's size.
- Uploads go as 16,384-row slices of larger batches, and in about one part per two
  slices: SF1 lineitem into Hardcharger over a LAN took 0.45-0.67 s with whole 122,880-row
  batches (a few of the server's insert threads got one more row group to compress than
  the others) and 0.31-0.36 s in slices. A small upload no longer opens every part.

- Python 3.11 is the minimum (`requires-python = ">=3.11"`): pyarrow 26, the floor,
  needs 3.11, so on 3.10 the package could never be installed.

### Fixed

- `AdbcStatementCancel` from another thread (Python's Ctrl-C handler) could cancel the
  caller's *next* statement instead: the C shim cancelled the blocked call's context first,
  which unblocked the caller before the server had been asked to stop anything, and when the
  statement's id was not known yet the session-wide cancel then landed on the statement the
  caller had moved on to (`SELECT 42` answered 409 `statement_cancelled`, reason `client`,
  about 1 run in 20 of the Ctrl-C test). The shim now asks the server first and cancels the
  context after. The Go API (`Statement.Cancel`) already had that order.
- A user's SQL that failed while running (`execution_error`, HTTP 422: a division by zero, a
  failed conversion, `error()`) was reported as ADBC `StatusInternal`, which says the driver
  or server is broken, so Python raised `InternalError`. It is now `StatusInvalidArgument`
  (`ProgrammingError`), like `sql_error` and like the DuckDB and PostgreSQL ADBC drivers. The
  mapping is now one table that covers every code in the spec's error table (§11) and the
  driver's `client_` codes; other changes: SQLSTATE class `23` on a SQL error is
  `StatusIntegrity` (`IntegrityError`; Hardcharger sends `23000` for a violated constraint);
  SQLSTATE `42P01`/`42P07` refine only `sql_error`/`execution_error`, not another code that
  carries one; `method_not_allowed` (and an unknown code with HTTP 405) is
  `StatusNotImplemented`, not `StatusInvalidState`; `ingest_part_exists` is
  `StatusAlreadyExists`; an unknown code with HTTP 408 is `StatusIO` (retryable), and one
  with no HTTP status at all (inside a failed statement document) is `StatusUnknown`, not
  `StatusInternal`. Tests: a unit test per code, one that fails when the spec's table
  (`go/chargewire/testdata/chargewire-errors.md`, or a `chargewire.md` named by
  `CHARGEWIRE_ADBC_TEST_SPEC`) or the protocol package has a code without a status, live Go
  tests and Python tests of the DB-API class of a division by zero, a conversion, a failure
  mid-stream, a syntax error, an unknown table, a duplicate key, a cancel, a timeout and a
  wrong password.
- A TLS connection (`chargewire://`, the default, or `https://`) to a server that speaks plain HTTP
  failed with Go's "server gave HTTP response to HTTPS client", after four tries. It now fails
  at once with what to do: `The server at HOST:PORT speaks plain HTTP, not TLS. Add ?tls=false
  to the URI to connect without encryption.` (for an `https://` URI: "Use an http:// URI
  instead of https://"), status `IO`. Only the TLS hello reaches the server: no request and no
  credential (live tests record the bytes through a TCP proxy, Go and Python). The driver never
  falls back to plaintext.
- Closing a connection after the server stopped returned an error ("closing the session: …
  connection refused"), so Python's `Connection.close()` raised and left the driver's database
  handle open ("Cannot close AdbcDatabase with open AdbcConnection"). Ending the session is now
  best-effort: at most two tries within 5 s (was four within 10 s), and a server that is gone,
  hangs or refuses no longer fails `Close`, which always releases the connection's local
  resources; the session ends on the server when it idles out. Live tests stop the server (or
  freeze it behind a proxy) and close statements, connections and the database, with no error,
  no leaked goroutine (goleak) and no Arrow memory left.
- `TestIntegrationIngestPartAnswerLost` failed now and then ("10 part requests, want 9"):
  the answer to the part sent again was itself lost, and the part went a third time. The
  server answers a part or upload sent again with its `Idempotency-Key` from the first
  upload without reading the body, and the Chargewire SDK then closed the connection with
  the body unread on Linux and macOS, which resets it, so a client still sending got a
  broken pipe or a reset instead of the answer (fixed in chargewire 2dbcd84, which drains
  before closing on every platform; Hardcharger main cadc02e takes it, through chargewire 3aefd27). The tests' fault proxy
  had the same defect on its own hop (the answer passed on, the rest of the body unread):
  it now reads every request body to its end. No row was ever written twice: every run
  checked the table row by row. New live test `TestIntegrationIngestAnswerToARetryArrives`
  sends a part and a one-request upload (about 2 MB) again 40 times each, directly and
  through the proxy, and asserts every answer arrives and the rows count once; it fails
  on servers before chargewire 2dbcd84 (8 of 10 runs on chargewire-duckdb 17e4609) and passes
  with it.
- The `dbapi` module exports PEP 249's exception classes (`Error`, `Warning`, `InterfaceError`,
  `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`,
  `ProgrammingError`, `NotSupportedError`), as PEP 249 requires of a driver module. They are
  `adbc_driver_manager`'s own classes, so `except dbapi.ProgrammingError` and
  `except adbc_driver_manager.ProgrammingError` catch the same errors.
- The browser sign-in live tests (`TestIntegrationBrowserSignIn`, `…Refused`) started every
  server with chargewire-duckdb's flags (`--oauth-issuer`, `--oauth-identities`), so against
  hardcharger-server, the server CI runs, they failed at startup. Their server flags and
  client-secret variable now default by `CHARGEWIRE_ADBC_TEST_SERVER_NAME`: for Hardcharger,
  `--token-allowed-issuer`, `--token-allowed-audience`, `--token-authorized-emails`,
  `--oauth-client-id` and `--oauth-base-url` (browser sign-in on the server's own port, §4.3)
  with `HARDCHARGER_OAUTH_CLIENT_SECRET`; both still take an override.
- Live tests that count ingest parts or cut a result after so many bytes failed now and then
  on a loaded machine (`TestIntegrationIngestInParts`: 32 parts, want 16;
  `TestIntegrationResumeSplitResult`: 0 resumed downloads): the one discovery round trip that
  `auto` decides by took over 10 ms on loopback, so they got the far link's zstd and 32 parts.
  The proxied live tests now count their link as near; the choice by round trip stays
  unit-tested. (Forcing every link far made 8 live tests fail before and none after.)

### Changed (CI)

- CI (`ci.yml`, every push and pull request) runs the live-server suites, Go (race detector)
  and Python, against hardcharger-server from `ghcr.io/gizmodata/hardcharger:main`; they ran
  only in the release workflow before. The release workflow gets them through `ci.yml`.

### Added

- `chargewire.NewDriverWithName(alloc, displayName)`: the driver under another GetInfo driver
  name, for drivers built on this one (hardcharger-adbc).
- Partitioned ingest (Chargewire spec §8.3): `adbc.chargewire.ingest.parts` (default 8). On a
  server with the `partitioned_ingest` capability, an ingest of two batches or more is
  opened once, sent as up to that many parts at once over separate connections, and
  committed as one; any failed part aborts it, so nothing is written. `1`, a one-batch
  upload, or a server without the capability sends one ordered stream as before. Python:
  `StatementOptions.INGEST_PARTS`.

### Changed

- `adbc.chargewire.statement.result_partitions` defaults to 8 (was 1): large results are split
  and read over several connections at once. The server decides: a result of fewer than
  two batches still comes as one stream, and a server without `result_partitions` never
  sees the request. Set it to 1 for one stream.

### Fixed

- A split result whose partition's connection broke could fail with "the statement was
  cancelled" (the server's reaction) instead of the broken connection and its advice to
  retain results, whenever another partition saw the cancellation first. A cancellation
  the application didn't ask for now waits (up to 2 s) for the partition that caused it.

### Added (earlier)

- Client certificate sign-in (Chargewire spec §4.4, servers with capability `mtls_identity`):
  `adbc.chargewire.auth.type` = `client_certificate` (alias `mtls`; default `password`) signs in
  with `{"grant_type": "client_certificate"}` over the TLS client certificate
  (`adbc.chargewire.tls.client_cert`/`client_key`), with no password, and renews the token the
  same way before it expires. Refused before connecting without a client certificate,
  over plain HTTP, or beside a username, password, subject token or access token; refused
  with a clear error by a server that doesn't offer it. The new problem codes
  (`client_certificate_required`, `unknown_certificate_identity`, `certificate_revoked`,
  `certificate_mismatch`, `certificate_identity_mismatch`) map to `Unauthenticated` and
  carry `chargewire.code`. Python: `DatabaseOptions.AUTH_TYPE` and `connect(auth_type=...)`.
- Split results (Chargewire spec §6.10): `adbc.chargewire.statement.result_partitions` (most
  partitions, default 1) and `adbc.chargewire.statement.result_order` (`preserve` or `any`).
  The driver reads every partition of a split result at once, one connection each, and
  merges them into one reader: round robin for `interleaved` results (the server's order),
  as they arrive for `any`. A failed or cut-off partition fails the whole result. Python:
  `StatementOptions.RESULT_PARTITIONS` and `RESULT_ORDER`.
- The HTTP transport keeps up to 256 idle connections per host (one per partition).

- Repository created: README with the driver's scope, planned layout and the
  mapping from ADBC calls to Chargewire operations.
- **Go ADBC driver** (`go/chargewire`) for any Chargewire server (spec draft 0.1.0), on
  `driverbase-go`, over Go's `net/http` with Arrow IPC from `arrow-go`:
  - `chargewire://host:port` URIs (HTTPS; `?tls=false` for plain HTTP; `https://`/`http://` and
    custom base paths too). TLS is verified unless skip-verify is set explicitly; custom CA,
    mutual TLS and server-name options.
  - Password, token-exchange and pre-issued-token sign-in, with tokens renewed before they
    expire and once when refused.
  - Queries on the streamed path, or asynchronous (submit, long-poll, retained result) with
    the async option; DDL/DML through `ExecuteQuery` returns an empty schema and the rows
    affected; `ExecuteUpdate`; `ExecutePartitions`/`ReadPartition`, also on other connections
    and from partition locations on other servers.
  - Prepared statements created on the server only when needed, Arrow parameters (one row
    per execution, `Bind` and `BindStream`), `GetParameterSchema`, `ExecuteSchema`.
  - Bulk ingest with the standard `adbc.ingest.*` options (create, append, replace,
    create_append, temporary, target catalog and schema), streamed above 16 MiB.
  - `GetObjects` (one request per level), `GetTableSchema`, `GetTableTypes`, `GetInfo` with
    the server's name and version; autocommit, commit, rollback; current catalog and schema.
  - Server-side cancel from `Cancel`, Ctrl+C, closing the statement, releasing an unread
    result or a Go context ending, including statements whose answer has not arrived.
  - Statement timeouts, query tags, zstd Arrow compression of results and uploads.
  - Resilience: `Idempotency-Key` on every submit, execute and ingest; retries of requests
    that are safe to repeat (with `Retry-After`); broken downloads of retained results
    resume from the next batch; streams without their end-of-stream marker are failures;
    lost sessions are replaced with their options replayed and the loss reported (never
    inside a transaction); finished statements are freed on the server in the background.
  - Errors carry the ADBC status for the server's error code, its `sqlstate`, and details
    `chargewire.code` and `chargewire.request_id`; `chargewire.ErrorCode` reads the code in Go.
- **C shared library** `libadbc_driver_chargewire` (entry point `AdbcDriverChargewireInit`),
  generated cgo exports, a driver manifest template, and a Linux build against glibc 2.17.
- **Python bindings** `adbc-driver-chargewire`: the library in a `py3-none` platform wheel and
  `dbapi.connect()`.
- Tests: Go and Python unit tests that need no server, and live-server suites ported from
  gizmosql-adbc plus Chargewire resilience tests through a fault-injecting proxy; they start
  the server named by `CHARGEWIRE_ADBC_TEST_SERVER_BINARY` and fail without it.
- CI (`.github/workflows/ci.yml`): gofmt, vet, Go and Python unit tests, the library on
  Linux (amd64, arm64), macOS and Windows with a glibc check, and the wheel. The live-server
  suites are not in CI yet: the reference server is built from a private repository.
- `docs/testing.md` (what is tested, what was not ported and why) and
  `docs/spec-feedback.md` (spec gaps and reference-server issues found).

### Changed

- Follows chargewire `188cd84`, which resolves every item in `docs/spec-feedback.md`:
  - A statement whose answer has not arrived is cancelled with the session cancel
    (`POST /sessions/{id}/cancel`) when it is the connection's only statement running;
    beside other statements, or on a server without it, the driver still resends with the
    `Idempotency-Key`. Explicit stops (Cancel, Close while running, a Go context ending)
    send `POST …/cancel`; releasing a result deletes the statement, which lets a write
    finish.
  - `ExecuteSchema` returns the prepared statement's `result_kind`/`result_schema` (the
    empty schema for statements without rows) instead of NotImplemented.
  - `GetObjects` reports primary and foreign keys, with one request to each key endpoint.
  - SQLSTATEs `42P01` and `42P07` are ADBC NotFound and AlreadyExists.
  - Removed workarounds the server no longer needs: growing long-poll waits (back to a
    fixed 15 s) and precision/scale taken from Arrow decimal types.

### Release engineering

- The release's live suites pull `hardcharger-server` from the image
  `ghcr.io/gizmodata/hardcharger` with `GITHUB_TOKEN` instead of a personal token.
- `THIRD_PARTY_NOTICES` (the Go runtime's and every linked Go module's license), generated by
  `scripts/third_party_notices.sh`, checked by CI, inside every wheel and library tarball.
- `.github/workflows/release.yml`: on a `vX.Y.Z` tag, every suite on the tagged commit (ci.yml,
  and the Go and Python live-server suites against `hardcharger-server` from Hardcharger's
  releases), then the library and a wheel for Linux amd64/arm64 (manylinux, glibc
  checked, installed and loaded on Oracle Linux 8), macOS arm64/amd64 and Windows amd64 (Windows
  arm64: the library only; its dependencies have no wheels there), a GitHub Release, and PyPI through the `pypi` environment (trusted publishing, approval).
