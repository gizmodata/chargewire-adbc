"""Option keys of the Chargewire driver (the ``chargewire.*`` options).

The names follow the ADBC driver style guide
(https://adbc-drivers.org/building-drivers/style-guide.html). The names from before it
(``adbc.chargewire.*``, and ``adbc.warp.*`` from before the protocol's rename) still work;
the driver prints a deprecation warning to stderr the first time a process uses each.
"""

import enum


class DatabaseOptions(enum.Enum):
    """Database options. The standard ``uri``, ``username`` and ``password``
    options and the ``adbc.telemetry.*`` options apply too."""

    #: Do not verify the server's TLS certificate (development only).
    TLS_SKIP_VERIFY = "chargewire.tls.skip_verify"
    #: PEM-encoded CA certificates to trust instead of the system's.
    TLS_ROOT_CERTS = "chargewire.tls.root_certs"
    #: PEM certificate chain presented to servers that require mutual TLS.
    TLS_CLIENT_CERT = "chargewire.tls.client_cert"
    #: PEM private key of the client certificate.
    TLS_CLIENT_KEY = "chargewire.tls.client_key"
    #: Host name the server certificate is checked against, instead of the URI's.
    TLS_SERVER_NAME = "chargewire.tls.server_name"
    #: How to sign in: "password" (the default: username and password, or a token) or
    #: "client_certificate" ("mtls"): the TLS client certificate (TLS_CLIENT_CERT and
    #: TLS_CLIENT_KEY) is the credential, with no password. The server binds the token to
    #: the certificate. "oauth_browser": sign in through a browser with the server's
    #: identity provider (Chargewire spec §4.3) when the first connection opens.
    AUTH_TYPE = "chargewire.auth.type"
    #: Browser sign-in: the command that opens the sign-in page (its URL is appended);
    #: default the system's browser; "none" prints the URL to stderr.
    OAUTH_BROWSER = "chargewire.oauth.browser"
    #: Browser sign-in: how long the person has to sign in, in seconds (default 300).
    OAUTH_TIMEOUT_SECONDS = "chargewire.oauth.timeout_seconds"
    #: A Chargewire access token obtained elsewhere (used as is, never renewed).
    ACCESS_TOKEN = "chargewire.auth.access_token"
    #: An identity provider's token, exchanged for an access token.
    SUBJECT_TOKEN = "chargewire.auth.subject_token"
    #: How often a request that is safe to repeat is sent, first try included.
    RETRY_MAX_ATTEMPTS = "chargewire.retry.max_attempts"
    #: How long, in seconds, a request is sent again while the server answers
    #: ``server_resuming`` (a stopped server starting again), each time after its
    #: Retry-After: "600" (the default); "0" returns the error at once.
    RESUME_TIMEOUT = "chargewire.retry.resume_timeout_seconds"
    #: Replace a session the server lost with a new one ("true", the default).
    SESSION_RECONNECT = "chargewire.session.reconnect"


class ConnectionOptions(enum.Enum):
    """Connection options (read-only)."""

    #: The connection's current server session id.
    SESSION_ID = "chargewire.session.id"
    #: How often the connection's session was lost and replaced.
    SESSION_REPLACEMENTS = "chargewire.session.replacements"
    #: The session's transaction as the server reports it: "none", "open", "aborted",
    #: or "unknown" (a value this driver doesn't know). Reading it costs a request.
    SESSION_TRANSACTION = "chargewire.session.transaction"


class StatementOptions(enum.Enum):
    """Statement options. They can also be set on the database or the
    connection, as defaults for the statements created from it."""

    #: Run queries asynchronously, with a retained, resumable result.
    ASYNC = "chargewire.query.async"
    #: Ask the server to keep results for re-reading ("true"/"false").
    RETAIN_RESULT = "chargewire.query.retain_result"
    #: Seconds after which the server cancels a statement.
    TIMEOUT_SECONDS = "chargewire.query.timeout_seconds"
    #: A JSON object recorded with every statement on the server.
    QUERY_TAG = "chargewire.query.tag"
    #: Arrow buffer compression for results and uploads: "auto" (the default: "lz4_frame"
    #: on a link with a round trip under 10 ms, "zstd" on a slower one), "lz4_frame",
    #: "zstd" or "none". A codec the server doesn't offer is not used.
    IPC_COMPRESSION = "chargewire.ipc.compression"
    #: The most partitions a result may be split into ("32", the default; "1": never). A
    #: server that offers it splits a large result (how many, it decides by the result's
    #: size), and the driver reads every partition at once, each over its own connection.
    #: A server without the capability is never asked.
    RESULT_PARTITIONS = "chargewire.query.result_partitions"
    #: "preserve" (the default): a split result comes back in order; "any": in whatever
    #: order its batches arrive.
    RESULT_ORDER = "chargewire.query.result_order"
    #: The most connections one bulk ingest is sent over at once: "auto" (the default: 16
    #: on a near link, 32 on a far one) or "1" to "256" ("1": one stream). On a server that
    #: offers it, an upload of more than one 16,384-row slice is a partitioned ingest, in
    #: about one part per two slices: its slices are spread over the parts, committed as
    #: one, all or nothing. The rows then land in no defined order; set "1" to keep the
    #: upload's order.
    INGEST_PARTS = "chargewire.ingest.parts"
    #: The id of the statement's last execution (read-only).
    STATEMENT_ID = "chargewire.query.id"


# The part after "chargewire." of each option whose name changed beyond its prefix, mapped
# to the part after the old prefixes.
_RENAMED = {
    "query.async": "statement.async",
    "query.retain_result": "statement.retain_result",
    "query.timeout_seconds": "statement.timeout_seconds",
    "query.tag": "statement.query_tag",
    "query.result_partitions": "statement.result_partitions",
    "query.result_order": "statement.result_order",
    "query.id": "statement.id",
    "retry.resume_timeout_seconds": "resume_timeout",
    "ipc.compression": "ipc_compression",
}


def _old_keys(key: str) -> tuple[str, ...]:
    """The names an option had before the style guide (empty for a key that isn't ours)."""
    if not key.startswith("chargewire."):
        return ()
    rest = key[len("chargewire.") :]
    rest = _RENAMED.get(rest, rest)
    return (f"adbc.chargewire.{rest}", f"adbc.warp.{rest}")


def _set_default(options: dict, key: str, value: str) -> None:
    """``options.setdefault(key, value)``, where an old name of the key counts as set: an
    option the caller gave under its old name wins over a keyword argument, as it did."""
    if key in options or any(old in options for old in _old_keys(key=key)):
        return
    options[key] = value
