"""Option keys of the Warp SQL driver (the ``adbc.warp.*`` options)."""

import enum


class DatabaseOptions(enum.Enum):
    """Database options. The standard ``uri``, ``username`` and ``password``
    options and the ``adbc.telemetry.*`` options apply too."""

    #: Do not verify the server's TLS certificate (development only).
    TLS_SKIP_VERIFY = "adbc.warp.tls.skip_verify"
    #: PEM-encoded CA certificates to trust instead of the system's.
    TLS_ROOT_CERTS = "adbc.warp.tls.root_certs"
    #: PEM certificate chain presented to servers that require mutual TLS.
    TLS_CLIENT_CERT = "adbc.warp.tls.client_cert"
    #: PEM private key of the client certificate.
    TLS_CLIENT_KEY = "adbc.warp.tls.client_key"
    #: Host name the server certificate is checked against, instead of the URI's.
    TLS_SERVER_NAME = "adbc.warp.tls.server_name"
    #: How to sign in: "password" (the default: username and password, or a token) or
    #: "client_certificate" ("mtls"): the TLS client certificate (TLS_CLIENT_CERT and
    #: TLS_CLIENT_KEY) is the credential, with no password. The server binds the token to
    #: the certificate. "oauth_browser": sign in through a browser with the server's
    #: identity provider (Warp SQL spec §4.3) when the first connection opens.
    AUTH_TYPE = "adbc.warp.auth.type"
    #: Browser sign-in: the command that opens the sign-in page (its URL is appended);
    #: default the system's browser; "none" prints the URL to stderr.
    OAUTH_BROWSER = "adbc.warp.oauth.browser"
    #: Browser sign-in: how long the person has to sign in, in seconds (default 300).
    OAUTH_TIMEOUT_SECONDS = "adbc.warp.oauth.timeout_seconds"
    #: A Warp SQL access token obtained elsewhere (used as is, never renewed).
    ACCESS_TOKEN = "adbc.warp.auth.access_token"
    #: An identity provider's token, exchanged for an access token.
    SUBJECT_TOKEN = "adbc.warp.auth.subject_token"
    #: How often a request that is safe to repeat is sent, first try included.
    RETRY_MAX_ATTEMPTS = "adbc.warp.retry.max_attempts"
    #: Replace a session the server lost with a new one ("true", the default).
    SESSION_RECONNECT = "adbc.warp.session.reconnect"


class ConnectionOptions(enum.Enum):
    """Connection options (read-only)."""

    #: The connection's current server session id.
    SESSION_ID = "adbc.warp.session.id"
    #: How often the connection's session was lost and replaced.
    SESSION_REPLACEMENTS = "adbc.warp.session.replacements"
    #: The session's transaction as the server reports it: "none", "open", "aborted",
    #: or "unknown" (a value this driver doesn't know). Reading it costs a request.
    SESSION_TRANSACTION = "adbc.warp.session.transaction"


class StatementOptions(enum.Enum):
    """Statement options. They can also be set on the database or the
    connection, as defaults for the statements created from it."""

    #: Run queries asynchronously, with a retained, resumable result.
    ASYNC = "adbc.warp.statement.async"
    #: Ask the server to keep results for re-reading ("true"/"false").
    RETAIN_RESULT = "adbc.warp.statement.retain_result"
    #: Seconds after which the server cancels a statement.
    TIMEOUT_SECONDS = "adbc.warp.statement.timeout_seconds"
    #: A JSON object recorded with every statement on the server.
    QUERY_TAG = "adbc.warp.statement.query_tag"
    #: Arrow buffer compression for results and uploads: "zstd" or "none".
    IPC_COMPRESSION = "adbc.warp.ipc_compression"
    #: The most partitions a result may be split into ("8", the default; "1": never). A
    #: server that offers it splits a large result, and the driver reads every partition at
    #: once, each over its own connection. A result of fewer than two batches comes as one
    #: stream either way, and a server without the capability is never asked.
    RESULT_PARTITIONS = "adbc.warp.statement.result_partitions"
    #: "preserve" (the default): a split result comes back in order; "any": in whatever
    #: order its batches arrive.
    RESULT_ORDER = "adbc.warp.statement.result_order"
    #: The most connections one bulk ingest is sent over at once ("8", the default; "1":
    #: one stream). On a server that offers it, an upload of two batches or more is a
    #: partitioned ingest: its batches are spread over the parts, committed as one, all or
    #: nothing. The rows then land in no defined order; set "1" to keep the upload's order.
    INGEST_PARTS = "adbc.warp.ingest.parts"
    #: The id of the statement's last execution (read-only).
    STATEMENT_ID = "adbc.warp.statement.id"
