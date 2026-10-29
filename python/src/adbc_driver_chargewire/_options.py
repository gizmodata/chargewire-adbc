"""Option keys of the Chargewire driver (the ``adbc.chargewire.*`` options)."""

import enum


class DatabaseOptions(enum.Enum):
    """Database options. The standard ``uri``, ``username`` and ``password``
    options and the ``adbc.telemetry.*`` options apply too."""

    #: Do not verify the server's TLS certificate (development only).
    TLS_SKIP_VERIFY = "adbc.chargewire.tls.skip_verify"
    #: PEM-encoded CA certificates to trust instead of the system's.
    TLS_ROOT_CERTS = "adbc.chargewire.tls.root_certs"
    #: PEM certificate chain presented to servers that require mutual TLS.
    TLS_CLIENT_CERT = "adbc.chargewire.tls.client_cert"
    #: PEM private key of the client certificate.
    TLS_CLIENT_KEY = "adbc.chargewire.tls.client_key"
    #: Host name the server certificate is checked against, instead of the URI's.
    TLS_SERVER_NAME = "adbc.chargewire.tls.server_name"
    #: How to sign in: "password" (the default: username and password, or a token) or
    #: "client_certificate" ("mtls"): the TLS client certificate (TLS_CLIENT_CERT and
    #: TLS_CLIENT_KEY) is the credential, with no password. The server binds the token to
    #: the certificate. "oauth_browser": sign in through a browser with the server's
    #: identity provider (Chargewire spec §4.3) when the first connection opens.
    AUTH_TYPE = "adbc.chargewire.auth.type"
    #: Browser sign-in: the command that opens the sign-in page (its URL is appended);
    #: default the system's browser; "none" prints the URL to stderr.
    OAUTH_BROWSER = "adbc.chargewire.oauth.browser"
    #: Browser sign-in: how long the person has to sign in, in seconds (default 300).
    OAUTH_TIMEOUT_SECONDS = "adbc.chargewire.oauth.timeout_seconds"
    #: A Chargewire access token obtained elsewhere (used as is, never renewed).
    ACCESS_TOKEN = "adbc.chargewire.auth.access_token"
    #: An identity provider's token, exchanged for an access token.
    SUBJECT_TOKEN = "adbc.chargewire.auth.subject_token"
    #: How often a request that is safe to repeat is sent, first try included.
    RETRY_MAX_ATTEMPTS = "adbc.chargewire.retry.max_attempts"
    #: Replace a session the server lost with a new one ("true", the default).
    SESSION_RECONNECT = "adbc.chargewire.session.reconnect"


class ConnectionOptions(enum.Enum):
    """Connection options (read-only)."""

    #: The connection's current server session id.
    SESSION_ID = "adbc.chargewire.session.id"
    #: How often the connection's session was lost and replaced.
    SESSION_REPLACEMENTS = "adbc.chargewire.session.replacements"
    #: The session's transaction as the server reports it: "none", "open", "aborted",
    #: or "unknown" (a value this driver doesn't know). Reading it costs a request.
    SESSION_TRANSACTION = "adbc.chargewire.session.transaction"


class StatementOptions(enum.Enum):
    """Statement options. They can also be set on the database or the
    connection, as defaults for the statements created from it."""

    #: Run queries asynchronously, with a retained, resumable result.
    ASYNC = "adbc.chargewire.statement.async"
    #: Ask the server to keep results for re-reading ("true"/"false").
    RETAIN_RESULT = "adbc.chargewire.statement.retain_result"
    #: Seconds after which the server cancels a statement.
    TIMEOUT_SECONDS = "adbc.chargewire.statement.timeout_seconds"
    #: A JSON object recorded with every statement on the server.
    QUERY_TAG = "adbc.chargewire.statement.query_tag"
    #: Arrow buffer compression for results and uploads: "auto" (the default: "lz4_frame"
    #: on a link with a round trip under 10 ms, "zstd" on a slower one), "lz4_frame",
    #: "zstd" or "none". A codec the server doesn't offer is not used.
    IPC_COMPRESSION = "adbc.chargewire.ipc_compression"
    #: The most partitions a result may be split into ("32", the default; "1": never). A
    #: server that offers it splits a large result (how many, it decides by the result's
    #: size), and the driver reads every partition at once, each over its own connection.
    #: A server without the capability is never asked.
    RESULT_PARTITIONS = "adbc.chargewire.statement.result_partitions"
    #: "preserve" (the default): a split result comes back in order; "any": in whatever
    #: order its batches arrive.
    RESULT_ORDER = "adbc.chargewire.statement.result_order"
    #: The most connections one bulk ingest is sent over at once: "auto" (the default: 16
    #: on a near link, 32 on a far one) or "1" to "256" ("1": one stream). On a server that
    #: offers it, an upload of more than one 16,384-row slice is a partitioned ingest, in
    #: about one part per two slices: its slices are spread over the parts, committed as
    #: one, all or nothing. The rows then land in no defined order; set "1" to keep the
    #: upload's order.
    INGEST_PARTS = "adbc.chargewire.ingest.parts"
    #: The id of the statement's last execution (read-only).
    STATEMENT_ID = "adbc.chargewire.statement.id"
