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
    #: The most partitions a result may be split into ("1", the default: never). A server
    #: that offers it splits a large result, and the driver reads every partition at once,
    #: each over its own connection.
    RESULT_PARTITIONS = "adbc.warp.statement.result_partitions"
    #: "preserve" (the default): a split result comes back in order; "any": in whatever
    #: order its batches arrive.
    RESULT_ORDER = "adbc.warp.statement.result_order"
    #: The id of the statement's last execution (read-only).
    STATEMENT_ID = "adbc.warp.statement.id"
