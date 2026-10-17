"""Shared fixtures.

Integration tests (marked ``integration``) run against a live server started from
``WARP_SQL_ADBC_TEST_SERVER_BINARY`` (a binary with warp-duckdb's interface: ``--host``,
``--port``, ``--username``, ``--tls-cert``/``--tls-key``, password in ``WARP_DUCKDB_PASSWORD``)
on a free port. Without it they fail; they never skip. Unit tests need no server. For another
server, name its password variable in ``WARP_SQL_ADBC_TEST_SERVER_PASSWORD_ENV`` (Hardcharger:
``HARDCHARGER_ADMIN_PASSWORD``), its user-name flag in
``WARP_SQL_ADBC_TEST_SERVER_USERNAME_FLAG`` (Hardcharger: ``--admin-username``), and its name
(for GetInfo) in ``WARP_SQL_ADBC_TEST_SERVER_NAME``.
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import os
import secrets
import signal
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

SERVER_BINARY_ENV = "WARP_SQL_ADBC_TEST_SERVER_BINARY"
SERVER_PASSWORD_ENV = (
    os.environ.get("WARP_SQL_ADBC_TEST_SERVER_PASSWORD_ENV") or "WARP_DUCKDB_PASSWORD"
)
USERNAME = "warp_test"

NATIONS = [
    "ALGERIA",
    "ARGENTINA",
    "BRAZIL",
    "CANADA",
    "EGYPT",
    "ETHIOPIA",
    "FRANCE",
    "GERMANY",
    "INDIA",
    "INDONESIA",
    "IRAN",
    "IRAQ",
    "JAPAN",
    "JORDAN",
    "KENYA",
    "MOROCCO",
    "MOZAMBIQUE",
    "PERU",
    "CHINA",
    "ROMANIA",
    "SAUDI ARABIA",
    "VIETNAM",
    "RUSSIA",
    "UNITED KINGDOM",
    "UNITED STATES",
]


@dataclasses.dataclass
class Server:
    host: str
    port: int
    password: str
    process: subprocess.Popen
    tls: bool = False

    @property
    def base_url(self) -> str:
        return f"{'https' if self.tls else 'http'}://{self.host}:{self.port}"

    @property
    def uri(self) -> str:
        """The server's warp:// URI (plain HTTP unless it serves TLS)."""
        if self.tls:
            return f"warp://{self.host}:{self.port}"
        return f"warp://{self.host}:{self.port}?tls=false"


def _free_port() -> int:
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_up(server: Server, timeout: float = 20) -> None:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if server.process.poll() is not None:
            break
        try:
            with urllib.request.urlopen(url=f"{server.base_url}/warp", timeout=2, context=context):
                return
        except (urllib.error.URLError, OSError) as error:
            if "CERTIFICATE_REQUIRED" in str(error):
                return  # a server that requires client certificates answered the handshake
            last_error = error
        time.sleep(0.1)
    stop_server(server=server)
    pytest.fail(reason=f"no Warp SQL server answered at {server.base_url}/warp ({last_error})")


def start_server(log: Path, extra_args: list[str] | None = None) -> Server:
    """Start the server binary on a free port with a fresh password that contains ':'
    and '@' (credentials travel as JSON fields, so nothing may split them)."""
    binary = os.environ.get(SERVER_BINARY_ENV)
    if not binary:
        pytest.fail(
            reason=f"set {SERVER_BINARY_ENV} to a warp-duckdb binary to run the integration tests"
        )
    extra_args = extra_args or []
    port = _free_port()
    password = secrets.token_urlsafe(nbytes=12) + ":p@ss:word"
    with log.open(mode="ab") as out:
        process = subprocess.Popen(
            args=[
                binary,
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                os.environ.get("WARP_SQL_ADBC_TEST_SERVER_USERNAME_FLAG") or "--username",
                USERNAME,
                *[_mtls_flag(arg=arg) for arg in extra_args],
            ],
            env={**os.environ, SERVER_PASSWORD_ENV: password},
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    server = Server(
        host="127.0.0.1",
        port=port,
        password=password,
        process=process,
        tls="--tls-cert" in extra_args,
    )
    _wait_up(server=server)
    return server


def _mtls_flag(arg: str) -> str:
    """The server's own name for the client-CA flag (Hardcharger: ``--mtls-ca-cert``)."""
    if arg == "--tls-client-ca":
        return os.environ.get("WARP_SQL_ADBC_TEST_SERVER_MTLS_FLAG") or arg
    return arg


def stop_server(server: Server) -> None:
    if server.process.poll() is not None:
        return
    server.process.send_signal(signal.SIGTERM)
    server.process.send_signal(signal.SIGTERM)  # the second signal stops without draining
    try:
        server.process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        server.process.kill()


@pytest.fixture(scope="session")
def server_log(tmp_path_factory) -> Path:
    """The file the test servers' output goes to."""
    return tmp_path_factory.mktemp("server") / "warp-duckdb.log"


@pytest.fixture(scope="session")
def warp_server(server_log):
    """A plain-HTTP server on loopback, with a small ``nation`` table."""
    server = start_server(log=server_log)
    from adbc_driver_warpsql import dbapi

    with dbapi.connect(uri=server.uri, username=USERNAME, password=server.password) as conn:
        with conn.cursor() as cur:
            values = ", ".join(f"({key}, '{name}')" for key, name in enumerate(NATIONS))
            cur.execute(
                operation=f"CREATE TABLE nation (n_nationkey INTEGER, n_name VARCHAR); "
                f"INSERT INTO nation VALUES {values}"
            )
    yield server
    stop_server(server=server)


@pytest.fixture(scope="session")
def warp_uri(warp_server) -> str:
    return warp_server.uri


@pytest.fixture
def conn(warp_server):
    """A fresh DB-API connection to the shared server."""
    from adbc_driver_warpsql import dbapi

    with dbapi.connect(
        uri=warp_server.uri, username=USERNAME, password=warp_server.password
    ) as connection:
        yield connection


def raw_request(server: Server, method: str, path: str, body: dict | None = None):
    """Send a request to the server with its own token (same principal as the driver),
    for tests that look behind the driver's back. Returns (status, parsed body)."""
    token_request = urllib.request.Request(
        url=f"{server.base_url}/warp/v1/auth/token",
        method="POST",
        data=json.dumps(
            obj={"grant_type": "password", "username": USERNAME, "password": server.password}
        ).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(url=token_request) as response:
        token = json.load(fp=response)["access_token"]
    request = urllib.request.Request(
        url=f"{server.base_url}{path}",
        method=method,
        data=None if body is None else json.dumps(obj=body).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(url=request) as response:
            content = response.read()
            return response.status, json.loads(s=content) if content else None
    except urllib.error.HTTPError as error:
        return error.code, json.loads(s=error.read() or b"null")


#: The identity (e-mail SAN) of make_certificates' client certificate.
CLIENT_EMAIL = "warp-test-client@example.com"


def make_certificates(out_dir: Path) -> dict[str, Path]:
    """A throwaway CA; a server certificate for 127.0.0.1/localhost it signed; and two
    client certificates it signed, with e-mail identities: ``client`` (CLIENT_EMAIL) and
    ``stranger`` (stranger@example.com)."""
    import ipaddress

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    def name(common_name: str) -> x509.Name:
        return x509.Name(
            attributes=[x509.NameAttribute(oid=NameOID.COMMON_NAME, value=common_name)]
        )

    ca_key = ec.generate_private_key(curve=ec.SECP256R1())
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(name=name(common_name="warp-sql-adbc test CA"))
        .issuer_name(name=name(common_name="warp-sql-adbc test CA"))
        .public_key(key=ca_key.public_key())
        .serial_number(number=x509.random_serial_number())
        .not_valid_before(time=now - datetime.timedelta(minutes=5))
        .not_valid_after(time=now + datetime.timedelta(days=1))
        .add_extension(extval=x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            extval=x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            extval=x509.SubjectKeyIdentifier.from_public_key(public_key=ca_key.public_key()),
            critical=False,
        )
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )
    server_key = ec.generate_private_key(curve=ec.SECP256R1())
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(name=name(common_name="localhost"))
        .issuer_name(name=ca_cert.subject)
        .public_key(key=server_key.public_key())
        .serial_number(number=x509.random_serial_number())
        .not_valid_before(time=now - datetime.timedelta(minutes=5))
        .not_valid_after(time=now + datetime.timedelta(days=1))
        .add_extension(
            extval=x509.SubjectAlternativeName(
                general_names=[
                    x509.DNSName(value="localhost"),
                    x509.IPAddress(value=ipaddress.ip_address(address="127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(
            extval=x509.AuthorityKeyIdentifier.from_issuer_public_key(
                public_key=ca_key.public_key()
            ),
            critical=False,
        )
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )
    paths = {
        "ca": out_dir / "ca.pem",
        "cert": out_dir / "server.pem",
        "key": out_dir / "server-key.pem",
    }
    for label, email in [("client", CLIENT_EMAIL), ("stranger", "stranger@example.com")]:
        client_key = ec.generate_private_key(curve=ec.SECP256R1())
        client_cert = (
            x509.CertificateBuilder()
            .subject_name(name=name(common_name=label))
            .issuer_name(name=ca_cert.subject)
            .public_key(key=client_key.public_key())
            .serial_number(number=x509.random_serial_number())
            .not_valid_before(time=now - datetime.timedelta(minutes=5))
            .not_valid_after(time=now + datetime.timedelta(days=1))
            .add_extension(
                extval=x509.SubjectAlternativeName(general_names=[x509.RFC822Name(value=email)]),
                critical=False,
            )
            .add_extension(
                extval=x509.AuthorityKeyIdentifier.from_issuer_public_key(
                    public_key=ca_key.public_key()
                ),
                critical=False,
            )
            .sign(private_key=ca_key, algorithm=hashes.SHA256())
        )
        paths[f"{label}_cert"] = out_dir / f"{label}.pem"
        paths[f"{label}_key"] = out_dir / f"{label}-key.pem"
        paths[f"{label}_cert"].write_bytes(
            data=client_cert.public_bytes(encoding=serialization.Encoding.PEM)
        )
        paths[f"{label}_key"].write_bytes(
            data=client_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    paths["ca"].write_bytes(data=ca_cert.public_bytes(encoding=serialization.Encoding.PEM))
    paths["cert"].write_bytes(data=server_cert.public_bytes(encoding=serialization.Encoding.PEM))
    paths["key"].write_bytes(
        data=server_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return paths
