// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// defaultMaxAttempts is how often a request that is safe to repeat is sent.
const defaultMaxAttempts = 4

type databaseImpl struct {
	driverbase.DatabaseImplBase

	uri         string
	credentials protocol.Credentials
	tls         tlsOptions
	maxAttempts int
	reconnect   bool
	defaults    execOptions
	// attachSession is the session every connection attaches to
	// (OptionSessionID); "" creates one per connection.
	attachSession string
	// closeOnRelease is OptionSessionCloseOnRelease when it was set.
	closeOnRelease *bool
	// Browser sign-in (AuthTypeOAuthBrowser): the opener's command and how
	// long the person has.
	browserSignIn  bool
	browserCommand string
	browserTimeout time.Duration

	// Set by the first successful Open; the options above are fixed from
	// then on.
	mu        sync.Mutex
	client    *protocol.Client
	transport *http.Transport
	discovery *protocol.Discovery
}

type tlsOptions struct {
	skipVerify            bool
	rootCerts             string
	clientCert, clientKey string
	serverName            string
}

func newDatabase(ctx context.Context, driver *driverbase.DriverImplBase, opts map[string]string) (*databaseImpl, error) {
	base, err := driverbase.NewDatabaseImplBase(ctx, driver, driverbase.TracingOptions{
		ExporterName:      opts[adbc.OptionKeyTelemetryTracesExporter],
		TracingFolderPath: opts[adbc.OptionKeyTelemetryTracesFolderPath],
	})
	if err != nil {
		return nil, err
	}
	// Each database learns its own server's vendor name and version.
	base.DriverInfo = cloneDriverInfo(driver.DriverInfo)
	db := &databaseImpl{DatabaseImplBase: base, maxAttempts: defaultMaxAttempts, reconnect: true,
		defaults: defaultExecOptions()}
	for key, value := range opts {
		if err := db.SetOption(ctx, key, value); err != nil {
			return nil, err
		}
	}
	// Contradicting credentials fail here already when the uri is known (and
	// otherwise at the first Open).
	if base, err := parseURI(db.uri); err == nil {
		if err := db.checkCredentials(base); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (d *databaseImpl) opened() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.client != nil
}

func (d *databaseImpl) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case adbc.OptionKeyTelemetryTracesExporter, adbc.OptionKeyTelemetryTracesFolderPath:
		return nil // read when the database was created
	case adbc.OptionKeyTelemetryTraceParent:
		d.SetTraceParent(value)
		return nil
	}
	if handled, err := d.defaults.set(key, value); handled {
		return err
	}
	if key == OptionAccessToken && d.opened() {
		return d.replaceAccessToken(value)
	}
	if d.opened() {
		return d.ErrorHelper.InvalidState("option %s must be set before the first connection is opened", key)
	}
	switch key {
	case adbc.OptionKeyURI:
		if _, err := parseURI(value); err != nil {
			return d.ErrorHelper.InvalidArgument("%s", err)
		}
		d.uri = value
	case adbc.OptionKeyUsername:
		d.credentials.Username = value
	case adbc.OptionKeyPassword:
		d.credentials.Password = value
	case OptionAuthType:
		switch strings.ToLower(value) {
		case "", AuthTypePassword:
			d.credentials.ClientCertificate, d.browserSignIn = false, false
		case AuthTypeClientCertificate, authTypeMTLS:
			d.credentials.ClientCertificate, d.browserSignIn = true, false
		case AuthTypeOAuthBrowser:
			d.credentials.ClientCertificate, d.browserSignIn = false, true
		default:
			return invalidOption(key, value, `"password", "client_certificate" ("mtls") or "oauth_browser"`)
		}
	case OptionOAuthBrowser:
		d.browserCommand = value
	case OptionOAuthTimeoutSeconds:
		seconds, err := strconv.ParseFloat(value, 64)
		if err != nil || seconds <= 0 {
			return invalidOption(key, value, "a number of seconds above 0")
		}
		d.browserTimeout = time.Duration(seconds * float64(time.Second))
	case OptionAccessToken:
		d.credentials.AccessToken = value
	case OptionSubjectToken:
		d.credentials.SubjectToken = value
	case OptionTLSSkipVerify:
		b, err := parseBool(key, value)
		if err != nil {
			return err
		}
		d.tls.skipVerify = b
	case OptionTLSRootCerts:
		d.tls.rootCerts = value
	case OptionTLSClientCert:
		d.tls.clientCert = value
	case OptionTLSClientKey:
		d.tls.clientKey = value
	case OptionTLSServerName:
		d.tls.serverName = value
	case OptionRetryMaxAttempts:
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 {
			return invalidOption(key, value, "a whole number, 1 or more")
		}
		d.maxAttempts = n
	case OptionSessionReconnect:
		b, err := parseBool(key, value)
		if err != nil {
			return err
		}
		d.reconnect = b
	case OptionSessionID:
		d.attachSession = value
	case OptionSessionCloseOnRelease:
		if value == "" {
			d.closeOnRelease = nil
			return nil
		}
		b, err := parseBool(key, value)
		if err != nil {
			return err
		}
		d.closeOnRelease = &b
	default:
		return d.DatabaseImplBase.SetOption(ctx, key, value)
	}
	return nil
}

func (d *databaseImpl) GetOption(ctx context.Context, key string) (string, error) {
	if value, ok := d.defaults.get(key); ok {
		return value, nil
	}
	switch key {
	case adbc.OptionKeyURI:
		return d.uri, nil
	case adbc.OptionKeyUsername:
		return d.credentials.Username, nil
	case OptionAuthType:
		switch {
		case d.credentials.ClientCertificate:
			return AuthTypeClientCertificate, nil
		case d.browserSignIn:
			return AuthTypeOAuthBrowser, nil
		}
		return AuthTypePassword, nil
	case OptionOAuthBrowser:
		return d.browserCommand, nil
	case OptionOAuthTimeoutSeconds:
		return strconv.FormatFloat(d.timeoutOrDefault().Seconds(), 'f', -1, 64), nil
	case OptionTLSSkipVerify:
		return strconv.FormatBool(d.tls.skipVerify), nil
	case OptionTLSServerName:
		return d.tls.serverName, nil
	case OptionRetryMaxAttempts:
		return strconv.Itoa(d.maxAttempts), nil
	case OptionSessionReconnect:
		return strconv.FormatBool(d.reconnect), nil
	case OptionSessionID:
		return d.attachSession, nil
	case OptionSessionCloseOnRelease:
		if d.closeOnRelease != nil {
			return strconv.FormatBool(*d.closeOnRelease), nil
		}
	case adbc.OptionKeyTelemetryTraceParent:
		return d.GetTraceParent(), nil
	}
	// Secrets (password, tokens, keys) are write-only.
	return d.DatabaseImplBase.GetOption(ctx, key)
}

// Open creates a session on the server. The first Open also discovers the
// server and signs in; later ones share that client and its token.
func (d *databaseImpl) Open(ctx context.Context) (cnxn adbc.ConnectionWithContext, err error) {
	ctx, span := d.StartSpan(ctx, "Database.Open")
	defer driverbase.EndSpanWithError(span, &err)

	client, discovery, err := d.connect(ctx)
	if err != nil {
		return nil, err
	}
	if d.attachSession != "" {
		session, err := client.AttachSession(ctx, d.attachSession)
		if err != nil {
			op := "attaching to session " + d.attachSession
			if protocol.HasCode(err, protocol.CodeSessionNotFound) {
				op += " (the server doesn't have it for this user: it ended or idled out, it belongs to " +
					"another user, or it lives on another server instance)"
			}
			return nil, wrapError(err, op)
		}
		return newConnection(d, client, discovery, session, d.closeOnReleaseOr(false)), nil
	}
	session, err := client.OpenSession(ctx, nil, d.reconnect)
	if err != nil {
		return nil, wrapError(err, "opening a session")
	}
	return newConnection(d, client, discovery, session, d.closeOnReleaseOr(true)), nil
}

// closeOnReleaseOr is OptionSessionCloseOnRelease, or def when it is unset.
func (d *databaseImpl) closeOnReleaseOr(def bool) bool {
	if d.closeOnRelease != nil {
		return *d.closeOnRelease
	}
	return def
}

func (d *databaseImpl) connect(ctx context.Context) (*protocol.Client, *protocol.Discovery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client != nil {
		return d.client, d.discovery, nil
	}
	base, err := parseURI(d.uri)
	if err != nil {
		return nil, nil, d.ErrorHelper.InvalidArgument("%s", err)
	}
	if err := d.checkCredentials(base); err != nil {
		return nil, nil, err
	}
	transport, err := d.newTransport(base)
	if err != nil {
		return nil, nil, err
	}
	client := protocol.NewClient(protocol.Config{
		BaseURL:     base,
		HTTPClient:  &http.Client{Transport: transport},
		Credentials: d.credentials,
		MaxAttempts: d.maxAttempts,
		UserAgent:   userAgent(),
		Logger:      d.Logger,
	})
	discovery, err := client.Discover(ctx)
	if err != nil {
		transport.CloseIdleConnections()
		if hint := plainHTTPError(err, d.uri, base); hint != nil {
			return nil, nil, hint
		}
		return nil, nil, wrapError(err, "connecting to "+base.Redacted())
	}
	if d.credentials.ClientCertificate && !discovery.Has(protocol.CapabilityMTLSIdentity) {
		transport.CloseIdleConnections()
		return nil, nil, d.ErrorHelper.Unauthenticated("the server does not offer client certificate sign-in (capability %s): it needs TLS with client CAs and must list the %s grant (it offers %v)",
			protocol.CapabilityMTLSIdentity, protocol.GrantClientCertificate, discovery.Auth.GrantTypes)
	}
	if d.browserSignIn {
		signedIn, err := runBrowserSignIn(ctx, client, discovery, openBrowser(d.browserCommand), d.browserTimeout)
		if err != nil {
			transport.CloseIdleConnections()
			return nil, nil, err
		}
		client.UseAccessToken(signedIn.AccessToken)
	} else {
		if grant := d.grantType(); grant != "" && !discovery.SupportsGrant(grant) {
			transport.CloseIdleConnections()
			return nil, nil, d.ErrorHelper.Unauthenticated("the server does not accept the %s grant (it offers %v)", grant, discovery.Auth.GrantTypes)
		}
		if err := client.Authenticate(ctx); err != nil {
			transport.CloseIdleConnections()
			return nil, nil, wrapError(err, "signing in")
		}
	}
	d.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoVendorName:    discovery.Server.Name,
		adbc.InfoVendorVersion: discovery.Server.Version,
	})
	d.client, d.transport, d.discovery = client, transport, discovery
	return client, discovery, nil
}

// replaceAccessToken gives an open database a new access token, when it signs
// in with one (OptionAccessToken, or a browser sign-in): its connections send
// it from their next request on, and Open uses it.
func (d *databaseImpl) replaceAccessToken(token string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.credentials.AccessToken == "" && !d.browserSignIn {
		return d.ErrorHelper.InvalidState("%s can be replaced only on a database that signs in with an access token "+
			"or through the browser; this one signs in another way", OptionAccessToken)
	}
	if token == "" {
		return d.ErrorHelper.InvalidArgument("%s cannot be empty", OptionAccessToken)
	}
	d.credentials.AccessToken = token
	d.client.ReplaceAccessToken(token)
	return nil
}

func (d *databaseImpl) timeoutOrDefault() time.Duration {
	if d.browserTimeout > 0 {
		return d.browserTimeout
	}
	return DefaultBrowserSignInTimeout
}

// openBrowser is the opener a database with OptionOAuthBrowser = command
// uses. Tests replace it.
var openBrowser = SystemBrowser

// grantType is the token grant the credentials use, or "" for a token issued
// elsewhere.
func (d *databaseImpl) grantType() string {
	switch {
	case d.credentials.AccessToken != "":
		return ""
	case d.credentials.ClientCertificate:
		return protocol.GrantClientCertificate
	case d.credentials.SubjectToken != "":
		return protocol.GrantTokenExchange
	}
	return protocol.GrantPassword
}

// checkCredentials refuses credentials that contradict each other: a
// certificate sign-in takes no password or token, and needs a client
// certificate over TLS.
func (d *databaseImpl) checkCredentials(base *url.URL) error {
	c := d.credentials
	if d.browserSignIn {
		var given []string
		for _, credential := range []struct {
			name, value string
		}{{adbc.OptionKeyUsername, c.Username}, {adbc.OptionKeyPassword, c.Password},
			{OptionSubjectToken, c.SubjectToken}, {OptionAccessToken, c.AccessToken}} {
			if credential.value != "" {
				given = append(given, credential.name)
			}
		}
		if len(given) > 0 {
			return d.ErrorHelper.InvalidArgument("%s=%s signs in through the browser; do not also set %s",
				OptionAuthType, AuthTypeOAuthBrowser, strings.Join(given, ", "))
		}
		return nil
	}
	if !d.credentials.ClientCertificate {
		return nil
	}
	var given []string
	for _, credential := range []struct {
		name, value string
	}{{adbc.OptionKeyUsername, c.Username}, {adbc.OptionKeyPassword, c.Password},
		{OptionSubjectToken, c.SubjectToken}, {OptionAccessToken, c.AccessToken}} {
		if credential.value != "" {
			given = append(given, credential.name)
		}
	}
	if len(given) > 0 {
		return d.ErrorHelper.InvalidArgument("%s=%s signs in with the client certificate alone; do not also set %s",
			OptionAuthType, AuthTypeClientCertificate, strings.Join(given, ", "))
	}
	if d.tls.clientCert == "" || d.tls.clientKey == "" {
		return d.ErrorHelper.InvalidArgument("%s=%s needs a client certificate: set %s and %s",
			OptionAuthType, AuthTypeClientCertificate, OptionTLSClientCert, OptionTLSClientKey)
	}
	if base.Scheme != "https" {
		return d.ErrorHelper.InvalidArgument("%s=%s needs TLS, but the uri %s is plain HTTP",
			OptionAuthType, AuthTypeClientCertificate, base.Redacted())
	}
	return nil
}

func (d *databaseImpl) newTransport(base *url.URL) (*http.Transport, error) {
	transport, err := transportFor(base, d.tls)
	if err != nil {
		return nil, d.ErrorHelper.InvalidArgument("%s", err)
	}
	return transport, nil
}

// transportFor is the HTTP transport for a server at base with these TLS
// options.
func transportFor(base *url.URL, options tlsOptions) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// A split result is read over one connection per partition (§6.10).
	transport.MaxIdleConnsPerHost = maxResultPartitions
	if base.Scheme == "http" {
		if options != (tlsOptions{}) {
			return nil, fmt.Errorf("TLS options were given, but the uri %s is plain HTTP", base.Redacted())
		}
		return transport, nil
	}
	config, err := options.config()
	if err != nil {
		return nil, err
	}
	transport.TLSClientConfig = config
	return transport, nil
}

// config builds the TLS configuration. Certificates are verified unless
// skip_verify is set explicitly.
func (o tlsOptions) config() (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: o.serverName}
	if o.skipVerify {
		config.InsecureSkipVerify = true
	}
	if o.rootCerts != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(o.rootCerts)) {
			return nil, fmt.Errorf("%s holds no PEM certificate", OptionTLSRootCerts)
		}
		config.RootCAs = pool
	}
	if (o.clientCert == "") != (o.clientKey == "") {
		return nil, fmt.Errorf("%s and %s go together", OptionTLSClientCert, OptionTLSClientKey)
	}
	if o.clientCert != "" {
		pair, err := tls.X509KeyPair([]byte(o.clientCert), []byte(o.clientKey))
		if err != nil {
			return nil, fmt.Errorf("client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	return config, nil
}

func (d *databaseImpl) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.transport != nil {
		d.transport.CloseIdleConnections()
	}
	return nil
}
