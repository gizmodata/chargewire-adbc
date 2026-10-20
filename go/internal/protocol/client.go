// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase/gadgets"
)

// Credentials say how the client gets its access token (§4). Exactly one way
// is used: a password grant, a token exchange, a client certificate, or a
// token issued elsewhere.
type Credentials struct {
	// ClientCertificate signs in with the client_certificate grant (§4.4):
	// the TLS client certificate is the credential.
	ClientCertificate bool
	Username          string
	Password          string
	// SubjectToken is an identity provider's token for the token_exchange
	// grant.
	SubjectToken string
	// AccessToken is a Warp SQL access token obtained outside the driver. It
	// is used as is and cannot be renewed.
	AccessToken string
}

// grant is the token-endpoint request these credentials make, or nil when
// they carry a ready access token.
func (c Credentials) grant() map[string]string {
	switch {
	case c.AccessToken != "":
		return nil
	case c.ClientCertificate:
		return map[string]string{"grant_type": GrantClientCertificate}
	case c.SubjectToken != "":
		return map[string]string{"grant_type": GrantTokenExchange, "subject_token": c.SubjectToken}
	default:
		return map[string]string{"grant_type": GrantPassword, "username": c.Username, "password": c.Password}
	}
}

// Config configures a Client.
type Config struct {
	// BaseURL is the protocol's base path, e.g. https://db.example.com/warp
	// (§2.1).
	BaseURL     *url.URL
	HTTPClient  *http.Client
	Credentials Credentials
	// MaxAttempts bounds how often a request that is safe to repeat is sent
	// (first try included).
	MaxAttempts int
	UserAgent   string
	Logger      *slog.Logger
}

// Client speaks the protocol to one server. It is safe for concurrent use;
// every session of an ADBC database shares one Client and its access token.
type Client struct {
	base        *url.URL
	http        *http.Client
	credentials Credentials
	maxAttempts int
	userAgent   string
	logger      *slog.Logger

	tokenMu sync.Mutex
	token   string
	renewAt time.Time // when to replace the token; zero when its lifetime is unknown
	// expiresAt is when the token expires, from the token response's
	// expires_in or the token's own exp claim; zero when unknown.
	expiresAt time.Time
}

// NewClient returns a client for cfg.BaseURL.
func NewClient(cfg Config) *Client {
	client := &Client{
		base:        cfg.BaseURL,
		http:        cfg.HTTPClient,
		credentials: cfg.Credentials,
		maxAttempts: max(cfg.MaxAttempts, 1),
		userAgent:   cfg.UserAgent,
		logger:      cfg.Logger,
		token:       cfg.Credentials.AccessToken,
		expiresAt:   TokenExpiry(cfg.Credentials.AccessToken),
	}
	if client.http == nil {
		client.http = http.DefaultClient
	}
	if client.logger == nil {
		client.logger = slog.New(slog.DiscardHandler)
	}
	return client
}

// BaseURL is the server's base path (§2.1).
func (c *Client) BaseURL() *url.URL {
	u := *c.base
	return &u
}

// Request is one protocol call.
type Request struct {
	Method string
	// Path is relative to the base path, e.g. "/v1/sessions".
	Path   string
	Query  url.Values
	Header http.Header
	Body   *Body
	// Idempotent marks a request that may be sent again when its response
	// is lost or the server asks for a retry: reads, deletes, and writes
	// that carry an Idempotency-Key (§6.7).
	Idempotent bool
	// BaseURL, when set, sends the request to another server of the same
	// deployment (a partition's location, §6.2).
	BaseURL *url.URL
	// Anonymous requests carry no access token (§3, §4.1).
	Anonymous bool
	// Trace, when set, observes the request's progress on the wire.
	Trace *httptrace.ClientTrace
	// MaxAttempts, when 1 or more, bounds how often this request is sent
	// below the client's own bound (best-effort requests such as ending a
	// session when a connection closes).
	MaxAttempts int
}

// Do sends req and returns a 2xx response, whose body the caller closes. An
// error response comes back as a *Problem.
//
// Do retries what is safe to retry: transport failures and 429/502/503/504
// answers to idempotent requests (honouring Retry-After), with backoff and
// jitter. A request refused with invalid_token is sent again once with a
// fresh token, when the credentials can obtain one and the refusal's reason
// allows it (§4.2).
func (c *Client) Do(ctx context.Context, req *Request) (*http.Response, error) {
	backoff := gadgets.Backoff{Start: 200 * time.Millisecond, Max: 5 * time.Second, Mul: 2}
	renewed := false
	for attempt := 1; ; attempt++ {
		token := ""
		if !req.Anonymous {
			var err error
			if token, err = c.accessToken(ctx); err != nil {
				return nil, err
			}
		}
		resp, err := c.send(ctx, req, token)
		maxAttempts := c.maxAttempts
		if req.MaxAttempts > 0 {
			maxAttempts = min(maxAttempts, req.MaxAttempts)
		}
		canRepeat := req.Body.Replayable() && attempt < maxAttempts
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if refusal := tlsAlertProblem(err); refusal != nil {
				// The server refused the client certificate at the handshake
				// (§4.4): that refusal, not an unreachable server.
				refusal.Detail += ": " + err.Error()
				return nil, refusal
			}
			if !req.Idempotent || !canRepeat || permanent(err) {
				return nil, &TransportError{Op: req.Method + " " + req.Path, Err: err}
			}
			c.logger.DebugContext(ctx, "retrying after transport error", "path", req.Path, "attempt", attempt, "error", err)
			if err := gadgets.SleepCtx(ctx, backoff.Next()); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode < 300 {
			return resp, nil
		}
		problem := readProblem(resp)
		switch {
		case problem.Code == CodeInvalidToken && renewable(problem.Reason) && !renewed && !req.Anonymous &&
			c.credentials.grant() != nil && req.Body.Replayable():
			// The token expired or the server forgot it: get a new one and
			// send the request again, once (§4.2). This is not a retry of the
			// operation, which the server refused before running it.
			renewed = true
			if err := c.renewToken(ctx, token); err != nil {
				return nil, err
			}
			attempt--
			continue
		case req.Idempotent && canRepeat && retryableStatus(resp.StatusCode):
			wait := problem.RetryAfter
			if wait == 0 {
				wait = backoff.Next()
			}
			c.logger.DebugContext(ctx, "retrying", "path", req.Path, "attempt", attempt, "status", resp.StatusCode, "code", problem.Code, "wait", wait)
			if err := gadgets.SleepCtx(ctx, wait); err != nil {
				return nil, err
			}
			continue
		}
		return nil, problem
	}
}

// renewable reports an invalid_token reason after which signing in again may
// help (§4.2): none, token_expired, or one this driver doesn't know. After
// token_revoked, credentials_changed or principal_dropped someone withdrew the
// access on purpose, or the old credentials fail too: the refusal and its
// reason go to the caller instead.
func renewable(reason string) bool {
	switch reason {
	case ReasonTokenRevoked, ReasonCredentialsChanged, ReasonPrincipalDropped:
		return false
	}
	return true
}

// permanent reports a transport error that sending again cannot cure: the
// TLS handshake failed on a certificate, or met a plain-HTTP server.
func permanent(err error) bool {
	var verification *tls.CertificateVerificationError
	var alert tls.AlertError
	var authority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	return errors.As(err, &verification) || errors.As(err, &alert) || errors.As(err, &authority) ||
		errors.As(err, &hostname) || errors.As(err, &invalid) || SpeaksPlainHTTP(err)
}

// SpeaksPlainHTTP reports a TLS handshake that the server answered with plain
// HTTP: the client sent only its TLS hello, and nothing else (no request, no
// credentials) reached the server.
func SpeaksPlainHTTP(err error) bool {
	if errors.Is(err, http.ErrSchemeMismatch) {
		return true
	}
	var record tls.RecordHeaderError
	return errors.As(err, &record) && string(record.RecordHeader[:]) == "HTTP/"
}

// retryableStatus is an answer that a repeat of the same request may not get.
func retryableStatus(status int) bool {
	switch status {
	case http.StatusMisdirectedRequest, http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (c *Client) send(ctx context.Context, req *Request, token string) (*http.Response, error) {
	base := c.base
	if req.BaseURL != nil {
		base = req.BaseURL
	}
	target := *base
	target.Path = strings.TrimSuffix(base.Path, "/") + req.Path
	target.RawPath = ""
	target.RawQuery = req.Query.Encode()

	var body io.Reader
	length := int64(0)
	if req.Body != nil {
		body, length = req.Body.Open()
	}
	if req.Trace != nil {
		ctx = httptrace.WithClientTrace(ctx, req.Trace)
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, target.String(), body)
	if err != nil {
		return nil, err
	}
	httpReq.ContentLength = length
	for key, values := range req.Header {
		httpReq.Header[key] = values
	}
	if req.Body != nil {
		httpReq.Header.Set("Content-Type", req.Body.contentType)
	}
	if httpReq.Header.Get("Accept") == "" {
		httpReq.Header.Set("Accept", MediaTypeJSON)
	}
	if token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}
	if c.userAgent != "" {
		httpReq.Header.Set("User-Agent", c.userAgent)
	}
	start := time.Now()
	resp, err := c.http.Do(httpReq)
	if err == nil {
		c.logger.DebugContext(ctx, "warp request", "method", req.Method, "path", req.Path,
			"status", resp.StatusCode, "request_id", resp.Header.Get("Warp-Request-Id"),
			"elapsed", time.Since(start))
	}
	return resp, err
}

// doJSON sends req and decodes a JSON answer into out (when out is non-nil).
func (c *Client) doJSON(ctx context.Context, req *Request, out any) (*http.Response, error) {
	resp, err := c.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp, nil
	}
	return resp, decodeInto(resp, out)
}

// decodeInto decodes a JSON response body into out.
func decodeInto(resp *http.Response, out any) error {
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		op := ""
		if resp.Request != nil {
			op = resp.Request.Method + " " + resp.Request.URL.Path
		}
		return &TransportError{Op: op, Err: fmt.Errorf("decoding the response: %w", err)}
	}
	return nil
}

// TransportError is a request that got no HTTP answer, or an unreadable one.
type TransportError struct {
	Op  string
	Err error
}

func (e *TransportError) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

// Discover reads the discovery document (§3.1) and checks the server speaks
// protocol version 1.
func (c *Client) Discover(ctx context.Context) (*Discovery, error) {
	doc := &Discovery{}
	// The time from the request's last byte to the answer's first is about one
	// round trip: the server answers discovery at once.
	var mu sync.Mutex
	var wrote time.Time
	var rtt time.Duration
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				mu.Lock()
				wrote = time.Now()
				mu.Unlock()
			}
		},
		GotFirstResponseByte: func() {
			mu.Lock()
			if !wrote.IsZero() {
				if d := time.Since(wrote); rtt == 0 || d < rtt {
					rtt = d
				}
			}
			mu.Unlock()
		},
	}
	if _, err := c.doJSON(ctx, &Request{Method: http.MethodGet, Path: "", Idempotent: true, Anonymous: true, Trace: trace}, doc); err != nil {
		return nil, err
	}
	mu.Lock()
	doc.RTT = rtt
	mu.Unlock()
	if doc.Protocol != "warp-sql" {
		return nil, fmt.Errorf("%s does not describe a Warp SQL server (protocol %q)", c.base.Redacted(), doc.Protocol)
	}
	for _, version := range doc.ProtocolVersions {
		if version == "1" {
			return doc, nil
		}
	}
	return nil, fmt.Errorf("the server at %s does not speak Warp SQL version 1 (it offers %v)", c.base.Redacted(), doc.ProtocolVersions)
}

// Authenticate obtains an access token now, so that wrong credentials fail
// at connect time rather than on the first statement.
func (c *Client) Authenticate(ctx context.Context) error {
	if c.credentials.grant() == nil {
		return nil
	}
	return c.renewToken(ctx, "")
}

// accessToken returns a token to send, renewing it first when it is about to
// expire. Renewing a little early keeps a long upload from being refused at
// its end (a proxy that buffers the body checks the token last).
func (c *Client) accessToken(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	token, fresh := c.token, c.tokenFresh()
	c.tokenMu.Unlock()
	if fresh {
		return token, nil
	}
	if c.credentials.grant() == nil {
		return token, nil // a token issued elsewhere: send it, the server decides
	}
	if err := c.renewToken(ctx, token); err != nil {
		return "", err
	}
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.token, nil
}

// currentToken is the token in use (possibly stale).
func (c *Client) currentToken() string {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.token
}

// tokenFresh reports whether the current token can still be sent. The caller
// holds tokenMu.
func (c *Client) tokenFresh() bool {
	return c.token != "" && (c.renewAt.IsZero() || time.Now().Before(c.renewAt))
}

// renewMargin is how long before its expiry a token is replaced, at most.
const renewMargin = 2 * time.Minute

// renewToken replaces stale with a new token, unless another caller already
// did.
func (c *Client) renewToken(ctx context.Context, stale string) error {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != stale && c.tokenFresh() {
		return nil
	}
	grant := c.credentials.grant()
	if grant == nil {
		return errors.New("a new access token is needed, but the driver was given a token issued elsewhere, which it cannot renew")
	}
	body, err := JSONBody(grant)
	if err != nil {
		return err
	}
	answer := &tokenResponse{}
	req := &Request{Method: http.MethodPost, Path: "/v1/auth/token", Body: body, Idempotent: true, Anonymous: true}
	if _, err := c.doJSON(ctx, req, answer); err != nil {
		return err
	}
	if answer.AccessToken == "" || !strings.EqualFold(answer.TokenType, "bearer") {
		return errors.New("the token endpoint did not return a bearer token")
	}
	c.token = answer.AccessToken
	c.renewAt = time.Time{}
	c.expiresAt = TokenExpiry(answer.AccessToken)
	if answer.ExpiresIn > 0 {
		lifetime := time.Duration(answer.ExpiresIn) * time.Second
		now := time.Now()
		c.renewAt = now.Add(lifetime - min(renewMargin, lifetime/4))
		c.expiresAt = now.Add(lifetime)
	}
	return nil
}

// CurrentToken is the access token the client sends, renewed first when it
// is about to expire and the client can renew it, and when it expires (zero
// when unknown). It is a bearer secret.
func (c *Client) CurrentToken(ctx context.Context) (string, time.Time, error) {
	if _, err := c.accessToken(ctx); err != nil {
		return "", time.Time{}, err
	}
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.token, c.expiresAt, nil
}

// TokenExpiry is the exp claim of a token that is a JWT, or zero. The token
// is not verified: the time only informs the caller (the server decides).
func TokenExpiry(token string) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		Exp json.Number `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == "" {
		return time.Time{}
	}
	seconds, err := claims.Exp.Float64()
	if err != nil || seconds <= 0 || seconds > 1<<40 {
		return time.Time{}
	}
	return time.Unix(int64(seconds), 0)
}
