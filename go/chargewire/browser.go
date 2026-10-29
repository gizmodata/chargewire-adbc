// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/chargewire-adbc/go/internal/protocol"
)

// Browser sign-in (spec §4.3): the driver asks the server for a sign-in flow,
// opens its authorize_url in a browser, where the person signs in with the
// server's identity provider, and polls the flow with its secret until the
// server hands out its access token (once). The identity provider's settings
// stay on the server; the driver never sees them.

// DefaultBrowserSignInTimeout is how long a browser sign-in may take by
// default.
const DefaultBrowserSignInTimeout = 300 * time.Second

// BrowserOpener shows authorizeURL to the person: it opens a browser, or
// prints the URL. It must not block until the sign-in is done.
type BrowserOpener func(ctx context.Context, authorizeURL string) error

// SystemBrowser returns the opener for OptionOAuthBrowser's value: "" opens
// the system's browser (open, xdg-open, rundll32), "none" prints the URL to
// stderr, anything else is a command run with the URL as its last argument.
// When a browser can't be started, the URL is printed instead.
func SystemBrowser(command string) BrowserOpener {
	return func(_ context.Context, authorizeURL string) error {
		if command == BrowserNone {
			printSignInURL(authorizeURL)
			return nil
		}
		if err := startBrowser(command, authorizeURL); err != nil {
			printSignInURL(authorizeURL)
			return nil
		}
		fmt.Fprintln(os.Stderr, "Waiting for the browser sign-in...")
		return nil
	}
}

func printSignInURL(authorizeURL string) {
	fmt.Fprintf(os.Stderr, "Open this URL in a browser to sign in:\n%s\n", authorizeURL)
}

// startBrowser runs command (or the system's opener) with url, without
// waiting for it: a browser started by its command often runs until closed.
func startBrowser(command, url string) error {
	var args []string
	switch {
	case command != "":
		args = strings.Fields(command)
	case runtime.GOOS == "darwin":
		args = []string{"open"}
	case runtime.GOOS == "windows":
		args = []string{"rundll32", "url.dll,FileProtocolHandler"}
	default:
		args = []string{"xdg-open"}
	}
	cmd := exec.Command(args[0], append(args[1:], url)...) //nolint:gosec // the user's own browser command
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

// BrowserSignInResult is what a browser sign-in yields.
type BrowserSignInResult struct {
	// AccessToken is the server's own access token (§4.1): set it as
	// OptionAccessToken. It cannot be renewed; sign in again once it expires.
	AccessToken string
	// ExpiresIn is the token's lifetime (0: the server didn't say).
	ExpiresIn time.Duration
	// Provider is the identity provider's name the server gives, or "".
	Provider string
}

// BrowserSignIn runs a browser sign-in against the server the options name:
// the uri and the adbc.chargewire.tls.* options, as for a database. It fails with
// StatusUnauthenticated when the server doesn't offer browser sign-in, the
// person or the identity provider refused, or the server refused who signed
// in (with the server's detail); StatusTimeout after timeout (0: the
// default); StatusCancelled when ctx is done.
func BrowserSignIn(ctx context.Context, options map[string]string, open BrowserOpener,
	timeout time.Duration) (*BrowserSignInResult, error) {
	options = currentOptions(options)
	base, err := parseURI(options[adbc.OptionKeyURI])
	if err != nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: fmt.Sprintf("[%s] %v", driverName, err)}
	}
	var tls tlsOptions
	for key, value := range options {
		switch key {
		case OptionTLSSkipVerify:
			if tls.skipVerify, err = parseBool(key, value); err != nil {
				return nil, err
			}
		case OptionTLSRootCerts:
			tls.rootCerts = value
		case OptionTLSClientCert:
			tls.clientCert = value
		case OptionTLSClientKey:
			tls.clientKey = value
		case OptionTLSServerName:
			tls.serverName = value
		}
	}
	transport, err := transportFor(base, tls)
	if err != nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: fmt.Sprintf("[%s] %v", driverName, err)}
	}
	defer transport.CloseIdleConnections()
	client := protocol.NewClient(protocol.Config{BaseURL: base, HTTPClient: &http.Client{Transport: transport},
		MaxAttempts: defaultMaxAttempts, UserAgent: userAgent()})
	discovery, err := client.Discover(ctx)
	if err != nil {
		if hint := plainHTTPError(err, options[adbc.OptionKeyURI], base); hint != nil {
			return nil, hint
		}
		return nil, wrapError(err, "connecting to "+base.Redacted())
	}
	return runBrowserSignIn(ctx, client, discovery, open, timeout)
}

// browserPollInterval is how long to wait between polls (tests shorten it).
var browserPollInterval = func(flow *protocol.BrowserFlow) time.Duration { return flow.PollInterval() }

// runBrowserSignIn runs the flow with a client that has discovered its server.
func runBrowserSignIn(ctx context.Context, client *protocol.Client, discovery *protocol.Discovery,
	open BrowserOpener, timeout time.Duration) (*BrowserSignInResult, error) {
	if !discovery.Has(protocol.CapabilityOAuthBrowser) {
		return nil, adbc.Error{Code: adbc.StatusUnauthenticated,
			Msg: fmt.Sprintf("[%s] the server does not offer browser sign-in (capability %s); sign in another way",
				driverName, protocol.CapabilityOAuthBrowser)}
	}
	if timeout <= 0 {
		timeout = DefaultBrowserSignInTimeout
	}
	if open == nil {
		open = SystemBrowser("")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	failed := func(err error) error {
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return adbc.Error{Code: adbc.StatusTimeout,
				Msg: fmt.Sprintf("[%s] browser sign-in: not completed within %s", driverName, timeout)}
		case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
			return adbc.Error{Code: adbc.StatusCancelled, Msg: fmt.Sprintf("[%s] browser sign-in: cancelled", driverName)}
		case protocol.HasCode(err, protocol.CodeFlowNotFound):
			return wrapError(err, "browser sign-in (it expired or was already collected)")
		}
		return wrapError(err, "browser sign-in")
	}

	flow, err := client.StartBrowserSignIn(ctx, "")
	if err != nil {
		return nil, failed(err)
	}
	if err := open(ctx, flow.AuthorizeURL); err != nil {
		return nil, failed(fmt.Errorf("opening the sign-in page: %w", err))
	}
	for {
		select {
		case <-ctx.Done():
			return nil, failed(ctx.Err())
		case <-time.After(browserPollInterval(flow)):
		}
		state, err := client.PollBrowserSignIn(ctx, flow)
		if err != nil {
			return nil, failed(err)
		}
		switch state.State {
		case protocol.FlowComplete:
			return &BrowserSignInResult{AccessToken: state.AccessToken,
				ExpiresIn: time.Duration(state.ExpiresIn) * time.Second,
				Provider:  discovery.BrowserSignInProvider()}, nil
		case protocol.FlowFailed:
			return nil, flowFailure(state)
		}
		// pending, or a state a later version adds: not terminal, keep
		// polling (§12).
	}
}

// flowFailure is a failed flow's error (§4.3.2). Its code came inside a 200
// poll answer, so the error carries no HTTP status. Whatever the code (an
// unknown one is a generic failure, §12), the sign-in failed: the status is
// StatusUnauthenticated.
func flowFailure(state *protocol.BrowserFlowState) error {
	problem := &protocol.Problem{Code: protocol.CodeClientSignInFailed,
		Detail: "the sign-in failed, and the server gave no reason"}
	if state.Error != nil {
		if state.Error.Code != "" {
			problem.Code = state.Error.Code
		}
		if state.Error.Detail != "" {
			problem.Detail = state.Error.Detail
		}
	}
	err := wrapError(problem, "browser sign-in")
	if wrapped, ok := err.(*driverError); ok {
		wrapped.adbcErr.Code = adbc.StatusUnauthenticated
	}
	return err
}
