// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// BrowserFlow is a browser sign-in flow (§4.3.1). PollSecret collects the
// token: it is never logged, shown or put in a URL.
type BrowserFlow struct {
	FlowID        string `json:"flow_id"`
	PollSecret    string `json:"poll_secret"`
	AuthorizeURL  string `json:"authorize_url"`
	PollIntervalS int64  `json:"poll_interval_s"`
	ExpiresAt     string `json:"expires_at"`
}

// PollInterval is how long to wait between polls (at least a second).
func (f *BrowserFlow) PollInterval() time.Duration {
	return time.Duration(max(f.PollIntervalS, 1)) * time.Second
}

// BrowserFlowState is a poll's answer (§4.3.2).
type BrowserFlowState struct {
	// State is pending, complete or failed, or a state a later version adds,
	// which is not terminal: keep polling (§12).
	State       string `json:"state"`
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	Error       *struct {
		Code   string `json:"code"`
		Detail string `json:"detail"`
	} `json:"error"`
}

// Browser flow states (§4.3.2). Open, like every enumeration (§12).
const (
	FlowPending  = "pending"
	FlowComplete = "complete"
	FlowFailed   = "failed"
)

// StartBrowserSignIn creates a browser sign-in flow (§4.3.1). returnTo is
// for clients in a browser; "" sends none.
func (c *Client) StartBrowserSignIn(ctx context.Context, returnTo string) (*BrowserFlow, error) {
	request := map[string]string{}
	if returnTo != "" {
		request["return_to"] = returnTo
	}
	body, err := JSONBody(request)
	if err != nil {
		return nil, err
	}
	flow := &BrowserFlow{}
	req := &Request{Method: http.MethodPost, Path: "/v1/auth/oauth/flows", Body: body, Anonymous: true}
	if _, err := c.doJSON(ctx, req, flow); err != nil {
		return nil, err
	}
	if flow.FlowID == "" || flow.PollSecret == "" || flow.AuthorizeURL == "" {
		return nil, ProtocolError("the server's browser sign-in answer lacks flow_id, poll_secret or authorize_url")
	}
	return flow, nil
}

// PollBrowserSignIn polls a flow once (§4.3.2). It is sent once, never
// repeated: a complete flow hands its token out a single time.
func (c *Client) PollBrowserSignIn(ctx context.Context, flow *BrowserFlow) (*BrowserFlowState, error) {
	body, err := JSONBody(map[string]string{"poll_secret": flow.PollSecret})
	if err != nil {
		return nil, err
	}
	state := &BrowserFlowState{}
	req := &Request{Method: http.MethodPost, Path: "/v1/auth/oauth/flows/" + url.PathEscape(flow.FlowID),
		Body: body, Anonymous: true, MaxAttempts: 1}
	if _, err := c.doJSON(ctx, req, state); err != nil {
		return nil, err
	}
	if state.State == FlowComplete && (state.AccessToken == "" || !strings.EqualFold(state.TokenType, "bearer")) {
		return nil, ProtocolError("the browser sign-in completed without a bearer token")
	}
	return state, nil
}

// UseAccessToken makes the client send token, one it cannot renew (a browser
// sign-in's): it is sent until the server refuses it. Call it before the
// client is shared.
func (c *Client) UseAccessToken(token string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.token = token
	c.credentials = Credentials{AccessToken: token}
	c.renewAt = time.Time{}
}

// ReplaceAccessToken sends token from now on in place of the token issued
// elsewhere that the client uses. Safe while the client is in use; only for
// a client whose credentials are such a token.
func (c *Client) ReplaceAccessToken(token string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.token = token
	c.renewAt = time.Time{}
}
