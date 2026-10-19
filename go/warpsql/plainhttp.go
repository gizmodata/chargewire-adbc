// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"net"
	"net/url"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// plainHTTPError says what to do when a TLS connection (warp://, the default,
// or https://) met a server that speaks plain HTTP, instead of Go's "server
// gave HTTP response to HTTPS client". Only the TLS hello was sent: no request
// and no credential reached the server. The driver never falls back to
// plaintext by itself. It returns nil for any other error.
//
// rawURI is the uri option as given, which says how to turn TLS off: ?tls=false
// for warp:// (and URIs built on it, such as hardcharger://), http:// for
// https://.
func plainHTTPError(err error, rawURI string, base *url.URL) error {
	if err == nil || base.Scheme != "https" || !protocol.SpeaksPlainHTTP(err) {
		return nil
	}
	port := base.Port()
	if port == "" {
		port = "443"
	}
	advice := "Add ?tls=false to the URI"
	if strings.HasPrefix(strings.ToLower(rawURI), "https:") {
		advice = "Use an http:// URI instead of https://"
	}
	return &driverError{
		adbcErr: adbc.Error{Code: adbc.StatusIO, Msg: "[" + driverName + "] The server at " +
			net.JoinHostPort(base.Hostname(), port) + " speaks plain HTTP, not TLS. " + advice +
			" to connect without encryption."},
		cause: err,
	}
}
