// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"net/http"
	"strings"
)

// Dialect is another spelling of protocol version 1 that a server may speak:
// the same requests and documents under another discovery name, base path,
// header prefix and error type prefix. A Client speaks the current spelling
// (ProtocolName, HeaderPrefix, ErrorTypePrefix) unless discovery settles on a
// Dialect (Config.Fallback); it then translates every request and response at
// the wire, so nothing above it sees the difference.
type Dialect struct {
	// Protocol is the discovery document's "protocol".
	Protocol string
	// Root is the base path the server mounts the protocol under.
	Root string
	// HeaderPrefix replaces HeaderPrefix in header names, e.g. "X-".
	HeaderPrefix string
	// ErrorTypePrefix replaces ErrorTypePrefix in problem types.
	ErrorTypePrefix string
}

// The current spelling.
const (
	ProtocolName    = "chargewire"
	HeaderPrefix    = "Chargewire-"
	ErrorTypePrefix = "urn:chargewire:error:"
)

// requestHeaders renames the current spelling's request headers to d's.
func (d *Dialect) requestHeaders(header http.Header) {
	renameHeaders(header, HeaderPrefix, d.HeaderPrefix)
}

// responseHeaders renames d's response headers to the current spelling.
func (d *Dialect) responseHeaders(header http.Header) {
	renameHeaders(header, d.HeaderPrefix, HeaderPrefix)
}

// problem rewrites d's problem type to the current spelling.
func (d *Dialect) problem(p *Problem) {
	if rest, ok := strings.CutPrefix(p.Type, d.ErrorTypePrefix); ok {
		p.Type = ErrorTypePrefix + rest
	}
}

func renameHeaders(header http.Header, from, to string) {
	from = http.CanonicalHeaderKey(from)
	for key, values := range header {
		if rest, ok := strings.CutPrefix(http.CanonicalHeaderKey(key), from); ok {
			delete(header, key)
			header[http.CanonicalHeaderKey(to+rest)] = values
		}
	}
}
