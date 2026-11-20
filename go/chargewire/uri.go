// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// SchemeChargewire is the driver's URI scheme. A chargewire:// URI is HTTPS: TLS is on
// unless the URI says ?tls=false.
//
//	chargewire://host:port                  HTTPS, base path /chargewire
//	chargewire://host:port/custom/prefix    HTTPS, base path /custom/prefix
//	chargewire://host:port?tls=false        plain HTTP (development, loopback)
//	https://host:port[/prefix]        same as chargewire://
//	http://host:port[/prefix]         plain HTTP
const SchemeChargewire = "chargewire"

// defaultBasePath is where servers mount the protocol unless they say
// otherwise (§2.1).
const defaultBasePath = "/chargewire"

// parseURI turns a connection URI into the protocol's base URL.
func parseURI(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("a uri is required, e.g. chargewire://host:port")
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Neither the uri nor Go's error (which quotes it, or the bad escape)
		// goes into the message: a uri that doesn't parse may hold a password.
		return nil, fmt.Errorf("invalid uri: %s", URIParseProblem(err))
	}
	if u.User != nil {
		return nil, fmt.Errorf("invalid uri %q: credentials do not belong in the uri; use the username and password options", u.Redacted())
	}
	query := u.Query()
	switch strings.ToLower(currentScheme(u.Scheme)) {
	case SchemeChargewire:
		u.Scheme = "https"
		if value := query.Get("tls"); value != "" {
			tls, err := parseBool("uri parameter tls", value)
			if err != nil {
				return nil, err
			}
			if !tls {
				u.Scheme = "http"
			}
		}
		query.Del("tls")
	case "https", "http":
		u.Scheme = strings.ToLower(u.Scheme)
	default:
		return nil, fmt.Errorf("invalid uri %q: the scheme must be chargewire://, https:// or http://", u.Redacted())
	}
	if len(query) > 0 {
		return nil, fmt.Errorf("invalid uri %q: unknown parameter(s) %s", u.Redacted(), strings.Join(sortedKeys(query), ", "))
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("invalid uri %q: no host", u.Redacted())
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = strings.TrimSuffix(u.Path, "/")
	if u.Path == "" {
		u.Path = defaultBasePath
	}
	u.Path = currentBasePath(u.Path)
	u.RawPath = ""
	return u, nil
}

func sortedKeys(values url.Values) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// URIParseProblem describes why url.Parse refused a uri without quoting any
// of it: a uri that doesn't parse may hold a password (a raw % in it is
// enough), and Go's error quotes the uri or the bad escape. For drivers
// built on this one too.
func URIParseProblem(err error) string {
	text := err.Error()
	switch {
	case strings.Contains(text, "invalid URL escape"):
		return "a % that doesn't start a two-digit hex escape (write a literal % as %25)"
	case strings.Contains(text, "invalid port"):
		return "the port is not a number"
	case strings.Contains(text, "invalid control character"):
		return "it contains a control character"
	case strings.Contains(text, "missing protocol scheme"), strings.Contains(text, "first path segment in URL cannot contain colon"):
		return "it has no scheme, e.g. chargewire://host:port"
	case strings.Contains(text, "invalid character"):
		return "it contains a character a host name can't have"
	}
	return "it is not a valid URL"
}
