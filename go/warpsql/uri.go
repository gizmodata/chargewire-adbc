// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// SchemeWarp is the driver's URI scheme. A warp:// URI is HTTPS: TLS is on
// unless the URI says ?tls=false.
//
//	warp://host:port                  HTTPS, base path /warp
//	warp://host:port/custom/prefix    HTTPS, base path /custom/prefix
//	warp://host:port?tls=false        plain HTTP (development, loopback)
//	https://host:port[/prefix]        same as warp://
//	http://host:port[/prefix]         plain HTTP
const SchemeWarp = "warp"

// defaultBasePath is where servers mount the protocol unless they say
// otherwise (§2.1).
const defaultBasePath = "/warp"

// parseURI turns a connection URI into the protocol's base URL.
func parseURI(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("a uri is required, e.g. warp://host:port")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid uri %q: %w", raw, err)
	}
	if u.User != nil {
		return nil, fmt.Errorf("invalid uri %q: credentials do not belong in the uri; use the username and password options", u.Redacted())
	}
	query := u.Query()
	switch strings.ToLower(u.Scheme) {
	case SchemeWarp:
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
		return nil, fmt.Errorf("invalid uri %q: the scheme must be warp://, https:// or http://", u.Redacted())
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
