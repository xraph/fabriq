// Package client provides a pure DSN (connection-string) parser for fabriq
// remote clients. It has no dependency on the rest of the fabriq module.
package client

import (
	"fmt"
	"net/url"
	"strconv"
)

// DSN represents a parsed fabriq connection string.
type DSN struct {
	Transport string
	TLS       bool
	Host      string
	Port      string
	BasePath  string
	Tenant    string
	Key       string
	Version   int
}

// ParseDSN parses a fabriq connection string of the form:
//
//	fabriq://<key>@<host>[:port][/tenant][?tls=true|false&version=N&basePath=/path]
//	fabriq+grpc://<key>@<host>[:port][/tenant][?tls=true|false&version=N&basePath=/path]
func ParseDSN(s string) (DSN, error) {
	u, err := url.Parse(s)
	if err != nil {
		return DSN{}, fmt.Errorf("client: invalid dsn: %w", err)
	}

	var transport string
	switch u.Scheme {
	case "fabriq":
		transport = "http"
	case "fabriq+grpc":
		transport = "grpc"
	default:
		return DSN{}, fmt.Errorf("client: unsupported dsn scheme %q", u.Scheme)
	}

	if u.User == nil {
		return DSN{}, fmt.Errorf("client: dsn missing key (userinfo)")
	}
	key := u.User.Username()
	if key == "" {
		return DSN{}, fmt.Errorf("client: dsn missing key (userinfo)")
	}

	host := u.Hostname()
	if host == "" {
		return DSN{}, fmt.Errorf("client: dsn missing host")
	}

	query := u.Query()

	// Determine TLS: default off for localhost/127.0.0.1, on otherwise;
	// explicit ?tls= overrides.
	tls := host != "localhost" && host != "127.0.0.1"
	if v := query.Get("tls"); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return DSN{}, fmt.Errorf("client: invalid tls query value %q: %w", v, err)
		}
		tls = parsed
	}

	port := u.Port()
	if port == "" {
		if tls {
			port = "443"
		} else {
			port = "80"
		}
	}

	tenant := ""
	if u.Path != "" && u.Path != "/" {
		tenant = trimLeadingSlash(u.Path)
	}

	version := 1
	if v := query.Get("version"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return DSN{}, fmt.Errorf("client: invalid version query value %q: %w", v, err)
		}
		version = parsed
	}

	basePath := "/admin"
	if v := query.Get("basePath"); v != "" {
		basePath = v
	}

	return DSN{
		Transport: transport,
		TLS:       tls,
		Host:      host,
		Port:      port,
		BasePath:  basePath,
		Tenant:    tenant,
		Key:       key,
		Version:   version,
	}, nil
}

// BaseURL renders the DSN as an HTTP(S) base URL: {http|https}://host:port{basePath}.
func (d DSN) BaseURL() string {
	scheme := "http"
	if d.TLS {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s:%s%s", scheme, d.Host, d.Port, d.BasePath)
}

func trimLeadingSlash(s string) string {
	if s != "" && s[0] == '/' {
		return s[1:]
	}
	return s
}
