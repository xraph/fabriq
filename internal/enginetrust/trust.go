// Package enginetrust validates inline trust for projection engine clients.
package enginetrust

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	maxCABytes = 64 << 10
	maxCACerts = 8
	certHeader = "-----BEGIN CERTIFICATE-----"
)

var dnsLabel = regexp.MustCompile(`^[a-zA-Z0-9_](?:[a-zA-Z0-9_-]{0,61}[a-zA-Z0-9_])?$`)

// FalkorTLS validates FalkorDB's mode, address and optional CA. A nil
// config uses plaintext.
func FalkorTLS(addr, mode string, ca []byte) (*tls.Config, error) {
	return RESPTLS("falkordb", addr, mode, ca)
}

// RedisTLS validates the Redis stream transport's mode, address and
// optional CA. A nil config uses plaintext.
func RedisTLS(addr, mode string, ca []byte) (*tls.Config, error) {
	return RESPTLS("redis", addr, mode, ca)
}

// RESPTLS validates TLS settings for a RESP engine reached through go-redis.
// engine names it in errors, which never repeat a configured value. mode is
// empty or disable for plaintext, or verify-full to check the certificate
// and the endpoint name, against ca when given and the system roots when
// not. A nil config uses plaintext.
func RESPTLS(engine, addr, mode string, ca []byte) (*tls.Config, error) {
	switch mode {
	case "", "disable":
		if len(ca) != 0 {
			return nil, fmt.Errorf("fabriq: %s CA requires verify-full TLS", engine)
		}
		if mode == "" {
			return nil, nil
		}
		// go-redis uses absolute paths for plaintext Unix sockets.
		if strings.HasPrefix(addr, "/") && len(addr) > 1 && !strings.ContainsRune(addr, 0) {
			return nil, nil
		}
	case "verify-full":
	default:
		return nil, fmt.Errorf("fabriq: %s TLS mode must be disable or verify-full", engine)
	}
	host, valid := socketHost(addr)
	if !valid {
		return nil, fmt.Errorf("fabriq: %s TLS settings require a valid endpoint", engine)
	}
	if mode == "disable" {
		return nil, nil
	}

	cfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	if len(ca) != 0 {
		certs, ok := parseCertificates(ca)
		if !ok {
			return nil, fmt.Errorf("fabriq: %s CA requires a valid certificate PEM bundle", engine)
		}
		cfg.RootCAs = x509.NewCertPool()
		for _, cert := range certs {
			cfg.RootCAs.AddCert(cert)
		}
	}
	return cfg, nil
}

// TLSDialer returns a go-redis dialer that completes the TLS handshake under
// the caller's context, so cancelling a probe or closing the client stops a
// stalled handshake instead of waiting for it.
func TLSDialer(cfg *tls.Config, timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := tls.Dialer{
		NetDialer: &net.Dialer{Timeout: timeout, KeepAlive: 5 * time.Minute},
		Config:    cfg,
	}
	return dialer.DialContext
}

// ValidateElasticsearch checks that an inline CA has only HTTPS endpoints and valid PEM.
func ValidateElasticsearch(addrs []string, ca []byte) error {
	if len(ca) == 0 {
		return nil
	}
	errHTTPS := errors.New("fabriq: elasticsearch CA requires valid HTTPS addresses")
	if len(addrs) == 0 {
		return errHTTPS
	}
	for _, addr := range addrs {
		u, err := url.Parse(addr)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
			u.RawQuery != "" || u.ForceQuery || strings.Contains(addr, "#") || strings.HasSuffix(u.Host, ":") {
			return errHTTPS
		}
		if port := u.Port(); port != "" && !validPort(port) {
			return errHTTPS
		}
	}
	if _, ok := parseCertificates(ca); !ok {
		return errors.New("fabriq: elasticsearch CA requires a valid certificate PEM bundle")
	}
	return nil
}

func socketHost(addr string) (string, bool) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || !validPort(port) || strings.ContainsAny(host, "\x00 \t\r\n") {
		return "", false
	}
	if ip, ipErr := netip.ParseAddr(host); ipErr == nil {
		return ip.WithZone("").String(), true
	}
	name := strings.TrimSuffix(host, ".")
	if len(name) > 253 || strings.HasPrefix(addr, "[") {
		return "", false
	}
	for label := range strings.SplitSeq(name, ".") {
		if !dnsLabel.MatchString(label) {
			return "", false
		}
	}
	return host, true
}

func validPort(value string) bool {
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && port > 0
}

func parseCertificates(raw []byte) ([]*x509.Certificate, bool) {
	if len(raw) > maxCABytes {
		return nil, false
	}
	data := bytes.TrimSpace(raw)
	certs := make([]*x509.Certificate, 0, maxCACerts)
	for len(data) > 0 {
		if len(certs) == maxCACerts || !bytes.HasPrefix(data, []byte(certHeader)) {
			return nil, false
		}
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, false
		}
		// pem.Decode can skip a malformed block on its way to a valid one.
		// Only the header at the start may occur in the consumed bytes.
		consumed := len(data) - len(rest)
		if bytes.Contains(data[len(certHeader):consumed], []byte("-----BEGIN ")) {
			return nil, false
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, false
		}
		certs = append(certs, cert)

		data = bytes.TrimLeftFunc(rest, unicode.IsSpace)
		spaces := len(rest) - len(data)
		// Every later header must start at a line boundary so the native
		// Elasticsearch trust option reads every certificate we validated.
		if len(data) > 0 && spaces > 0 && rest[spaces-1] != '\n' {
			return nil, false
		}
	}
	return certs, len(certs) > 0
}
