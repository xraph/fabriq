package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/xraph/fabriq/core/tenant"
)

// authenticator verifies JWTs and stamps the tenant from VALIDATED claims.
// Tenant context is never derived from a forwarded header: the gateway in
// front of this service routes, it does not authenticate for us.
type authenticator struct {
	secret []byte
}

func newAuthenticator(secret []byte) *authenticator {
	return &authenticator{secret: secret}
}

// Authenticate extracts the bearer token (Authorization header, or the
// token query parameter for EventSource clients, which cannot set
// headers), verifies it, and returns a tenant-stamped context.
func (a *authenticator) Authenticate(r *http.Request) (context.Context, error) {
	raw := ""
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		raw = strings.TrimPrefix(h, "Bearer ")
	} else if q := r.URL.Query().Get("token"); q != "" {
		raw = q
	}
	if raw == "" {
		return nil, fmt.Errorf("missing bearer token")
	}

	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return a.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	tid, _ := claims["tenant_id"].(string)
	if tid == "" {
		return nil, fmt.Errorf("token has no tenant_id claim")
	}
	return tenant.WithTenant(r.Context(), tid)
}
