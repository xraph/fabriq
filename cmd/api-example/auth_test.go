package main

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/xraph/fabriq/core/tenant"
)

func signToken(t *testing.T, secret []byte, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAuthenticate_ValidTokenStampsTenant(t *testing.T) {
	secret := []byte("test-secret")
	a := newAuthenticator(secret)
	token := signToken(t, secret, jwt.MapClaims{"tenant_id": "acme", "exp": time.Now().Add(time.Hour).Unix()})

	r := httptest.NewRequest("GET", "/api/v1/assets", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	ctx, err := a.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	tid, err := tenant.FromContext(ctx)
	if err != nil || tid != "acme" {
		t.Fatalf("tenant = (%q, %v)", tid, err)
	}
}

func TestAuthenticate_TokenQueryParamForEventSource(t *testing.T) {
	secret := []byte("test-secret")
	a := newAuthenticator(secret)
	token := signToken(t, secret, jwt.MapClaims{"tenant_id": "acme"})

	r := httptest.NewRequest("GET", "/api/v1/subscribe?token="+token, nil)
	ctx, err := a.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if tid, _ := tenant.FromContext(ctx); tid != "acme" {
		t.Fatalf("tenant = %q", tid)
	}
}

func TestAuthenticate_Rejections(t *testing.T) {
	secret := []byte("test-secret")
	a := newAuthenticator(secret)

	cases := []struct {
		name  string
		setup func(r *httptest.ResponseRecorder) string // returns token
	}{}
	_ = cases

	t.Run("missing token", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/", nil)
		if _, err := a.Authenticate(r); err == nil {
			t.Fatal("missing token must fail")
		}
	})
	t.Run("wrong secret", func(t *testing.T) {
		bad := signToken(t, []byte("other-secret"), jwt.MapClaims{"tenant_id": "acme"})
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+bad)
		if _, err := a.Authenticate(r); err == nil {
			t.Fatal("token signed with the wrong secret must fail")
		}
	})
	t.Run("expired", func(t *testing.T) {
		expired := signToken(t, secret, jwt.MapClaims{"tenant_id": "acme", "exp": time.Now().Add(-time.Hour).Unix()})
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+expired)
		if _, err := a.Authenticate(r); err == nil {
			t.Fatal("expired token must fail")
		}
	})
	t.Run("no tenant claim", func(t *testing.T) {
		tok := signToken(t, secret, jwt.MapClaims{"sub": "user1"})
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+tok)
		if _, err := a.Authenticate(r); err == nil {
			t.Fatal("token without tenant_id must fail")
		}
	})
	t.Run("alg none rejected", func(t *testing.T) {
		// Manually crafted unsigned token (alg=none attack).
		unsigned := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJ0ZW5hbnRfaWQiOiJhY21lIn0."
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+unsigned)
		if _, err := a.Authenticate(r); err == nil {
			t.Fatal("alg=none must fail")
		}
	})
	t.Run("forwarded header is never trusted", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("X-Tenant-ID", "evil") // gateways must not be able to inject
		if _, err := a.Authenticate(r); err == nil {
			t.Fatal("forwarded tenant header must not authenticate")
		}
	})
}
