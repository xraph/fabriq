package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/xraph/fabriq/core/tenant"
)

// TestDevAuthenticator_StampsBearerAsTenant proves the dev authenticator maps the
// bearer token straight to the tenant id (dev shortcut) and stamps it so the
// facade sees it.
func TestDevAuthenticator_StampsBearerAsTenant(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer acme"))

	out, err := devAuthenticator()(ctx)
	if err != nil {
		t.Fatalf("devAuthenticator: %v", err)
	}
	got, err := tenant.Require(out)
	if err != nil {
		t.Fatalf("tenant.Require: %v", err)
	}
	if got != "acme" {
		t.Fatalf("tenant = %q, want acme", got)
	}
}

// TestDevAuthenticator_RejectsMissingBearer proves a call with no credential is
// rejected rather than reaching the facade tenant-less.
func TestDevAuthenticator_RejectsMissingBearer(t *testing.T) {
	if _, err := devAuthenticator()(context.Background()); err == nil {
		t.Fatal("devAuthenticator with no credential = nil error, want rejection")
	}
}
