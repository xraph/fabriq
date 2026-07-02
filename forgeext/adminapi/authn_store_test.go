//go:build integration

package adminapi

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// newKeyStoreDB spins up a real Postgres, runs the full migration chain (which
// creates fabriq_api_key via 0027), connects as the restricted app role, and
// returns a KeyStore over the borrowed grove handle.
func newKeyStoreDB(t *testing.T) KeyStore {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// Run migrations as the schema owner (superuser).
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = owner.Close()

	// Connect as the restricted app role (fabriq_api_key is not under RLS, so
	// the role can read/write it directly — mirrors production auth lookups).
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	return NewKeyStore(a.Grove())
}

func TestKeyStore_IssueLookupRoundTrip(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	issued, err := ks.Issue(ctx, KeySpec{Label: "ci", TenantID: "acme", CanManageKeys: true})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if issued.ID == "" || issued.Prefix == "" || issued.Key == "" {
		t.Fatalf("Issue returned empty fields: %+v", issued)
	}

	// The plaintext key must round-trip: hashing it and looking that hash up
	// finds the row we just wrote.
	rec, found, err := ks.Lookup(ctx, hashKey(issued.Key))
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !found {
		t.Fatal("Lookup: expected found=true for the issued key")
	}
	if rec.ID != issued.ID {
		t.Fatalf("Lookup rec.ID = %q, want %q", rec.ID, issued.ID)
	}
	if rec.Prefix != issued.Prefix {
		t.Fatalf("Lookup rec.Prefix = %q, want %q", rec.Prefix, issued.Prefix)
	}
	if rec.TenantID != "acme" {
		t.Fatalf("Lookup rec.TenantID = %q, want acme", rec.TenantID)
	}
	if rec.Label != "ci" {
		t.Fatalf("Lookup rec.Label = %q, want ci", rec.Label)
	}
	if !rec.CanManageKeys {
		t.Fatal("Lookup rec.CanManageKeys = false, want true")
	}
	if rec.CreatedAt.IsZero() {
		t.Fatal("Lookup rec.CreatedAt is zero")
	}
	if rec.RevokedAt != nil {
		t.Fatalf("Lookup rec.RevokedAt = %v, want nil", rec.RevokedAt)
	}
}

func TestKeyStore_LookupUnknown(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	_, found, err := ks.Lookup(ctx, "deadbeef-no-such-hash")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if found {
		t.Fatal("Lookup of unknown hash: expected found=false")
	}
}

func TestKeyStore_MultiTenantStoredNull(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	// TenantID == "" -> stored NULL -> read back as "".
	issued, err := ks.Issue(ctx, KeySpec{Label: "global"})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	rec, found, err := ks.Lookup(ctx, hashKey(issued.Key))
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if rec.TenantID != "" {
		t.Fatalf("multi-tenant key TenantID = %q, want empty", rec.TenantID)
	}
	if rec.CanManageKeys {
		t.Fatal("CanManageKeys defaulted true, want false")
	}
}

func TestKeyStore_ListRedactsHash(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	a, err := ks.Issue(ctx, KeySpec{Label: "a", TenantID: "t1"})
	if err != nil {
		t.Fatalf("Issue a: %v", err)
	}
	b, err := ks.Issue(ctx, KeySpec{Label: "b"})
	if err != nil {
		t.Fatalf("Issue b: %v", err)
	}

	recs, err := ks.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("List returned %d records, want 2", len(recs))
	}

	// KeyRecord carries no hash/plaintext field at all — redaction is
	// structural. Assert the issued ids are present and the prefix survives.
	seen := map[string]bool{}
	for _, r := range recs {
		seen[r.ID] = true
		if r.Prefix == "" {
			t.Fatalf("List rec %q has empty Prefix", r.ID)
		}
	}
	if !seen[a.ID] || !seen[b.ID] {
		t.Fatalf("List missing issued ids: %+v", recs)
	}
}

// TestKeyStore_IssueSession_Expires exercises the session-token path end to
// end: a short-TTL session resolves as found with a future ExpiresAt right
// after issue, and a near-immediate-expiry session's ExpiresAt is in the past
// by the time we check it.
func TestKeyStore_IssueSession_Expires(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	issued, err := ks.IssueSession(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	if issued.ID == "" || issued.Prefix == "" || issued.Key == "" {
		t.Fatalf("IssueSession returned empty fields: %+v", issued)
	}

	rec, found, err := ks.Lookup(ctx, hashKey(issued.Key))
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !found {
		t.Fatal("Lookup: expected found=true for the issued session")
	}
	if rec.ExpiresAt == nil {
		t.Fatal("Lookup rec.ExpiresAt is nil, want set")
	}
	if !rec.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("Lookup rec.ExpiresAt = %v, want in the future immediately after issue", rec.ExpiresAt)
	}

	// A session issued with a tiny (already-elapsed by the time we check) TTL
	// must have an ExpiresAt in the past.
	shortLived, err := ks.IssueSession(ctx, 1*time.Nanosecond)
	if err != nil {
		t.Fatalf("IssueSession (short-lived): %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	rec2, found, err := ks.Lookup(ctx, hashKey(shortLived.Key))
	if err != nil {
		t.Fatalf("Lookup (short-lived): %v", err)
	}
	if !found {
		t.Fatal("Lookup (short-lived): expected found=true")
	}
	if rec2.ExpiresAt == nil {
		t.Fatal("Lookup (short-lived) rec.ExpiresAt is nil, want set")
	}
	if !rec2.ExpiresAt.Before(time.Now().UTC()) {
		t.Fatalf("Lookup (short-lived) rec.ExpiresAt = %v, want in the past", rec2.ExpiresAt)
	}
}

func TestKeyStore_RevokeKeepsRowVisible(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	issued, err := ks.Issue(ctx, KeySpec{Label: "revoke-me", TenantID: "t1"})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	if err := ks.Revoke(ctx, issued.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	// Revocation must NOT hide the row from Lookup — it is enforced by the
	// middleware. RevokedAt must be set.
	rec, found, err := ks.Lookup(ctx, hashKey(issued.Key))
	if err != nil {
		t.Fatalf("Lookup after revoke: %v", err)
	}
	if !found {
		t.Fatal("expected found=true after revoke (row stays visible)")
	}
	if rec.RevokedAt == nil {
		t.Fatal("RevokedAt is nil after Revoke")
	}
	if rec.RevokedAt.IsZero() {
		t.Fatal("RevokedAt is zero after Revoke")
	}
}
