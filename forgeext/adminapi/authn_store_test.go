//go:build integration

package adminapi

import (
	"context"
	"errors"
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
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
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

// listedLastUsed returns the LastUsedAt List reports for key id.
func listedLastUsed(t *testing.T, ks KeyStore, id string) *time.Time {
	t.Helper()
	recs, err := ks.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for i := range recs {
		if recs[i].ID == id {
			return recs[i].LastUsedAt
		}
	}
	t.Fatalf("key %s not in List", id)
	return nil
}

// TestKeyStore_TouchSetsLastUsedAt checks the optional KeyToucher capability
// on the real store: a fresh key has no last use, Touch stamps it, a later
// Touch moves it forward, an earlier one cannot move it back, and an unknown
// id is not an error.
func TestKeyStore_TouchSetsLastUsedAt(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)
	toucher, ok := ks.(KeyToucher)
	if !ok {
		t.Fatalf("%T does not implement KeyToucher", ks)
	}

	issued, err := ks.Issue(ctx, KeySpec{Label: "touch-me"})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if got := listedLastUsed(t, ks, issued.ID); got != nil {
		t.Fatalf("LastUsedAt = %v on a never-used key, want nil", got)
	}

	first := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	if err := toucher.Touch(ctx, issued.ID, first); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if got := listedLastUsed(t, ks, issued.ID); got == nil || !got.Equal(first) {
		t.Fatalf("LastUsedAt = %v, want %v", got, first)
	}

	second := first.Add(2 * time.Minute)
	if err := toucher.Touch(ctx, issued.ID, second); err != nil {
		t.Fatalf("second Touch: %v", err)
	}
	if got := listedLastUsed(t, ks, issued.ID); got == nil || !got.Equal(second) {
		t.Fatalf("LastUsedAt = %v, want %v", got, second)
	}

	// A late touch (an older timestamp) must not move the value back.
	if err := toucher.Touch(ctx, issued.ID, first); err != nil {
		t.Fatalf("late Touch: %v", err)
	}
	if got := listedLastUsed(t, ks, issued.ID); got == nil || !got.Equal(second) {
		t.Fatalf("LastUsedAt = %v after a late touch, want it to stay %v", got, second)
	}

	if err := toucher.Touch(ctx, "no-such-key", second); err != nil {
		t.Fatalf("Touch of an unknown id = %v, want nil", err)
	}
}

// TestKeyStore_WorksBeforeLastUsedMigration pins the rollout order: a new
// binary may run ahead of `fabriq migrate up`. On a database without
// migration 0035 a valid key must still authenticate (Lookup), List must still
// work with no LastUsedAt, and Touch must fail without taking anything down.
func TestKeyStore_WorksBeforeLastUsedMigration(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() }) // stays open: the test rolls back through it
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	ks := NewKeyStore(a.Grove())

	issued, err := ks.Issue(ctx, KeySpec{Label: "pre-0035", CanManageKeys: true})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Roll back, one migration at a time and bounded, until last_used_at is
	// gone, so the test does not depend on how many migrations follow 0035.
	hasColumn := func() bool {
		return len(fabriqtest.QueryStrings(t, superDSN,
			`SELECT column_name FROM information_schema.columns
			 WHERE table_name = 'fabriq_api_key' AND column_name = 'last_used_at'`)) > 0
	}
	for i := 0; hasColumn(); i++ {
		if i == 20 {
			t.Fatal("last_used_at still present after 20 rollbacks")
		}
		rb, err := orch.Rollback(ctx)
		if err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if len(rb.Rollback) == 0 {
			t.Fatal("ran out of migrations before last_used_at was dropped")
		}
	}

	rec, found, err := ks.Lookup(ctx, hashKey(issued.Key))
	if err != nil || !found {
		t.Fatalf("Lookup without the column: found=%v err=%v", found, err)
	}
	if rec.ID != issued.ID || !rec.CanManageKeys {
		t.Fatalf("Lookup returned %+v, want the issued key", rec)
	}

	recs, err := ks.List(ctx)
	if err != nil {
		t.Fatalf("List without the column: %v", err)
	}
	var seen bool
	for _, r := range recs {
		if r.ID == issued.ID {
			seen = true
		}
		if r.LastUsedAt != nil {
			t.Errorf("key %s LastUsedAt = %v, want nil before the migration", r.ID, r.LastUsedAt)
		}
	}
	if !seen {
		t.Fatalf("issued key %s missing from List", issued.ID)
	}

	if err := ks.(KeyToucher).Touch(ctx, issued.ID, time.Now()); err == nil {
		t.Error("Touch without the column succeeded, want an error the middleware drops")
	}
	if _, found, err := ks.Lookup(ctx, hashKey(issued.Key)); err != nil || !found {
		t.Fatalf("Lookup after a failed Touch: found=%v err=%v", found, err)
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

func TestKeyStore_RevokeKeepsFirstRevokedAt(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	issued, err := ks.Issue(ctx, KeySpec{Label: "revoke-twice", TenantID: "t1"})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := ks.Revoke(ctx, issued.ID); err != nil {
		t.Fatalf("Revoke #1: %v", err)
	}
	first, _, err := ks.Lookup(ctx, hashKey(issued.Key))
	if err != nil {
		t.Fatalf("Lookup #1: %v", err)
	}
	if first.RevokedAt == nil {
		t.Fatal("RevokedAt is nil after the first Revoke")
	}

	time.Sleep(10 * time.Millisecond) // a rewritten timestamp would differ
	if err := ks.Revoke(ctx, issued.ID); err != nil {
		t.Fatalf("Revoke #2: %v (a repeat revoke must succeed)", err)
	}
	second, _, err := ks.Lookup(ctx, hashKey(issued.Key))
	if err != nil {
		t.Fatalf("Lookup #2: %v", err)
	}
	if second.RevokedAt == nil || !second.RevokedAt.Equal(*first.RevokedAt) {
		t.Fatalf("RevokedAt moved from %v to %v on a repeat revoke", first.RevokedAt, second.RevokedAt)
	}
}

func TestKeyStore_RevokeUnknown_ErrKeyNotFound(t *testing.T) {
	ctx := context.Background()
	ks := newKeyStoreDB(t)

	err := ks.Revoke(ctx, "no-such-key-id")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Revoke(unknown) = %v, want ErrKeyNotFound", err)
	}
}
