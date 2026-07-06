package postgres

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/pathctx"
)

func TestLockKeyFor_RoleInHighBits(t *testing.T) {
	if LockKeyRelayFor("tenant_acme")>>32 != LockKeyRelay {
		t.Fatalf("relay role not recoverable: %x", LockKeyRelayFor("tenant_acme"))
	}
	if LockKeyDocumentPlaneFor("tenant_acme")>>32 != LockKeyDocumentPlane {
		t.Fatalf("doc role not recoverable")
	}
	// Same schema, different roles must not collide.
	if LockKeyRelayFor("tenant_acme") == LockKeyDocumentPlaneFor("tenant_acme") {
		t.Fatal("roles collide for the same schema")
	}
}

func TestLockKeyFor_StableAndDistinct(t *testing.T) {
	if LockKeyRelayFor("tenant_a") != LockKeyRelayFor("tenant_a") {
		t.Fatal("unstable key")
	}
	if LockKeyRelayFor("tenant_a") == LockKeyRelayFor("tenant_b") {
		t.Fatal("distinct schemas collide")
	}
}

func TestLockKeyFor_NeverCollidesWithStaticKeys(t *testing.T) {
	statics := []int64{LockKeyRelay, LockKeyReconciler, LockKeyDocumentPlane, LockKeyBlobGC}
	for _, s := range []string{"tenant_a", "tenant_b", "tenant_zzzz", "tenant_acme_corp"} {
		fors := []int64{LockKeyRelayFor(s), LockKeyReconcilerFor(s), LockKeyDocumentPlaneFor(s), LockKeyBlobGCFor(s)}
		for _, f := range fors {
			for _, st := range statics {
				if f == st {
					t.Fatalf("schema key for %q collided with static %d", s, st)
				}
			}
		}
	}
}

func TestMaintenance_LockKeys_SchemaScopedWhenPresent(t *testing.T) {
	m := &Maintenance{}
	// No schema on ctx → static keys (database mode).
	r, d := m.lockKeys(context.Background())
	if r != LockKeyRelay || d != LockKeyDocumentPlane {
		t.Fatalf("database mode should use static keys, got %d/%d", r, d)
	}
	// Schema on ctx → schema-scoped keys.
	ctx := pathctx.MustWithSearchPath(context.Background(), "tenant_acme")
	r, d = m.lockKeys(ctx)
	if r != LockKeyRelayFor("tenant_acme") || d != LockKeyDocumentPlaneFor("tenant_acme") {
		t.Fatalf("schema mode should use schema-scoped keys, got %d/%d", r, d)
	}
}

func TestCrdtDocsRef_SchemaQualifiedWhenPresent(t *testing.T) {
	d := &DocStore{}
	if got := d.crdtDocsRef(context.Background()); got != "fabriq_crdt_docs" {
		t.Fatalf("database mode ref = %q, want bare", got)
	}
	ctx := pathctx.MustWithSearchPath(context.Background(), "tenant_acme")
	if got := d.crdtDocsRef(ctx); got != "tenant_acme.fabriq_crdt_docs" {
		t.Fatalf("schema mode ref = %q, want tenant_acme.fabriq_crdt_docs", got)
	}
}
