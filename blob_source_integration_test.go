//go:build integration

package fabriq_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// openSatEncTest opens fabriq as the app role with an encryption key configured.
func openSatEncTest(t *testing.T, withKey bool) (*fabriq.Fabriq, *fabriq.Stores) {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close()
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	cfg := fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: appDSN}}
	if withKey {
		key := make([]byte, 32)
		for i := range key {
			key[i] = byte(i + 7)
		}
		cfg.Encryption = fabriq.EncryptionConfig{Key: base64.StdEncoding.EncodeToString(key)}
	}
	f, stores, err := fabriq.Open(ctx, reg, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = stores.Close() })
	return f, stores
}

func TestBlobSourceEncryptedRoundTrip(t *testing.T) {
	ctx := context.Background()
	f, _ := openSatEncTest(t, true)
	tctx := tenant.MustWithTenant(ctx, "acme")

	ref, err := f.CreateSource(tctx, fabriq.SourceInput{
		Name: "primary-s3", Provider: "s3", BasePath: "bucket/data",
		Auth: map[string]any{"accessKey": "AK", "secretKey": "SECRET"}, Enabled: true,
	})
	if err != nil {
		t.Fatalf("CreateSource: %v", err)
	}

	// The stored auth_enc column is ciphertext, not the plaintext secret.
	var enc []byte
	if err := f.Relational().Query(tctx, &enc, `SELECT auth_enc FROM fabriq_blob_sources WHERE id = $1`, ref.ID); err != nil {
		t.Fatalf("reading auth_enc: %v", err)
	}
	if len(enc) == 0 {
		t.Fatal("auth_enc is empty")
	}
	if string(enc) == "SECRET" || contains(enc, "SECRET") {
		t.Fatal("auth stored in plaintext")
	}

	// GetSource decrypts.
	got, err := f.GetSource(tctx, ref.ID)
	if err != nil {
		t.Fatalf("GetSource: %v", err)
	}
	if got.Auth["secretKey"] != "SECRET" || got.Provider != "s3" {
		t.Fatalf("decrypted source wrong: %+v", got)
	}
}

func TestBlobSourceFailClosedWithoutKey(t *testing.T) {
	ctx := context.Background()
	f, _ := openSatEncTest(t, false) // no key
	tctx := tenant.MustWithTenant(ctx, "acme")
	_, err := f.CreateSource(tctx, fabriq.SourceInput{
		Name: "x", Provider: "s3", Auth: map[string]any{"k": "v"},
	})
	if err == nil {
		t.Fatal("CreateSource with auth and no key must fail closed")
	}
}

func contains(b []byte, s string) bool {
	return len(b) >= len(s) && (string(b) == s || indexOf(b, s) >= 0)
}
func indexOf(b []byte, s string) int {
	for i := 0; i+len(s) <= len(b); i++ {
		if string(b[i:i+len(s)]) == s {
			return i
		}
	}
	return -1
}
