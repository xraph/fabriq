package adminapi

import (
	"strings"
	"testing"
)

func TestGenerateKey_ShapeAndHash(t *testing.T) {
	key, prefix, hash, err := generateKey()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, "fq_") {
		t.Fatalf("key %q lacks fq_ prefix", key)
	}
	if prefix != key[:7] {
		t.Fatalf("prefix %q != key[:7] %q", prefix, key[:7])
	}
	if hash != hashKey(key) {
		t.Fatalf("hash mismatch")
	}
	if len(hash) != 64 {
		t.Fatalf("sha256 hex must be 64 chars, got %d", len(hash))
	}
	// uniqueness
	k2, _, _, _ := generateKey()
	if k2 == key {
		t.Fatal("keys must be unique")
	}
}
