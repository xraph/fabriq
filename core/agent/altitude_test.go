package agent

import (
	"encoding/json"
	"testing"
)

func TestResolveAltitude_BudgetDrivesDescent(t *testing.T) {
	if got := resolveAltitude(AltAuto, 100, 1000); got != AltEntity {
		t.Fatalf("generous budget should descend to entities, got %v", got)
	}
	if got := resolveAltitude(AltAuto, 100000, 500); got != AltTenant {
		t.Fatalf("tight budget should climb to tenant, got %v", got)
	}
	if got := resolveAltitude(AltScope, 1, 1); got != AltScope {
		t.Fatalf("explicit altitude must pass through, got %v", got)
	}
}

func TestDedupeByAltitude(t *testing.T) {
	items := []ContextItem{
		{Entity: "note", ID: "n1"},
		{Entity: DigestEntity, ID: TenantRootID(), Row: json.RawMessage(`{"level":2}`)},
	}
	atEntity := dedupeByAltitude(items, AltEntity)
	for _, it := range atEntity {
		if it.Entity == DigestEntity {
			t.Fatal("AltEntity must drop digest items")
		}
	}
	atTenant := dedupeByAltitude(items, AltTenant)
	for _, it := range atTenant {
		if it.Entity == "note" {
			t.Fatal("AltTenant must drop covered entity items when a tenant digest is present")
		}
	}
}

func TestIsDigest(t *testing.T) {
	if !isDigest(DigestEntity) {
		t.Fatal("isDigest should return true for DigestEntity")
	}
	if isDigest("note") {
		t.Fatal("isDigest should return false for non-digest entity")
	}
	if isDigest("") {
		t.Fatal("isDigest should return false for empty string")
	}
}

func TestDigestLevel(t *testing.T) {
	if got := digestLevel(json.RawMessage(`{"level":2}`)); got != 2 {
		t.Fatalf("expected level 2, got %d", got)
	}
	if got := digestLevel(json.RawMessage(`{"level":0}`)); got != 0 {
		t.Fatalf("expected level 0, got %d", got)
	}
	if got := digestLevel(json.RawMessage(`{}`)); got != 0 {
		t.Fatalf("expected level 0 when absent, got %d", got)
	}
	if got := digestLevel(json.RawMessage(nil)); got != 0 {
		t.Fatalf("expected level 0 for nil input, got %d", got)
	}
	if got := digestLevel(json.RawMessage(`not json`)); got != 0 {
		t.Fatalf("expected level 0 for unparseable input, got %d", got)
	}
}

func TestDedupeByAltitude_EdgeCases(t *testing.T) {
	// empty input — must not panic
	out := dedupeByAltitude(nil, AltEntity)
	if out != nil && len(out) != 0 {
		t.Fatalf("expected empty output for nil input, got %v", out)
	}

	// no digests present at AltTenant — leave items as-is
	noDigests := []ContextItem{
		{Entity: "note", ID: "n1"},
		{Entity: "doc", ID: "d1"},
	}
	kept := dedupeByAltitude(noDigests, AltTenant)
	if len(kept) != 2 {
		t.Fatalf("AltTenant with no digests: expected 2 items, got %d", len(kept))
	}

	// AltAuto is a no-op pass-through
	mixed := []ContextItem{
		{Entity: "note", ID: "n1"},
		{Entity: DigestEntity, ID: TenantRootID(), Row: json.RawMessage(`{"level":2}`)},
	}
	passThrough := dedupeByAltitude(mixed, AltAuto)
	if len(passThrough) != len(mixed) {
		t.Fatalf("AltAuto should pass through unchanged, got %d items", len(passThrough))
	}

	// all digests at AltEntity — returns empty (all dropped)
	allDigests := []ContextItem{
		{Entity: DigestEntity, ID: "digest:0:note:n1", Row: json.RawMessage(`{"level":0}`)},
		{Entity: DigestEntity, ID: TenantRootID(), Row: json.RawMessage(`{"level":2}`)},
	}
	out2 := dedupeByAltitude(allDigests, AltEntity)
	if len(out2) != 0 {
		t.Fatalf("AltEntity with all digest items: expected 0 items, got %d", len(out2))
	}
}
