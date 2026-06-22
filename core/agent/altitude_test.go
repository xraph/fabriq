package agent

import (
	"encoding/json"
	"testing"
)

func TestResolveAltitude_BudgetDrivesDescent(t *testing.T) {
	if got := resolveAltitude(AltAuto, 100, 0, 1000); got != AltEntity {
		t.Fatalf("generous budget should descend to entities, got %v", got)
	}
	// both entity and backbone exceed the budget → falls to AltTenant.
	if got := resolveAltitude(AltAuto, 100000, 80000, 500); got != AltTenant {
		t.Fatalf("tight budget should climb to tenant, got %v", got)
	}
	if got := resolveAltitude(AltScope, 1, 0, 1); got != AltScope {
		t.Fatalf("explicit altitude must pass through, got %v", got)
	}
}

func TestResolveAltitude_ThreeTier(t *testing.T) {
	// entities don't fit, backbone does → AltScope (the new middle rung).
	if got := resolveAltitude(AltAuto, 100, 30, 50); got != AltScope {
		t.Fatalf("entities>budget, backbone<=budget → AltScope; got %v", got)
	}
	// entities fit → AltEntity.
	if got := resolveAltitude(AltAuto, 40, 30, 50); got != AltEntity {
		t.Fatalf("entities<=budget → AltEntity; got %v", got)
	}
	// neither fits → AltTenant.
	if got := resolveAltitude(AltAuto, 100, 80, 50); got != AltTenant {
		t.Fatalf("neither fits → AltTenant; got %v", got)
	}
	// explicit altitude passes through.
	if got := resolveAltitude(AltEntity, 100, 80, 50); got != AltEntity {
		t.Fatalf("explicit altitude passes through; got %v", got)
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

func TestDigestCovers(t *testing.T) {
	// level 2 (tenant) covers every entity regardless of its row.
	tenant := json.RawMessage(`{"level":2}`)
	if !digestCovers(tenant, ContextItem{Entity: "note", ID: "n1"}) {
		t.Fatal("tenant digest must cover every entity")
	}

	// level 0 (entity) covers exactly its source (sourceKind+sourceId match).
	l0 := json.RawMessage(`{"level":0,"sourceKind":"note","sourceId":"n1"}`)
	if !digestCovers(l0, ContextItem{Entity: "note", ID: "n1"}) {
		t.Fatal("L0 digest must cover its exact source entity")
	}
	if digestCovers(l0, ContextItem{Entity: "note", ID: "n3"}) {
		t.Fatal("L0 digest must not cover a different entity")
	}
	if digestCovers(l0, ContextItem{Entity: "doc", ID: "n1"}) {
		t.Fatal("L0 digest must not cover a different entity kind")
	}

	// level 1 scope covers an entity whose row carries the scopeId as a value.
	scope := json.RawMessage(`{"level":1,"kind":"scope","scopeId":"s1"}`)
	covered := ContextItem{Entity: "note", ID: "n2", Row: json.RawMessage(`{"id":"n2","site_id":"s1"}`)}
	if !digestCovers(scope, covered) {
		t.Fatal("scope digest must cover an entity carrying its scopeId as a field value")
	}
	uncovered := ContextItem{Entity: "note", ID: "n1", Row: json.RawMessage(`{"id":"n1","site_id":"s9"}`)}
	if digestCovers(scope, uncovered) {
		t.Fatal("scope digest must not cover an entity not carrying its scopeId")
	}
	// empty scopeId never covers.
	emptyScope := json.RawMessage(`{"level":1,"kind":"scope","scopeId":""}`)
	if digestCovers(emptyScope, covered) {
		t.Fatal("scope digest with empty scopeId must not cover anything")
	}

	// level 1 cluster never covers (entity rows lack a SemHash bucket).
	cluster := json.RawMessage(`{"level":1,"kind":"cluster"}`)
	if digestCovers(cluster, covered) {
		t.Fatal("cluster digest must not cover entities (no SemHash on entity rows)")
	}

	// unparseable row → not covering.
	if digestCovers(json.RawMessage(`not json`), covered) {
		t.Fatal("unparseable digest row must not cover")
	}
}

func TestDedupeByAltitude_CoverageAware(t *testing.T) {
	scopeDigest := ContextItem{
		Entity: DigestEntity, ID: ScopeID("site", "s1"),
		Row: json.RawMessage(`{"level":1,"kind":"scope","scopeId":"s1"}`),
	}

	// An entity NOT covered by any present digest SURVIVES at AltScope.
	t.Run("uncovered entity survives", func(t *testing.T) {
		items := []ContextItem{
			scopeDigest,
			{Entity: "note", ID: "n1", Row: json.RawMessage(`{"id":"n1","site_id":"s9"}`)},
		}
		out := dedupeByAltitude(items, AltScope)
		if !containsEntity(out, "note", "n1") {
			t.Fatalf("uncovered note must survive at AltScope; got %v", entityIDs(out))
		}
		if !containsEntity(out, DigestEntity, scopeDigest.ID) {
			t.Fatal("digest item must be retained")
		}
	})

	// An entity COVERED by a present scope digest is DROPPED.
	t.Run("covered entity dropped by scope digest", func(t *testing.T) {
		items := []ContextItem{
			scopeDigest,
			{Entity: "note", ID: "n2", Row: json.RawMessage(`{"id":"n2","site_id":"s1"}`)},
		}
		out := dedupeByAltitude(items, AltScope)
		if containsEntity(out, "note", "n2") {
			t.Fatalf("covered note must be dropped at AltScope; got %v", entityIDs(out))
		}
	})

	// A tenant digest covers everything → entity dropped at AltTenant.
	t.Run("tenant digest covers everything", func(t *testing.T) {
		items := []ContextItem{
			{Entity: DigestEntity, ID: TenantRootID(), Row: json.RawMessage(`{"level":2}`)},
			{Entity: "note", ID: "n7", Row: json.RawMessage(`{"id":"n7","site_id":"whatever"}`)},
		}
		out := dedupeByAltitude(items, AltTenant)
		if containsEntity(out, "note", "n7") {
			t.Fatalf("tenant digest must drop every entity at AltTenant; got %v", entityIDs(out))
		}
	})

	// An L0 digest covers its exact source; a different entity survives.
	t.Run("L0 digest covers only its source", func(t *testing.T) {
		items := []ContextItem{
			{Entity: DigestEntity, ID: L0ID("note", "n1"), Row: json.RawMessage(`{"level":0,"sourceKind":"note","sourceId":"n1"}`)},
			{Entity: "note", ID: "n1", Row: json.RawMessage(`{"id":"n1"}`)},
			{Entity: "note", ID: "n3", Row: json.RawMessage(`{"id":"n3"}`)},
		}
		out := dedupeByAltitude(items, AltScope)
		if containsEntity(out, "note", "n1") {
			t.Fatalf("L0 digest must drop its covered source n1; got %v", entityIDs(out))
		}
		if !containsEntity(out, "note", "n3") {
			t.Fatalf("uncovered n3 must survive; got %v", entityIDs(out))
		}
	})

	// A cluster digest alone does not drop an entity.
	t.Run("cluster digest does not prune", func(t *testing.T) {
		items := []ContextItem{
			{Entity: DigestEntity, ID: "digest:1:cluster:abc:0", Row: json.RawMessage(`{"level":1,"kind":"cluster"}`)},
			{Entity: "note", ID: "n5", Row: json.RawMessage(`{"id":"n5","site_id":"s1"}`)},
		}
		out := dedupeByAltitude(items, AltScope)
		if !containsEntity(out, "note", "n5") {
			t.Fatalf("cluster digest must not drop an entity; got %v", entityIDs(out))
		}
	})
}

// containsEntity reports whether items holds an item with the given entity/id.
func containsEntity(items []ContextItem, entity, id string) bool {
	for _, it := range items {
		if it.Entity == entity && it.ID == id {
			return true
		}
	}
	return false
}

// entityIDs renders items as "entity/id" pairs for test failure messages.
func entityIDs(items []ContextItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.Entity+"/"+it.ID)
	}
	return out
}

// TestDigestCovers_Cluster: a flat SimHash cluster digest covers an entity whose
// Bucket falls in its prefix; not one outside; intermediate "#" ids never cover.
func TestDigestCovers_Cluster(t *testing.T) {
	p := ClusterPrefix(^uint64(0), 4) // top-4-bits cluster
	cid := ClusterID(p, 4)
	clusterRow := []byte(`{"id":"` + cid + `","level":1,"kind":"cluster"}`)

	inBucket := ContextItem{Entity: "note", ID: "n1", Bucket: ^uint64(0)} // top 4 bits set
	outBucket := ContextItem{Entity: "note", ID: "n2", Bucket: 0}         // top 4 bits clear

	if !digestCovers(clusterRow, inBucket) {
		t.Fatal("cluster must cover an entity whose Bucket matches its prefix")
	}
	if digestCovers(clusterRow, outBucket) {
		t.Fatal("cluster must NOT cover an entity outside its prefix")
	}
	interRow := []byte(`{"id":"` + cid + `#0000000000000000","level":1,"kind":"cluster"}`)
	if digestCovers(interRow, inBucket) {
		t.Fatal("intermediate (#) cluster ids must never prune")
	}
}

// TestParseClusterID covers the flat form and rejects intermediate/non-cluster ids.
func TestParseClusterID(t *testing.T) {
	p := ClusterPrefix(^uint64(0), 4)
	pr, bits, ok := ParseClusterID(ClusterID(p, 4))
	if !ok || pr != p || bits != 4 {
		t.Fatalf("flat parse wrong: pr=%x bits=%d ok=%v", pr, bits, ok)
	}
	if _, _, ok := ParseClusterID("digest:1:scope:site:s1"); ok {
		t.Fatal("scope id must not parse as cluster")
	}
	if _, _, ok := ParseClusterID(ClusterID(p, 4) + "#abc"); ok {
		t.Fatal("intermediate id must not parse as flat cluster")
	}
}

func TestDedupeByAltitude_EdgeCases(t *testing.T) {
	// empty input — must not panic
	out := dedupeByAltitude(nil, AltEntity)
	if len(out) != 0 {
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
