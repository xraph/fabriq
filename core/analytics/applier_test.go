package analytics_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

type widget struct {
	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id"`
	Version  int64  `grove:"version"`
	Name     string `grove:"name"`
	SSN      string `grove:"ssn"`
}

func regWith(spec *registry.AnalyticsSpec) *registry.Registry {
	r := registry.New()
	_ = r.Register(registry.EntitySpec{Name: "widget", Kind: registry.KindAggregate, Model: widget{}, Analytics: spec})
	return r
}

func env(typ string, v int64, payload string) event.Envelope {
	return event.Envelope{
		TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: v, Type: typ,
		At: time.Unix(100, 0).UTC(), Payload: json.RawMessage(payload),
	}
}

func TestApply_DenyByDefault(t *testing.T) {
	a := analytics.NewApplier(regWith(nil))
	_, _, ok, err := a.Apply(env("widget.updated", 1, `{"name":"a","ssn":"secret"}`))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if ok {
		t.Fatal("unmarked entity should produce no records")
	}
}

func TestApply_IncludeProjectsAllowedFields(t *testing.T) {
	a := analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}}))
	fact, ev, ok, err := a.Apply(env("widget.updated", 3, `{"name":"a","ssn":"secret"}`))
	if err != nil || !ok {
		t.Fatalf("apply ok=%v err=%v", ok, err)
	}
	var got map[string]any
	_ = json.Unmarshal(fact.Payload, &got)
	if _, leaked := got["ssn"]; leaked {
		t.Fatalf("ssn leaked into fact payload: %s", fact.Payload)
	}
	if got["name"] != "a" {
		t.Fatalf("name missing: %s", fact.Payload)
	}
	if fact.Version != 3 || ev.Version != 3 || ev.Type != "widget.updated" {
		t.Fatalf("bad record metadata: %+v %+v", fact, ev)
	}
}

func TestApply_IncludeAllPassesWholePayload(t *testing.T) {
	a := analytics.NewApplier(regWith(&registry.AnalyticsSpec{IncludeAll: true}))
	fact, _, ok, _ := a.Apply(env("widget.updated", 1, `{"name":"a","ssn":"secret"}`))
	if !ok {
		t.Fatal("expected records")
	}
	var got map[string]any
	_ = json.Unmarshal(fact.Payload, &got)
	if got["ssn"] != "secret" {
		t.Fatalf("IncludeAll should keep all fields: %s", fact.Payload)
	}
}

func TestApply_DeleteMarksDeletedEmptyPayload(t *testing.T) {
	a := analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}}))
	fact, _, ok, _ := a.Apply(env("widget.deleted", 5, `{}`))
	if !ok || !fact.Deleted {
		t.Fatalf("delete not marked: ok=%v deleted=%v", ok, fact.Deleted)
	}
}

func TestApply_HashPseudonymizesField(t *testing.T) {
	reg := regWith(&registry.AnalyticsSpec{Include: []string{"name"}, Hash: []string{"ssn"}})
	a := analytics.NewApplier(reg, analytics.WithHashSalt("pepper"))
	fact, _, ok, err := a.Apply(env("widget.updated", 1, `{"name":"a","ssn":"secret"}`))
	if err != nil || !ok {
		t.Fatalf("apply ok=%v err=%v", ok, err)
	}
	var got map[string]any
	_ = json.Unmarshal(fact.Payload, &got)
	if got["name"] != "a" {
		t.Fatalf("name should pass through raw: %s", fact.Payload)
	}
	h, _ := got["ssn"].(string)
	if h == "" || h == "secret" {
		t.Fatalf("ssn should be a non-empty hash, not the raw value: %s", fact.Payload)
	}
	// Same value + salt -> same hash (referential integrity for group-by).
	fact2, _, _, _ := a.Apply(env("widget.updated", 1, `{"name":"b","ssn":"secret"}`))
	var got2 map[string]any
	_ = json.Unmarshal(fact2.Payload, &got2)
	if got2["ssn"] != h {
		t.Fatalf("same ssn hashed differently: %v vs %v", got2["ssn"], h)
	}
	// Different salt -> different hash.
	a2 := analytics.NewApplier(reg, analytics.WithHashSalt("other"))
	fact3, _, _, _ := a2.Apply(env("widget.updated", 1, `{"name":"a","ssn":"secret"}`))
	var got3 map[string]any
	_ = json.Unmarshal(fact3.Payload, &got3)
	if got3["ssn"] == h {
		t.Fatal("different salt produced the same hash")
	}
}

func TestApply_NestedPathInclude(t *testing.T) {
	// Include a nested leaf and drop everything else, including a sibling PII field.
	reg := regWith(&registry.AnalyticsSpec{Include: []string{"name", "meta.region"}})
	a := analytics.NewApplier(reg)
	fact, _, ok, err := a.Apply(env("widget.updated", 1,
		`{"name":"a","meta":{"region":"us","ssn":"secret"},"other":9}`))
	if err != nil || !ok {
		t.Fatalf("apply ok=%v err=%v", ok, err)
	}
	// Deterministic canonical bytes: only name + the nested region survive.
	if got := string(fact.Payload); got != `{"meta":{"region":"us"},"name":"a"}` {
		t.Fatalf("nested include payload = %s, want only name + meta.region", got)
	}
}

func TestApply_NestedPathHash(t *testing.T) {
	reg := regWith(&registry.AnalyticsSpec{Include: []string{"name"}, Hash: []string{"meta.userId"}})
	a := analytics.NewApplier(reg, analytics.WithHashSalt("s"))
	fact, _, ok, _ := a.Apply(env("widget.updated", 1,
		`{"name":"a","meta":{"userId":"u-42","email":"x@y.z"}}`))
	if !ok {
		t.Fatal("expected records")
	}
	var got map[string]any
	_ = json.Unmarshal(fact.Payload, &got)
	meta, _ := got["meta"].(map[string]any)
	if meta == nil {
		t.Fatalf("meta missing: %s", fact.Payload)
	}
	if _, leaked := meta["email"]; leaked {
		t.Fatalf("nested email leaked: %s", fact.Payload)
	}
	h, _ := meta["userId"].(string)
	if h == "" || h == "u-42" {
		t.Fatalf("meta.userId should be hashed, not raw: %s", fact.Payload)
	}
}

func TestApply_NestedPathMissingSegmentDropped(t *testing.T) {
	// A path whose intermediate segment isn't an object contributes nothing.
	reg := regWith(&registry.AnalyticsSpec{Include: []string{"name", "meta.region"}})
	a := analytics.NewApplier(reg)
	fact, _, ok, _ := a.Apply(env("widget.updated", 1, `{"name":"a","meta":"not-an-object"}`))
	if !ok {
		t.Fatal("expected records")
	}
	if got := string(fact.Payload); got != `{"name":"a"}` {
		t.Fatalf("payload = %s, want only name (meta.region unresolvable)", got)
	}
}

func TestApply_HashOnlySpecIncludesHashedField(t *testing.T) {
	a := analytics.NewApplier(regWith(&registry.AnalyticsSpec{Hash: []string{"ssn"}}), analytics.WithHashSalt("s"))
	fact, _, ok, err := a.Apply(env("widget.updated", 1, `{"name":"a","ssn":"x"}`))
	if err != nil || !ok {
		t.Fatalf("apply ok=%v err=%v", ok, err)
	}
	var got map[string]any
	_ = json.Unmarshal(fact.Payload, &got)
	if _, ok := got["ssn"]; !ok {
		t.Fatalf("hashed field should be present: %s", fact.Payload)
	}
	if _, leaked := got["name"]; leaked {
		t.Fatalf("non-listed field leaked: %s", fact.Payload)
	}
}

func TestApply_Deterministic(t *testing.T) {
	a := analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}}))
	e := env("widget.updated", 1, `{"name":"a","ssn":"x"}`)
	f1, _, _, _ := a.Apply(e)
	f2, _, _, _ := a.Apply(e)
	if string(f1.Payload) != string(f2.Payload) {
		t.Fatalf("non-deterministic payload: %s vs %s", f1.Payload, f2.Payload)
	}
}
