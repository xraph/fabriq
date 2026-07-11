package insights_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/registry"
)

type widget struct {
	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id"`
	Version  int64  `grove:"version"`
	Name     string `grove:"name"`
	Price    int    `grove:"price"`
	Qty      int    `grove:"qty"`
	SSN      string `grove:"ssn"`
}

func regWith(spec *registry.InsightsSpec) *registry.Registry {
	r := registry.New()
	_ = r.Register(registry.EntitySpec{Name: "widget", Kind: registry.KindAggregate, Model: widget{}, Insights: spec})
	return r
}

func env(typ string, v int64, payload string) event.Envelope {
	return event.Envelope{
		TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: v, Type: typ,
		At: time.Unix(100, 0).UTC(), Payload: json.RawMessage(payload),
	}
}

func TestApply_DenyByDefault(t *testing.T) {
	a := insights.NewApplier(regWith(nil))
	_, ok, err := a.Apply(env("widget.updated", 1, `{"name":"a","price":10,"qty":2,"ssn":"secret"}`))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if ok {
		t.Fatal("unmarked entity should produce no records")
	}
}

func TestApply_ProjectsOnlyDeclaredColumns(t *testing.T) {
	spec := &registry.InsightsSpec{Measures: []string{"price", "qty"}, Dimensions: []string{"name"}}
	a := insights.NewApplier(regWith(spec))
	fact, ok, err := a.Apply(env("widget.updated", 3, `{"name":"a","price":10,"qty":2,"ssn":"secret"}`))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for an Insights-marked entity")
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(fact.Payload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 projected keys, got %d: %v", len(got), got)
	}
	for _, k := range []string{"name", "price", "qty"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("expected key %q in projected payload", k)
		}
	}
	if _, ok := got["ssn"]; ok {
		t.Fatal("ssn should not appear in the projected payload (kept narrow to declared columns)")
	}
	if fact.TenantID != "t1" || fact.Entity != "widget" || fact.AggID != "w1" || fact.Version != 3 {
		t.Fatalf("unexpected fact identity: %+v", fact)
	}
	if fact.Deleted {
		t.Fatal("expected Deleted=false for a non-delete event")
	}
}

func TestApply_DeletedEvent(t *testing.T) {
	spec := &registry.InsightsSpec{Measures: []string{"price"}}
	a := insights.NewApplier(regWith(spec))
	fact, ok, err := a.Apply(env("widget.deleted", 5, `{"name":"a","price":10}`))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for an Insights-marked entity even on delete")
	}
	if !fact.Deleted {
		t.Fatal("expected Deleted=true")
	}
	if string(fact.Payload) != "{}" {
		t.Fatalf("expected empty payload on delete, got %s", fact.Payload)
	}
}

func TestApply_Deterministic(t *testing.T) {
	spec := &registry.InsightsSpec{Measures: []string{"price", "qty"}, Dimensions: []string{"name"}}
	a := insights.NewApplier(regWith(spec))
	e := env("widget.updated", 7, `{"name":"a","price":10,"qty":2,"ssn":"secret"}`)

	fact1, ok1, err1 := a.Apply(e)
	fact2, ok2, err2 := a.Apply(e)
	if err1 != nil || err2 != nil || !ok1 || !ok2 {
		t.Fatalf("apply failed: err1=%v ok1=%v err2=%v ok2=%v", err1, ok1, err2, ok2)
	}
	if string(fact1.Payload) != string(fact2.Payload) {
		t.Fatalf("expected byte-identical payloads, got %s vs %s", fact1.Payload, fact2.Payload)
	}
}
