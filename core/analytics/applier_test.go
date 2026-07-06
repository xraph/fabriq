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

func env(agg, typ string, v int64, payload string) event.Envelope {
	return event.Envelope{
		TenantID: "t1", Aggregate: agg, AggID: "w1", Version: v, Type: typ,
		At: time.Unix(100, 0).UTC(), Payload: json.RawMessage(payload),
	}
}

func TestApply_DenyByDefault(t *testing.T) {
	a := analytics.NewApplier(regWith(nil))
	_, _, ok, err := a.Apply(env("widget", "widget.updated", 1, `{"name":"a","ssn":"secret"}`))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if ok {
		t.Fatal("unmarked entity should produce no records")
	}
}

func TestApply_IncludeProjectsAllowedFields(t *testing.T) {
	a := analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}}))
	fact, ev, ok, err := a.Apply(env("widget", "widget.updated", 3, `{"name":"a","ssn":"secret"}`))
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
	fact, _, ok, _ := a.Apply(env("widget", "widget.updated", 1, `{"name":"a","ssn":"secret"}`))
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
	fact, _, ok, _ := a.Apply(env("widget", "widget.deleted", 5, `{}`))
	if !ok || !fact.Deleted {
		t.Fatalf("delete not marked: ok=%v deleted=%v", ok, fact.Deleted)
	}
}

func TestApply_Deterministic(t *testing.T) {
	a := analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}}))
	e := env("widget", "widget.updated", 1, `{"name":"a","ssn":"x"}`)
	f1, _, _, _ := a.Apply(e)
	f2, _, _, _ := a.Apply(e)
	if string(f1.Payload) != string(f2.Payload) {
		t.Fatalf("non-deterministic payload: %s vs %s", f1.Payload, f2.Payload)
	}
}
