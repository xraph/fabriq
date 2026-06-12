package registry_test

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func TestDerive_EventType(t *testing.T) {
	if got := registry.EventType("asset", registry.VerbUpdated); got != "asset.updated" {
		t.Fatalf("EventType = %q", got)
	}
	if got := registry.EventType("site", registry.VerbCreated); got != "site.created" {
		t.Fatalf("EventType = %q", got)
	}
	if got := registry.EventType("tag", registry.VerbDeleted); got != "tag.deleted" {
		t.Fatalf("EventType = %q", got)
	}
}

func TestDerive_ChannelNames(t *testing.T) {
	cases := []struct {
		scope registry.Scope
		id    string
		want  string
	}{
		{registry.ByID, "01H", "changes:acme:id:01H"},
		{registry.ByTenant, "acme", "changes:acme:tenant:acme"},
		{registry.ByField("site", "site_id"), "S1", "changes:acme:site:S1"},
	}
	for _, tc := range cases {
		if got := registry.ChannelName("acme", tc.scope, tc.id); got != tc.want {
			t.Errorf("ChannelName(%v) = %q, want %q", tc.scope, got, tc.want)
		}
	}
}

func TestDerive_StreamKey(t *testing.T) {
	if registry.StreamKey() != "fabriq:events" {
		t.Fatalf("StreamKey = %q", registry.StreamKey())
	}
}

func TestDerive_GraphNames(t *testing.T) {
	if got := registry.GraphName("acme"); got != "tenant_acme" {
		t.Fatalf("GraphName = %q", got)
	}
	if got := registry.GraphNameVersioned("acme", 4); got != "tenant_acme_v4" {
		t.Fatalf("GraphNameVersioned = %q", got)
	}
}

func TestDerive_SearchIndexNames(t *testing.T) {
	if got := registry.SearchIndexAlias("acme", "assets"); got != "fabriq_acme_assets" {
		t.Fatalf("SearchIndexAlias = %q", got)
	}
	if got := registry.SearchIndexVersioned("acme", "assets", 2); got != "fabriq_acme_assets_v2" {
		t.Fatalf("SearchIndexVersioned = %q", got)
	}
}

func BenchmarkDeriveChannel(b *testing.B) {
	scope := registry.ByField("site", "site_id")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = registry.ChannelName("acme", scope, "S1")
	}
}
