// core/agent/assemble_test.go
package agent

import "testing"

func TestFuse_RanksByReciprocalRank(t *testing.T) {
	channels := map[string][]ref{
		"vector": {{Entity: "doc", ID: "a"}, {Entity: "doc", ID: "b"}},
		"search": {{Entity: "doc", ID: "b"}, {Entity: "doc", ID: "c"}},
	}
	got := fuse(channels, nil)
	if len(got) != 3 {
		t.Fatalf("want 3 fused refs, got %d", len(got))
	}
	if got[0].ID != "b" {
		t.Fatalf("want b first (in both channels), got %q", got[0].ID)
	}
	if len(got[0].sources) != 2 {
		t.Fatalf("want b sourced from 2 channels, got %v", got[0].sources)
	}
}

func TestFuse_WeightsBoostChannel(t *testing.T) {
	channels := map[string][]ref{
		"vector": {{Entity: "doc", ID: "a"}},
		"search": {{Entity: "doc", ID: "b"}},
	}
	got := fuse(channels, map[string]float64{"search": 10})
	if got[0].ID != "b" {
		t.Fatalf("want search-weighted b first, got %q", got[0].ID)
	}
}

func TestPack_StopsAtBudget(t *testing.T) {
	items := []ContextItem{{ID: "a", Tokens: 40}, {ID: "b", Tokens: 40}, {ID: "c", Tokens: 40}}
	kept, omitted, used := pack(items, 100)
	if len(kept) != 2 || omitted != 1 || used != 80 {
		t.Fatalf("want 2/1/80, got %d/%d/%d", len(kept), omitted, used)
	}
}

func TestPack_StopsAtFirstOverflow(t *testing.T) {
	items := []ContextItem{{ID: "a", Tokens: 200}, {ID: "b", Tokens: 10}}
	kept, omitted, used := pack(items, 100)
	if len(kept) != 0 || omitted != 2 || used != 0 {
		t.Fatalf("want 0/2/0, got %d/%d/%d", len(kept), omitted, used)
	}
}
