package fabriq

import (
	"context"
	"testing"
)

// TestShardDirectory verifies the directory Open builds for a multi-shard
// set: hash placement by default, with config-pinned tenants overriding.
func TestShardDirectory(t *testing.T) {
	ctx := context.Background()
	ids := []string{"shard-a", "shard-b"}

	// No pins: pure hash placement.
	plain := shardDirectory(ids, nil)
	hashHome, err := plain.Shard(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if hashHome != "shard-a" && hashHome != "shard-b" {
		t.Fatalf("hash routed to unknown shard %q", hashHome)
	}

	// Pin acme to the shard the hash would NOT pick; everyone else hashes.
	pinned := "shard-a"
	if hashHome == "shard-a" {
		pinned = "shard-b"
	}
	dir := shardDirectory(ids, map[string]string{"acme": pinned})
	got, err := dir.Shard(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if got != pinned {
		t.Fatalf("pinned tenant routed to %q, want %q", got, pinned)
	}
	other, err := dir.Shard(ctx, "globex")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := plain.Shard(ctx, "globex")
	if other != want {
		t.Fatalf("unpinned tenant diverged from hash placement: %q != %q", other, want)
	}
}
