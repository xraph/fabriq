package postgres

import "testing"

func TestWithSharedSchema_PlumbsToAccessor(t *testing.T) {
	cfg := openConfig{}
	WithSharedSchema("fabriq_shared")(&cfg)
	if cfg.sharedSchema != "fabriq_shared" {
		t.Fatalf("got %q", cfg.sharedSchema)
	}
}
