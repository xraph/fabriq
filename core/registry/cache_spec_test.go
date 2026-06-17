package registry_test

import (
	"testing"
	"time"

	"github.com/xraph/fabriq/core/registry"
)

func TestEntitySpecCacheOptIn(t *testing.T) {
	// Zero value: not cached.
	var none registry.EntitySpec
	if none.Cache != nil {
		t.Fatal("zero EntitySpec must have nil Cache (caching off by default)")
	}
	// Opted in.
	spec := registry.EntitySpec{
		Cache: &registry.CacheSpec{TTL: time.Minute, Scoped: true},
	}
	if spec.Cache == nil || spec.Cache.TTL != time.Minute || !spec.Cache.Scoped {
		t.Fatalf("cache spec not carried: %+v", spec.Cache)
	}
}
