package fabriq_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestLiveQueryNotConfigured verifies the facade returns a typed error when no
// live oracle/tailer is wired (e.g. a relational-only embedding), rather than
// panicking or silently doing nothing.
func TestLiveQueryNotConfigured(t *testing.T) {
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	w := fabriqtest.NewWorld(reg)
	f, err := fabriq.New(reg, fabriq.Ports{Store: w.Store, Relational: w.Rel})
	if err != nil {
		t.Fatal(err)
	}
	q := livequery.LiveQuery{
		Entity: "asset",
		Where:  query.Where{query.Eq("kind", "pump")},
		Sort:   []livequery.SortKey{{Column: "name"}},
		Limit:  10,
	}
	if _, _, _, err := f.LiveQuery(context.Background(), q); !errors.Is(err, fabriq.ErrStoreNotConfigured) {
		t.Fatalf("want ErrStoreNotConfigured, got %v", err)
	}
}
