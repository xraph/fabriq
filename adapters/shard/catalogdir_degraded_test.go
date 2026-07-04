package shard

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// degradingCat returns a degraded NotFound for "ghost" and counts calls, so we
// can prove the directory did NOT cache the degraded negative.
type degradingCat struct{ calls int }

func (d *degradingCat) Get(_ context.Context, id string) (catalog.Entry, error) {
	d.calls++
	return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeNotFound, "stale",
		fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"catalog": "degraded"}}))
}
func (d *degradingCat) Put(context.Context, catalog.Entry) (catalog.Entry, error) {
	return catalog.Entry{}, nil
}
func (d *degradingCat) List(context.Context, catalog.Cursor, int) ([]catalog.Entry, catalog.Cursor, error) {
	return nil, "", nil
}

func TestCatalogDirectory_DegradedNegativeNotCached(t *testing.T) {
	cat := &degradingCat{}
	now := time.Now()
	dir := CatalogDirectory(cat, time.Minute, WithCatalogClock(func() time.Time { return now }))

	for i := 0; i < 3; i++ {
		if _, err := dir.Shard(context.Background(), "ghost"); fabriqerr.CodeOf(err) != fabriqerr.CodeNotFound {
			t.Fatalf("want NotFound, got %v", err)
		}
	}
	if cat.calls != 3 {
		t.Fatalf("degraded NotFound must not be cached: got %d catalog calls, want 3", cat.calls)
	}
}
