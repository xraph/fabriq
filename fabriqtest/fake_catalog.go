package fabriqtest

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// FakeCatalog is the in-memory tenant catalog (catalog.Catalog): the
// deterministic control plane the db-per-tenant router, provisioner and
// sweeper are unit-tested against. Semantics are pinned by the shared
// contract suite (core/catalog/catalogtest), which the Postgres control
// store must also pass.
type FakeCatalog struct {
	mu      sync.Mutex
	entries map[string]catalog.Entry
	// clock advances monotonically so CAS tokens are strictly ordered even
	// within one wall-clock tick.
	clock time.Time
}

var _ catalog.Catalog = (*FakeCatalog)(nil)

// NewFakeCatalog returns an empty catalog.
func NewFakeCatalog() *FakeCatalog {
	return &FakeCatalog{
		entries: map[string]catalog.Entry{},
		clock:   time.Now().UTC(),
	}
}

func (f *FakeCatalog) tick() time.Time {
	f.clock = f.clock.Add(time.Microsecond)
	return f.clock
}

// Get implements catalog.Catalog.
func (f *FakeCatalog) Get(_ context.Context, tenantID string) (catalog.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entries[tenantID]
	if !ok {
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeNotFound,
			"tenant is not in the catalog.", fabriqerr.WithEntity("tenant", tenantID))
	}
	return e, nil
}

// Put implements catalog.Catalog (optimistic concurrency on UpdatedAt).
func (f *FakeCatalog) Put(_ context.Context, e catalog.Entry) (catalog.Entry, error) {
	if err := catalog.ValidateEntry(e); err != nil {
		return catalog.Entry{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, exists := f.entries[e.TenantID]
	switch {
	case e.UpdatedAt.IsZero():
		if exists {
			return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeAlreadyExists,
				"tenant is already in the catalog.", fabriqerr.WithEntity("tenant", e.TenantID))
		}
	case !exists || !stored.UpdatedAt.Equal(e.UpdatedAt):
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeVersionConflict,
			"catalog entry was modified concurrently.", fabriqerr.WithEntity("tenant", e.TenantID))
	}
	e.UpdatedAt = f.tick()
	f.entries[e.TenantID] = e
	return e, nil
}

// List implements catalog.Catalog: stable tenant-id order, opaque cursor.
func (f *FakeCatalog) List(_ context.Context, cursor catalog.Cursor, limit int) ([]catalog.Entry, catalog.Cursor, error) {
	if limit <= 0 {
		limit = 100
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.entries))
	for id := range f.entries {
		if id > string(cursor) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]catalog.Entry, 0, len(ids))
	for _, id := range ids {
		out = append(out, f.entries[id])
	}
	next := catalog.Cursor("")
	if len(ids) == limit && len(ids) > 0 {
		next = catalog.Cursor(ids[len(ids)-1])
	}
	return out, next, nil
}
