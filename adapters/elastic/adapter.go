// Package elastic is fabriq's search adapter (PHASE 5 — SCAFFOLD).
//
// PLANNED SHAPE (see also index.go and mutate.go):
//
//   - adapter.go: SearchQuerier on go-elasticsearch/v8 (the client dep is
//     added when the implementation lands, keeping the scaffold light).
//     Search(q) runs a multi_match over the entity's declared fields
//     against the tenant's ALIAS (registry.SearchIndexAlias).
//   - mutate.go: DocIndex/DocDeindex -> _bulk ops with external_gte
//     versioning (the Version field gates idempotency engine-side).
//   - index.go: versioned indexes (registry.SearchIndexVersioned) behind
//     atomic alias swaps — rebuilds create assets_v{N+1}, reindex FROM
//     POSTGRES (never from the old index), then swap the alias in one
//     _aliases call and drop the old index after soak.
//
// Until then every method returns ErrStoreNotConfigured-shaped errors so
// misconfiguration is loud, and the facade's Search() port stays typed.
package elastic

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
)

// Adapter implements query.SearchQuerier (phase 5).
type Adapter struct{}

var _ query.SearchQuerier = (*Adapter)(nil)

func errPending(op string) error {
	return fmt.Errorf("fabriq: elasticsearch %s not implemented yet (phase 5): %w", op, fabriqerr.ErrStoreNotConfigured)
}

// Search implements query.SearchQuerier.
//
// TODO(phase 5): multi_match over declared fields, tenant alias routing,
// hits into *[]map[string]any.
func (a *Adapter) Search(context.Context, query.SearchQuery, any) error {
	return errPending("Search")
}

// ApplyMutations implements query.SearchQuerier.
//
// TODO(phase 5): translate to _bulk index/delete with external_gte
// version gating; one bulk request per consumed batch.
func (a *Adapter) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return errPending("ApplyMutations")
}
