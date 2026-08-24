package fabriqtest

import (
	"testing"

	"github.com/xraph/fabriq/core/cache"
	coretest "github.com/xraph/fabriq/core/fabriqtest"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// The fakes live in core/fabriqtest so the core module can test itself
// without depending on the main module. These aliases keep
// github.com/xraph/fabriq/fabriqtest working for anyone importing it.
//
// Aliases, not wrappers: fabriqtest.World and coretest.World are the same
// type, so methods, struct fields and type assertions all carry over.
type (
	World               = coretest.World
	FakeStore           = coretest.FakeStore
	FakeRelational      = coretest.FakeRelational
	FakeGraph           = coretest.FakeGraph
	FakeNode            = coretest.FakeNode
	FakeSearch          = coretest.FakeSearch
	FakeTS              = coretest.FakeTS
	FakeVector          = coretest.FakeVector
	FakeSpatial         = coretest.FakeSpatial
	FakeDocumentStore   = coretest.FakeDocumentStore
	FakeProjectionState = coretest.FakeProjectionState
	FakeBlob            = coretest.FakeBlob
	FakeCAS             = coretest.FakeCAS
	FakeCache           = coretest.FakeCache
	FakeCatalog         = coretest.FakeCatalog
	FakeAnalytics       = coretest.FakeAnalytics
	FakeAnalyticsSink   = coretest.FakeAnalyticsSink
)

// ErrFakeNotFound is the not-found sentinel the fakes return.
var ErrFakeNotFound = coretest.ErrFakeNotFound

// NewWorld builds a fake world backed by shared in-memory storage.
func NewWorld(reg *registry.Registry) *World { return coretest.NewWorld(reg) }

// NewFabric returns a query.Fabric over the world's fakes.
func NewFabric(w *World) query.Fabric { return coretest.NewFabric(w) }

// NewFakeGraph builds a graph querier over a relational querier.
func NewFakeGraph(reg *registry.Registry, rel query.RelationalQuerier) *FakeGraph {
	return coretest.NewFakeGraph(reg, rel)
}

// NewFakeSearch builds an in-memory search querier.
func NewFakeSearch(reg *registry.Registry) *FakeSearch { return coretest.NewFakeSearch(reg) }

// NewFakeBlob builds an in-memory blob store.
func NewFakeBlob() *FakeBlob { return coretest.NewFakeBlob() }

// NewFakeCAS builds an in-memory content-addressable store.
func NewFakeCAS() *FakeCAS { return coretest.NewFakeCAS() }

// NewFakeCache builds an in-memory cache.
func NewFakeCache() *FakeCache { return coretest.NewFakeCache() }

// NewFakeCatalog builds an in-memory tenant catalog.
func NewFakeCatalog() *FakeCatalog { return coretest.NewFakeCatalog() }

// NewFakeAnalytics builds an in-memory analytics querier.
func NewFakeAnalytics(reg *registry.Registry) *FakeAnalytics {
	return coretest.NewFakeAnalytics(reg)
}

// NewFakeAnalyticsSink builds an in-memory analytics sink.
func NewFakeAnalyticsSink() *FakeAnalyticsSink { return coretest.NewFakeAnalyticsSink() }

// RunCacheConformance runs the shared cache conformance suite.
func RunCacheConformance(t *testing.T, newCache func(t *testing.T) cache.Cache) {
	coretest.RunCacheConformance(t, newCache)
}
