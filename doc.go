// Package fabriq is the TWINOS data fabric: the single module through which
// application code talks to every datastore.
//
// Fabriq enforces three invariants across stores:
//
//   - Every write emits exactly one versioned event (transactional outbox).
//   - Every access is tenant-scoped (structural stamping + RLS + hook backstop).
//   - Projections (graph, search) are derived from Postgres and always
//     rebuildable from it.
//
// The kernel in core/ is domain-agnostic and engine-agnostic: capability
// ports (Relational, Graph, Search, Timeseries, Vector, Document), a
// declarative schema registry, a command plane, and a subscription hub.
// Engine dialects live exclusively under adapters/. The TWINOS domain pack
// lives in domain/ and is the only TWINOS-aware package.
//
//	f, err := fabriq.Open(ctx, cfg,
//	    fabriq.WithConflationWindow(150*time.Millisecond),
//	)
//
// Fabriq is built on the Forge ecosystem: storage on github.com/xraph/grove,
// binaries on github.com/xraph/forge (apps) and forge/cli (CLI).
package fabriq
