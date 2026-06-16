// Package conformance is fabriq's cross-port conformance kit: one exported
// Case table per capability port, run against BOTH the in-memory fabriqtest
// fakes and the real adapters, so the fakes cannot silently drift from
// Postgres / FalkorDB / Elasticsearch truth.
//
// The defining property is "drift becomes a failing test": if the fake's
// List ordering changes, or a real adapter's filter semantics diverge from
// the fake's, a conformance subtest goes red — at go-test speed for the
// fakes, under the integration build tag for the adapters.
//
// Deliberate fake-vs-real divergences (the fake serializes transactions,
// stores raw time-series points only, has no relevance scorer, …) are
// encoded as Capability requirements on individual cases and gated through
// skip-or-assert-degraded logic. Every capability is justified in the
// reviewed ledger (ledger.go); introducing a new divergence is mechanically
// forced through a ledger edit.
package conformance
