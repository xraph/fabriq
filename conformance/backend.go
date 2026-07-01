package conformance

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// Capability names a behavior a backend either supports exactly or
// legitimately diverges on. Cases that Require a capability the
// system-under-test lacks are skipped or asserted-degraded.
type Capability string

const (
	// CapConcurrentTx: real optimistic-concurrency version conflicts (the
	// fake serializes transactions one at a time).
	CapConcurrentTx Capability = "concurrent-tx"
	// CapBucketedAgg: TimescaleDB time_bucket aggregation (the fake stores
	// raw points only).
	CapBucketedAgg Capability = "ts-bucketed-agg"
	// CapRelevanceScore: real full-text relevance ranking (the fake returns
	// id-order, having no scorer).
	CapRelevanceScore Capability = "search-relevance"
	// CapRawSQL: the relational raw-SQL escape hatch (the fake has no SQL
	// engine).
	CapRawSQL Capability = "raw-sql"
	// CapRawCypher: uncanned openCypher (the fake serves canned responses).
	CapRawCypher Capability = "raw-cypher"
	// CapPersistence: survives a reopen (the fakes are in-memory).
	CapPersistence Capability = "cross-restart"
	// CapBlobPresign: the byte store issues presigned client-direct URLs.
	CapBlobPresign Capability = "blob-presign"
	// CapBlobMultipart: the byte store supports resumable multipart uploads.
	CapBlobMultipart Capability = "blob-multipart"
	// CapBlobRange: the byte store supports byte-range reads.
	CapBlobRange Capability = "blob-range"
)

// CapabilitySet is the set of capabilities a backend supports exactly.
type CapabilitySet map[Capability]bool

// Has reports whether the set contains want.
func (c CapabilitySet) Has(want Capability) bool { return c[want] }

// missing returns the subset of requires the set does not provide, in order.
func (c CapabilitySet) missing(requires []Capability) []Capability {
	var out []Capability
	for _, r := range requires {
		if !c[r] {
			out = append(out, r)
		}
	}
	return out
}

// Backend is one system-under-test: the fakes, or a real adapter set over a
// container. The conformance runner drives it through the shared Case tables.
type Backend interface {
	// Name identifies the backend in subtest names and skip reasons, e.g.
	// "fake", "postgres", "falkordb", "elasticsearch".
	Name() string

	// Capabilities are the behaviors this backend supports exactly.
	Capabilities() CapabilitySet

	// Setup returns a fresh, isolated environment for ONE case. The backend
	// registers its own t.Cleanup. Isolation is by unique tenant; no
	// truncation is required.
	Setup(t *testing.T) *Env
}

// Env is one isolated case environment. A nil port field means "not
// implemented by this backend" — that port's whole suite is skipped.
type Env struct {
	Ctx         context.Context         // unique primary tenant for this case
	ForeignCtx  context.Context         // a second, distinct tenant over the same store
	Registry    *registry.Registry      // domain.RegisterAll(reg)
	Exec        *command.Executor       // write path; nil → command/store suite skips
	Relational  query.RelationalQuerier // nil → relational suite skips
	Graph       query.GraphQuerier
	Search      query.SearchQuerier
	Vector      query.VectorQuerier
	Spatial     query.SpatialQuerier
	TS          query.TSQuerier
	Projection  projection.StateReader
	Blob        blob.Store // nil → blob suite skips
	GraphTarget string     // fresh graph/projection target for this case
	// EmbeddingDim is the dimension required by the vector store.
	// 0 means "no fixed dimension" (the fake accepts any size; RunVector uses
	// 3-dimensional test vectors). Set to 768 for the postgres backend, which
	// enforces the schema-declared vector(768) column.
	EmbeddingDim int
}

// Degradation describes what a backend lacking a case's required capability
// must return INSTEAD of the happy-path result. Set one or more of ExpectErrIs,
// ExpectErrContains, or ExpectCode.
type Degradation struct {
	// ExpectErrIs requires errors.Is(err, ExpectErrIs).
	ExpectErrIs error
	// ExpectErrContains requires the error message to contain this substring.
	ExpectErrContains string
	// ExpectCode, when non-empty, requires errors.As(err, &fabriqerr.Error) with
	// that Code — asserting drivers classify a fault identically.
	ExpectCode fabriqerr.Code
}
