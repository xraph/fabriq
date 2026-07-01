package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

// digestNodeSpec returns the typed digest_node entity spec — the
// context-distillation Merkle-tree node. It mirrors domain.Register's
// registration (admin-demo builds its registry by hand rather than calling
// domain.RegisterAll, so the digest entity must be added explicitly). Registering
// it does three things: the command plane learns the row shape so the Distiller
// can write nodes, the agent Toolkit's read API (Map/Digest) resolves it, and
// the adminapi distill capability flag flips to true.
//
// The physical digest_nodes table is created by fabriq's own migrations
// (0022-0024), which fabriq.Open runs — no EnsureDynamic is needed (this is a
// typed grove model, not a DynamicSchema).
func digestNodeSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name:      agent.DigestEntity,
		Kind:      registry.KindAggregate,
		Model:     (*domain.DigestNode)(nil),
		GraphNode: "DigestNode",
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	}
}

// demoSummarizer is a DETERMINISTIC, NON-LLM stub summarizer for the demo.
//
// admin-demo has NO language model. The core Distiller REQUIRES a Summarizer
// (agent.NewDistiller rejects a nil one), so the demo supplies this stub: it
// produces stable, human-readable summary text from the structural inputs alone.
// For an L0 leaf it echoes the (already-guarded) source text; for an internal
// (scope/cluster/tenant) node it lists the child kinds it rolls up. Because the
// output is a pure function of the input, repeated distillation is idempotent
// and the Merkle short-circuit works exactly as with a real model.
//
// It implements agent.Summarizer. Do NOT use it for anything but a local demo —
// it does not summarize meaning, it just renders the structure into words.
type demoSummarizer struct{}

// Summarize returns deterministic summary text for one digest node. It never
// errors: every input maps to a string.
func (demoSummarizer) Summarize(_ context.Context, in agent.SummaryInput) (string, error) {
	if in.Level == agent.LevelEntity {
		// L0: echo the source text (trimmed to the token budget's rough char span)
		// so the leaf summary reads like the row it came from.
		raw := strings.TrimSpace(string(in.Raw))
		if raw == "" {
			return "(empty)", nil
		}
		const maxChars = 240
		if len(raw) > maxChars {
			raw = raw[:maxChars]
		}
		return "Entity: " + raw, nil
	}

	// Internal node: render a stable, deterministic roll-up of the children. Count
	// child kinds and emit them in sorted order so the text (and thus the
	// ContentHash-independent summary blob) is reproducible across runs.
	kinds := map[string]int{}
	for _, ch := range in.Children {
		kinds[ch.Kind]++
	}
	parts := make([]string, 0, len(kinds))
	for k := range kinds {
		parts = append(parts, k)
	}
	sort.Strings(parts)
	rendered := make([]string, 0, len(parts))
	for _, k := range parts {
		rendered = append(rendered, fmt.Sprintf("%d %s", kinds[k], k))
	}

	label := strings.Title(in.Kind) //nolint:staticcheck // ASCII kind names; Title is fine and avoids a deps pull
	if in.Scope.Name != "" {
		return fmt.Sprintf("%s %s=%s: summary of %s",
			label, in.Scope.Name, in.Scope.ID, strings.Join(rendered, ", ")), nil
	}
	return fmt.Sprintf("%s: summary of %s", label, strings.Join(rendered, ", ")), nil
}

var _ agent.Summarizer = demoSummarizer{}

// seedDistillTree builds the tenant's context-distillation Merkle tree from the
// already-seeded product and customer rows. It is idempotent: when the tenant
// root (digest:2:tenant) already exists the whole pass is skipped. Otherwise it
// constructs a Distiller (the demo embedder + the deterministic stub summarizer
// + the CAS) and runs one full Distill pass, which creates L0 leaves per
// distillable row, then rolls up the L1 scope backbone and the L2 tenant root.
//
// Requires CAS to be configured (the Distiller stores summary text in the
// content-addressed store). cmd/admin-demo enables it (Storage.EnableCas true),
// so cas is non-nil here; when it is nil the function is a no-op (returns false).
//
// HONEST LIMITATION (observed, not a demo bug): the core Rollup does not
// materialize a complete L1/L2 backbone for every dataset shape. On the demo
// data, the globex tenant builds a full tree (L2 root → L1 scope/cluster → L0),
// while acme-corp (a larger, skewed product set) builds the L0 leaves and the
// cluster backbone but NOT a persisted L2 tenant root or its multi-member scope
// nodes — they are written to the command outbox but absent from the digest_node
// projection after the pass. This is reproducible on a clean database and lives
// entirely in core/agent's Rollup (which this demo must not modify), so the
// adminapi distill endpoints degrade gracefully: /distill/map returns whatever
// nodes ARE projected, and /distill/node/<root> returns 404 for a tenant whose
// root did not materialize. See .p8-3-report.md for the full diagnosis.
//
// Returns (built, report, error): built=false means skipped (tenant already has
// digest nodes, or CAS unavailable); on a fresh build the report carries
// per-pass counts.
func seedDistillTree(
	ctx context.Context,
	f *fabriq.Fabriq,
	reg *registry.Registry,
	emb agent.Embedder,
	cas blob.CAS,
	tenantID string,
) (bool, agent.BackfillReport, error) {
	if cas == nil {
		return false, agent.BackfillReport{}, nil
	}

	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return false, agent.BackfillReport{}, err
	}

	// Idempotency: if the tenant already has ANY digest node, treat the tree as
	// built and skip. We deliberately do NOT key idempotency on the L2 tenant
	// ROOT alone: the core Rollup does not always materialize the root for every
	// dataset shape (see seedDistillTree's package note + .p8-3-report.md), so a
	// root-only check would re-distill a partially-built tenant on every restart,
	// churning the command outbox. The presence of any digest row is the correct
	// "this tenant was distilled" signal.
	if exists, ckErr := tenantHasDigest(tctx, f, reg); ckErr != nil {
		return false, agent.BackfillReport{}, ckErr
	} else if exists {
		return false, agent.BackfillReport{}, nil
	}

	dist, derr := agent.NewDistiller(
		f, reg, emb, demoSummarizer{}, nil /* guard: identity */, cas,
		agent.DistillConfig{VectorDims: emb.Dims()},
	)
	if derr != nil {
		return false, agent.BackfillReport{}, derr
	}

	rep, rErr := dist.Distill(tctx)
	if rErr != nil {
		return false, rep, rErr
	}
	return true, rep, nil
}

// tenantHasDigest reports whether the tenant in ctx already has at least one
// digest node, via a one-row typed relational list. Used as the
// distillation-seed idempotency guard so a re-run skips an already-distilled
// tenant (see seedDistillTree). reg is unused today but kept in the signature so
// the guard can grow entity-aware checks without a call-site change.
func tenantHasDigest(ctx context.Context, f *fabriq.Fabriq, _ *registry.Registry) (bool, error) {
	var nodes []domain.DigestNode
	if err := f.Relational().List(ctx, agent.DigestEntity, query.ListQuery{Limit: 1}, &nodes); err != nil {
		if errors.Is(err, fabriq.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return len(nodes) > 0, nil
}
