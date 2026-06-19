package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// ErrSummaryBlocked is the internal sentinel for a guard veto. DistillL0 does
// NOT return it — a blocked summary is fail-closed (changed=false, nothing
// stored) — but the rollup paths reuse it to distinguish a block from an error.
var ErrSummaryBlocked = errors.New("agent: summary blocked by guard")

// DistillConfig tunes the Distiller. Zero values get defaults via withDefaults.
type DistillConfig struct {
	RecipeVersion string // salts ContentHash; bump to invalidate the whole tree
	VectorDims    int
	SemSeed       int64
	ClusterBits   int // top-p SemHash bits used as the cluster bucket key
	NoiseFloor    int // min members for a bucket to become a cluster node
	FailOpenGuard bool
	Budget        int // default L0 summary token budget
}

func (c *DistillConfig) withDefaults() {
	if c.RecipeVersion == "" {
		c.RecipeVersion = "v1"
	}
	if c.VectorDims <= 0 {
		c.VectorDims = defaultVectorDims
	}
	if c.SemSeed == 0 {
		c.SemSeed = 1
	}
	if c.ClusterBits <= 0 {
		c.ClusterBits = 12
	}
	if c.NoiseFloor <= 0 {
		c.NoiseFloor = 2
	}
	if c.Budget <= 0 {
		c.Budget = 256
	}
}

// Distiller builds and maintains a tenant's context-distillation Merkle tree.
// L0 leaves are derived from distillable source rows; internal nodes (scope,
// cluster, tenant) are produced by rollup. A ContentHash Merkle short-circuit
// keeps re-distillation cheap, and a two-stage Guard (ingest + emit) fences PII
// out of both the model and the content-addressed store.
type Distiller struct {
	fab    query.Fabric
	reg    *registry.Registry
	emb    Embedder
	sum    Summarizer
	guard  Guard
	cas    blob.CAS
	cfg    DistillConfig
	planes [64][]float32
}

// NewDistiller builds a Distiller. emb, sum, cas are required; guard is optional
// (nil = identity). The embedder's dimensionality must match the configured
// VectorDims (after defaults).
func NewDistiller(fab query.Fabric, reg *registry.Registry, emb Embedder, sum Summarizer, guard Guard, cas blob.CAS, cfg DistillConfig) (*Distiller, error) {
	if fab == nil || reg == nil {
		return nil, fmt.Errorf("agent: distiller requires Fabric and Registry")
	}
	if emb == nil || sum == nil || cas == nil {
		return nil, fmt.Errorf("agent: distiller requires Embedder, Summarizer, and CAS")
	}
	cfg.withDefaults()
	if emb.Dims() != cfg.VectorDims {
		return nil, fmt.Errorf("agent: distiller embedder dims %d != configured %d", emb.Dims(), cfg.VectorDims)
	}
	return &Distiller{
		fab: fab, reg: reg, emb: emb, sum: sum, guard: guard, cas: cas, cfg: cfg,
		planes: NewSemPlanes(cfg.VectorDims, cfg.SemSeed),
	}, nil
}

// distillSpec returns the DistillSpec for an entity, or nil when the entity is
// unknown or not opted into distillation.
func (d *Distiller) distillSpec(entity string) *registry.DistillSpec {
	e, ok := d.reg.Get(entity)
	if !ok {
		return nil
	}
	return e.Spec.Distill
}

// distillTextFor builds the L0 source text from a row's column values. Mirrors
// embedTextFor: spec.Text overrides SourceFields when set.
func distillTextFor(spec *registry.DistillSpec, vals map[string]any) string {
	if spec.Text != nil {
		return spec.Text(vals)
	}
	parts := make([]string, 0, len(spec.SourceFields))
	for _, f := range spec.SourceFields {
		if v, ok := vals[f]; ok && v != nil {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
	}
	return strings.Join(parts, " ")
}

// DistillL0 distills one source row into its L0 digest node. Returns changed=true
// when the node was (re)summarized and persisted; false on a Merkle
// short-circuit (unchanged source), on a guard block (fail-closed: nothing is
// stored), for non-distillable entities, or for empty source text.
func (d *Distiller) DistillL0(ctx context.Context, entity, id string, vals map[string]any) (bool, error) {
	spec := d.distillSpec(entity)
	if spec == nil {
		return false, nil
	}
	if id == "" {
		return false, fmt.Errorf("agent: distill %q: empty id", entity)
	}
	raw := distillTextFor(spec, vals)
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	nodeID := L0ID(entity, id)

	// Merkle short-circuit: the ContentHash is structural (over the source, not
	// the non-deterministic summary), so an unchanged row never calls the model.
	newContentHash := L0ContentHash(d.cfg.RecipeVersion, SourceFieldHash(raw))
	if existing, ok, err := d.getNode(ctx, nodeID); err != nil {
		return false, err
	} else if ok && existing.ContentHash == newContentHash {
		return false, nil // unchanged — no LLM call
	}

	// Guard ingest: redact raw content BEFORE the model sees it.
	gi, _ := applyGuard(ctx, d.guard, GuardInput{
		Stage: GuardIngest, TenantID: tenantOf(ctx), Level: LevelEntity, Text: raw,
	}, d.cfg.FailOpenGuard)
	if gi.Blocked {
		return false, nil // fail-closed: drop
	}

	budget := spec.Budget
	if budget <= 0 {
		budget = d.cfg.Budget
	}
	summary, err := d.sum.Summarize(ctx, SummaryInput{
		Level: LevelEntity, Kind: KindEntityNode, Raw: []byte(gi.Text), Budget: budget,
	})
	if err != nil {
		return false, fmt.Errorf("agent: summarize %q/%s: %w", entity, id, err)
	}

	// Guard emit: check the generated summary BEFORE it is hashed + written to CAS.
	ge, _ := applyGuard(ctx, d.guard, GuardInput{
		Stage: GuardEmit, TenantID: tenantOf(ctx), Level: LevelEntity, Text: summary,
	}, d.cfg.FailOpenGuard)
	if ge.Blocked {
		return false, nil // fail-closed: drop
	}

	if _, err := d.persistSummary(ctx, persistArgs{
		id: nodeID, level: LevelEntity, kind: KindEntityNode,
		sourceKind: entity, sourceID: id,
		summaryText: ge.Text, contentHash: newContentHash,
		parents: d.l0Parents(spec, vals),
	}); err != nil {
		return false, err
	}
	return true, nil
}

// persistArgs are the inputs to persistSummary: the digest node identity, the
// already-guarded summary text, the structural ContentHash, and the parent
// scope/cluster ids to back-link.
type persistArgs struct {
	id          string
	level       int
	kind        string
	scopeName   string
	scopeID     string
	sourceKind  string
	sourceID    string
	summaryText string
	contentHash string
	parents     []string
	children    []string // ChildIDs for internal (rollup) nodes; nil for L0 leaves
}

// persistSummary stores a node's summary in CAS, embeds it, computes its
// SemHash, writes the digest_node row through the command plane, upserts the
// node's vector, and back-links it into each parent. Returns the written row.
func (d *Distiller) persistSummary(ctx context.Context, args persistArgs) (digestRow, error) {
	hash, _, err := d.cas.Store(ctx, bytes.NewReader([]byte(args.summaryText)))
	if err != nil {
		return digestRow{}, fmt.Errorf("agent: cas store %s: %w", args.id, err)
	}
	vecs, err := d.emb.Embed(ctx, []string{args.summaryText})
	if err != nil {
		return digestRow{}, fmt.Errorf("agent: embed summary %s: %w", args.id, err)
	}
	if len(vecs) != 1 {
		return digestRow{}, fmt.Errorf("agent: embed summary %s: got %d vectors for 1 input", args.id, len(vecs))
	}
	vec := vecs[0]
	sem := FormatSemHash(SemHash(vec, d.planes))

	row := digestRow{
		ID:          args.id,
		Level:       args.level,
		Kind:        args.kind,
		ScopeName:   args.scopeName,
		ScopeID:     args.scopeID,
		SourceID:    args.sourceID,
		SourceKind:  args.sourceKind,
		SummaryHash: hash,
		ContentHash: args.contentHash,
		SemHash:     sem,
		ChildIDs:    args.children,
		ParentIDs:   args.parents,
		UpdatedAt:   time.Now().UnixNano(),
	}
	if err := d.upsertNode(ctx, row); err != nil {
		return digestRow{}, err
	}
	if err := d.fab.Vector().Upsert(ctx, DigestEntity, args.id, vec, nil); err != nil {
		return digestRow{}, fmt.Errorf("agent: vector upsert %s: %w", args.id, err)
	}
	// Back-link this node into each parent's ChildIDs. The parent nodes
	// themselves (scope/cluster/tenant) are (re)summarized during rollup.
	for _, pid := range args.parents {
		if err := d.linkChild(ctx, pid, args.id); err != nil {
			return digestRow{}, err
		}
	}
	return row, nil
}

// l0Parents returns the scope-node parents of an L0 leaf: one ScopeID per
// declared scope whose value is present in vals. The cluster parent is assigned
// later (during rollup) once the SemHash is known, so it is NOT included here.
func (d *Distiller) l0Parents(spec *registry.DistillSpec, vals map[string]any) []string {
	parents := make([]string, 0, len(spec.Scopes))
	for _, name := range spec.Scopes {
		if v := scopeValue(vals, name); v != "" {
			parents = append(parents, ScopeID(name, v))
		}
	}
	return parents
}

// scopeValue reads a scope's value from a row: it prefers the "<name>_id"
// column, falling back to "<name>". Returns "" when neither is a non-empty
// string.
func scopeValue(vals map[string]any, name string) string {
	for _, col := range []string{name + "_id", name} {
		if v, ok := vals[col]; ok && v != nil {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// linkChild appends childID to a parent node's ChildIDs (idempotently),
// re-upserting the parent. The parent may not exist yet (its summary is written
// during rollup); in that case linkChild is a no-op — rollup will gather
// children from the source side.
func (d *Distiller) linkChild(ctx context.Context, parentID, childID string) error {
	parent, ok, err := d.getNode(ctx, parentID)
	if err != nil {
		return err
	}
	if !ok {
		return nil // parent not materialized yet; rollup will build it
	}
	for _, c := range parent.ChildIDs {
		if c == childID {
			return nil // already linked
		}
	}
	parent.ChildIDs = append(parent.ChildIDs, childID)
	return d.upsertNode(ctx, parent)
}

// tenantOf returns the tenant id stamped on ctx (empty if unstamped).
func tenantOf(ctx context.Context) string {
	id, _ := tenant.Require(ctx)
	return id
}

// digestRow is the in-package, domain-free projection of a digest_node row.
// Exported fields let white-box tests assert on the Merkle/scope state.
type digestRow struct {
	ID          string
	Level       int
	Kind        string
	ScopeName   string
	ScopeID     string
	SourceID    string
	SourceKind  string
	SummaryHash string
	ContentHash string
	SemHash     string
	ChildIDs    []string
	ParentIDs   []string
	UpdatedAt   int64
}

// toVals renders the row as a column-keyed map for Binding.Populate. It omits
// version and tenant_id — the command plane stamps both (tenant from ctx).
func (r digestRow) toVals() map[string]any {
	return map[string]any{
		"id":           r.ID,
		"level":        r.Level,
		"kind":         r.Kind,
		"scope_name":   r.ScopeName,
		"scope_id":     r.ScopeID,
		"source_id":    r.SourceID,
		"source_kind":  r.SourceKind,
		"summary_hash": r.SummaryHash,
		"content_hash": r.ContentHash,
		"sem_hash":     r.SemHash,
		"child_ids":    r.ChildIDs,
		"parent_ids":   r.ParentIDs,
		"updated_at":   r.UpdatedAt,
	}
}

// digestRowFromVals builds a digestRow from Binding.ValuesByColumn output.
// version and tenant_id are present in the map but irrelevant to Merkle logic
// and are ignored.
func digestRowFromVals(m map[string]any) digestRow {
	return digestRow{
		ID:          asString(m["id"]),
		Level:       asInt(m["level"]),
		Kind:        asString(m["kind"]),
		ScopeName:   asString(m["scope_name"]),
		ScopeID:     asString(m["scope_id"]),
		SourceID:    asString(m["source_id"]),
		SourceKind:  asString(m["source_kind"]),
		SummaryHash: asString(m["summary_hash"]),
		ContentHash: asString(m["content_hash"]),
		SemHash:     asString(m["sem_hash"]),
		ChildIDs:    asStrings(m["child_ids"]),
		ParentIDs:   asStrings(m["parent_ids"]),
		UpdatedAt:   asInt64(m["updated_at"]),
	}
}

// getNode reads a digest_node row by id, returning (zero, false, nil) when the
// row is absent within the tenant's scope.
func (d *Distiller) getNode(ctx context.Context, id string) (digestRow, bool, error) {
	ent, ok := d.reg.Get(DigestEntity)
	if !ok {
		return digestRow{}, false, fmt.Errorf("agent: %q not registered", DigestEntity)
	}
	model := ent.Binding.NewModel()
	if err := d.fab.Relational().Get(ctx, DigestEntity, id, model); err != nil {
		var nfe *fabriqerr.NotFoundError
		if errors.Is(err, fabriqerr.ErrNotFound) || errors.As(err, &nfe) {
			return digestRow{}, false, nil
		}
		return digestRow{}, false, err
	}
	vals, err := ent.Binding.ValuesByColumn(model)
	if err != nil {
		return digestRow{}, false, err
	}
	return digestRowFromVals(vals), true, nil
}

// upsertNode writes a digest_node row through the command plane using the
// registry's type-erased model primitives. A map payload is rejected by the
// command plane for a typed entity, so the row is materialized into the
// registered model via Binding.Populate. version and tenant_id are stamped by
// the command plane (tenant from ctx) and must not be in toVals().
func (d *Distiller) upsertNode(ctx context.Context, row digestRow) error {
	ent, ok := d.reg.Get(DigestEntity)
	if !ok {
		return fmt.Errorf("agent: %q not registered", DigestEntity)
	}
	model := ent.Binding.NewModel()
	if err := ent.Binding.Populate(model, row.toVals()); err != nil {
		return fmt.Errorf("agent: populate digest node %s: %w", row.ID, err)
	}
	if _, err := d.fab.Exec(ctx, command.Command{
		Entity:  DigestEntity,
		Op:      command.OpUpsert,
		AggID:   row.ID,
		Payload: model,
	}); err != nil {
		return fmt.Errorf("agent: upsert digest node %s: %w", row.ID, err)
	}
	return nil
}

// --- value coercion helpers (ValuesByColumn returns interface-typed fields) --

func asString(v any) string {
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

func asStrings(v any) []string {
	if s, ok := v.([]string); ok {
		return s
	}
	return nil
}
