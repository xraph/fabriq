package agent

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/xraph/fabriq/core/query"
)

// RollupReport summarizes one Rollup pass over the tenant in ctx.
type RollupReport struct {
	ScopeNodes    int  // scope (L1) nodes (re)summarized this pass
	ClusterNodes  int  // cluster (L1) nodes (re)summarized this pass
	TenantRolled  bool // the tenant (L2) root was (re)summarized this pass
	ShortCircuits int  // internal nodes that matched their Merkle hash (no model call)
}

// Rollup recomputes the tenant's L1 (scope + cluster) backbone and L2 (tenant)
// root from the current L0 leaves, with a ContentHash Merkle short-circuit at
// every internal node: a node whose sorted child ContentHashes are unchanged is
// not re-summarized. Tenant scope comes from ctx (RLS).
//
// Cluster assignment is part of this pass: each L0 is bucketed by its SemHash
// prefix; buckets meeting the noise floor become cluster nodes and are linked as
// parents of their members. Below-floor (singleton) buckets get no cluster node.
func (d *Distiller) Rollup(ctx context.Context) (RollupReport, error) {
	var rep RollupReport

	l0s, err := d.listNodes(ctx, LevelEntity)
	if err != nil {
		return rep, fmt.Errorf("agent: rollup list L0: %w", err)
	}

	// 1. Cluster assignment: bucket L0s by SemHash prefix; link members of any
	//    above-floor bucket to their cluster node.
	buckets := map[string][]string{} // clusterID -> member L0 ids
	for _, n := range l0s {
		prefix := ClusterPrefix(parseSemOrZero(n.SemHash), d.cfg.ClusterBits)
		cid := ClusterID(prefix, d.cfg.ClusterBits)
		buckets[cid] = append(buckets[cid], n.ID)
	}
	cluster := map[string]bool{} // above-floor cluster ids
	for cid, members := range buckets {
		if !NoiseFloorMet(len(members), d.cfg.NoiseFloor) {
			continue
		}
		cluster[cid] = true
		for _, mid := range members {
			if err := d.ensureParent(ctx, mid, cid); err != nil {
				return rep, fmt.Errorf("agent: rollup link cluster %s: %w", cid, err)
			}
		}
	}

	// Re-list L0s after cluster linking so member ParentIDs reflect the cluster
	// assignment when we gather scope/cluster membership below.
	l0s, err = d.listNodes(ctx, LevelEntity)
	if err != nil {
		return rep, fmt.Errorf("agent: rollup relist L0: %w", err)
	}

	// 2. Scope nodes: one per distinct digest:1:scope:* id in L0 ParentIDs.
	scopeMembers := map[string][]string{}
	for _, n := range l0s {
		for _, pid := range n.ParentIDs {
			if isScopeID(pid) {
				scopeMembers[pid] = append(scopeMembers[pid], n.ID)
			}
		}
	}
	for sid := range scopeMembers {
		name, id := parseScopeID(sid)
		rolled, scErr := d.rollupFromMembers(ctx, persistArgs{
			id: sid, level: LevelScope, kind: KindScopeNode,
			scopeName: name, scopeID: id,
			parents: []string{TenantRootID()},
		}, scopeMembers[sid])
		if scErr != nil {
			return rep, scErr
		}
		if rolled {
			rep.ScopeNodes++
		} else {
			rep.ShortCircuits++
		}
	}

	// 3. Cluster nodes: one per above-floor bucket.
	for cid := range cluster {
		rolled, cErr := d.rollupFromMembers(ctx, persistArgs{
			id: cid, level: LevelScope, kind: KindClusterNode,
			parents: []string{TenantRootID()},
		}, buckets[cid])
		if cErr != nil {
			return rep, cErr
		}
		if rolled {
			rep.ClusterNodes++
		} else {
			rep.ShortCircuits++
		}
	}

	// 4. Tenant root: children = all current L1 (scope + cluster) nodes.
	l1s, err := d.listNodes(ctx, LevelScope)
	if err != nil {
		return rep, fmt.Errorf("agent: rollup list L1: %w", err)
	}
	l1IDs := make([]string, 0, len(l1s))
	for _, n := range l1s {
		l1IDs = append(l1IDs, n.ID)
	}
	rolled, err := d.rollupFromMembers(ctx, persistArgs{
		id: TenantRootID(), level: LevelTenant, kind: KindTenantNode,
	}, l1IDs)
	if err != nil {
		return rep, err
	}
	if rolled {
		rep.TenantRolled = true
	} else if len(l1IDs) > 0 {
		rep.ShortCircuits++
	}

	return rep, nil
}

// rollupFromMembers loads the member nodes' ContentHashes + summaries, then runs
// the shared short-circuit/summarize/persist path. memberIDs may be in any
// order; child hashes are sorted for an order-independent Merkle hash.
func (d *Distiller) rollupFromMembers(ctx context.Context, args persistArgs, memberIDs []string) (bool, error) {
	children, childHashes, err := d.childDigests(ctx, memberIDs)
	if err != nil {
		return false, fmt.Errorf("agent: rollup children for %s: %w", args.id, err)
	}
	return d.rollupNode(ctx, args, childHashes, children)
}

// rollupNode is the shared internal-node path: Merkle short-circuit, summarize,
// guard-emit, persist. Returns rolled=true only when a new summary was written;
// false on a short-circuit or a guard block (fail-closed).
func (d *Distiller) rollupNode(ctx context.Context, args persistArgs, childHashes []string, children []ChildDigest) (bool, error) {
	ch := RollupContentHash(d.cfg.RecipeVersion, childHashes)

	// Merkle short-circuit: an internal node whose sorted child hashes are
	// unchanged is not re-summarized.
	if existing, ok, err := d.getNode(ctx, args.id); err != nil {
		return false, err
	} else if ok && existing.ContentHash == ch {
		if d.obs != nil {
			d.obs.ShortCircuited()
		}
		return false, nil
	}

	if d.obs != nil {
		d.obs.Summarized()
	}
	summary, err := d.sum.Summarize(ctx, SummaryInput{
		Level:    args.level,
		Kind:     args.kind,
		Scope:    ScopeRef{Name: args.scopeName, ID: args.scopeID},
		Children: children,
		Budget:   d.cfg.Budget,
	})
	if err != nil {
		return false, fmt.Errorf("agent: summarize %s: %w", args.id, err)
	}

	// Guard emit: check the generated summary BEFORE it is hashed + written.
	ge, _ := applyGuard(ctx, d.guard, GuardInput{
		Stage: GuardEmit, TenantID: tenantOf(ctx),
		Scope: ScopeRef{Name: args.scopeName, ID: args.scopeID},
		Level: args.level, Text: summary,
	}, d.cfg.FailOpenGuard)
	if ge.Blocked {
		if d.obs != nil {
			d.obs.GuardBlocked()
		}
		return false, nil // fail-closed: drop (see ErrSummaryBlocked)
	}

	args.summaryText = ge.Text
	args.contentHash = ch
	args.children = make([]string, 0, len(children))
	for _, c := range children {
		args.children = append(args.children, c.ID)
	}
	if _, err := d.persistSummary(ctx, args); err != nil {
		return false, err
	}
	if d.obs != nil {
		d.obs.NodeBuilt()
	}
	return true, nil
}

// childDigests loads each member node and its summary text from CAS, returning
// the ChildDigest fan-in for summarization plus the sorted-for-Merkle child
// ContentHashes. Missing members (race / unmaterialized) are skipped.
func (d *Distiller) childDigests(ctx context.Context, ids []string) ([]ChildDigest, []string, error) {
	children := make([]ChildDigest, 0, len(ids))
	hashes := make([]string, 0, len(ids))
	for _, id := range ids {
		node, ok, err := d.getNode(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			continue
		}
		summary, err := d.retrieveSummary(ctx, node.SummaryHash)
		if err != nil {
			return nil, nil, err
		}
		children = append(children, ChildDigest{ID: node.ID, Kind: node.Kind, Summary: summary})
		hashes = append(hashes, node.ContentHash)
	}
	return children, hashes, nil
}

// retrieveSummary reads a summary blob's text from CAS by content hash.
func (d *Distiller) retrieveSummary(ctx context.Context, hash string) (string, error) {
	rc, err := d.cas.Retrieve(ctx, hash)
	if err != nil {
		return "", fmt.Errorf("agent: cas retrieve %s: %w", hash, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("agent: cas read %s: %w", hash, err)
	}
	return string(b), nil
}

// listNodes lists all digest_node rows at a given level for the tenant in ctx.
// It mirrors index.go's listVals typed path: List into a *[]<model> slice, then
// project each model through Binding.ValuesByColumn into a digestRow.
func (d *Distiller) listNodes(ctx context.Context, level int) ([]digestRow, error) {
	ent, ok := d.reg.Get(DigestEntity)
	if !ok {
		return nil, fmt.Errorf("agent: %q not registered", DigestEntity)
	}
	q := query.ListQuery{Where: query.Where{query.Eq("level", level)}}

	mt := ent.Binding.ModelType()
	slicePtr := reflect.New(reflect.SliceOf(mt))
	if err := d.fab.Relational().List(ctx, DigestEntity, q, slicePtr.Interface()); err != nil {
		return nil, err
	}
	slice := slicePtr.Elem()
	out := make([]digestRow, 0, slice.Len())
	for i := 0; i < slice.Len(); i++ {
		vals, err := ent.Binding.ValuesByColumn(slice.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		out = append(out, digestRowFromVals(vals))
	}
	return out, nil
}

// ensureParent appends parentID to a member node's ParentIDs (idempotently) and
// re-upserts the member when the link is newly added. Unlike linkChild (which
// mutates the parent's ChildIDs), this records the back-edge on the child so the
// next listNodes pass sees the cluster assignment.
func (d *Distiller) ensureParent(ctx context.Context, memberID, parentID string) error {
	node, ok, err := d.getNode(ctx, memberID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	for _, p := range node.ParentIDs {
		if p == parentID {
			return nil // already linked
		}
	}
	node.ParentIDs = append(node.ParentIDs, parentID)
	if err := d.upsertNode(ctx, node); err != nil {
		return err
	}
	// Mirror the edge onto the cluster parent's ChildIDs (no-op until the
	// cluster node is materialized by rollupNode).
	return d.linkChild(ctx, parentID, memberID)
}

// parseSemOrZero parses a formatted SemHash, returning 0 on empty/invalid input
// (a zero/empty embedding hashes to 0, so an unset SemHash buckets at prefix 0).
func parseSemOrZero(s string) uint64 {
	h, _ := ParseSemHash(s)
	return h
}

// isScopeID reports whether id is a scope (L1) node id (digest:1:scope:*).
func isScopeID(id string) bool {
	return strings.HasPrefix(id, "digest:1:scope:")
}

// parseScopeID splits a scope node id "digest:1:scope:<name>:<id>" into its
// scope name and scope value. Returns ("","") when the id is malformed.
func parseScopeID(id string) (name, value string) {
	const prefix = "digest:1:scope:"
	if !strings.HasPrefix(id, prefix) {
		return "", ""
	}
	rest := id[len(prefix):]
	i := strings.Index(rest, ":")
	if i < 0 {
		return rest, ""
	}
	return rest[:i], rest[i+1:]
}
