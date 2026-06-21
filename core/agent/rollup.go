package agent

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
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

	// Build an in-memory index of L0 rows already listed so scope/cluster rolls
	// can resolve child refs without per-child getNode round-trips.
	idx := make(map[string]digestRow, len(l0s))
	for _, n := range l0s {
		idx[n.ID] = n
	}

	// 1. Cluster assignment. Primary bucket = top-p SimHash prefix. A bucket
	//    becomes a cluster only if its PRIMARY membership clears the noise floor.
	//    With ProbeRadius>0 a node ALSO joins any already-above-floor cluster
	//    whose prefix is within Hamming ProbeRadius of its primary prefix —
	//    probes never create a cluster, only widen an existing one.
	buckets := map[string][]string{} // primary: clusterID -> member L0 ids
	for _, n := range l0s {
		prefix := ClusterPrefix(parseSemOrZero(n.SemHash), d.cfg.ClusterBits)
		cid := ClusterID(prefix, d.cfg.ClusterBits)
		buckets[cid] = append(buckets[cid], n.ID)
	}
	cluster := map[string]bool{} // above-floor cluster ids (gated on PRIMARY count)
	for cid, members := range buckets {
		if NoiseFloorMet(len(members), d.cfg.NoiseFloor) {
			cluster[cid] = true
		}
	}
	// Effective membership: primary ∪ probe (probe only into existing clusters).
	effective := map[string]map[string]bool{}
	addEff := func(cid, mid string) {
		if effective[cid] == nil {
			effective[cid] = map[string]bool{}
		}
		effective[cid][mid] = true
	}
	for cid := range cluster {
		for _, mid := range buckets[cid] {
			addEff(cid, mid)
		}
	}
	if d.cfg.ProbeRadius > 0 {
		for _, n := range l0s {
			prefix := ClusterPrefix(parseSemOrZero(n.SemHash), d.cfg.ClusterBits)
			for _, pp := range probePrefixes(prefix, d.cfg.ClusterBits, d.cfg.ProbeRadius) {
				cid := ClusterID(pp, d.cfg.ClusterBits)
				if cluster[cid] {
					addEff(cid, n.ID)
				}
			}
		}
	}
	// Link effective members; build deterministic member lists for rollup.
	clusterMembers := map[string][]string{}
	for cid := range cluster {
		mids := make([]string, 0, len(effective[cid]))
		for mid := range effective[cid] {
			mids = append(mids, mid)
		}
		sort.Strings(mids)
		clusterMembers[cid] = mids
		for _, mid := range mids {
			if err = d.ensureParent(ctx, mid, cid); err != nil {
				return rep, fmt.Errorf("agent: rollup link cluster %s: %w", cid, err)
			}
		}
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
		rolled, scErr := d.rollupGroup(ctx, persistArgs{
			id: sid, level: LevelScope, kind: KindScopeNode,
			scopeName: name, scopeID: id,
			parents: []string{TenantRootID()},
		}, idx, scopeMembers[sid], d.cfg.ClusterBits)
		if scErr != nil {
			return rep, scErr
		}
		if rolled {
			rep.ScopeNodes++
		} else {
			rep.ShortCircuits++
		}
	}

	// 3. Cluster nodes: one per above-floor bucket, over effective membership.
	for cid := range cluster {
		rolled, cErr := d.rollupGroup(ctx, persistArgs{
			id: cid, level: LevelScope, kind: KindClusterNode,
			parents: []string{TenantRootID()},
		}, idx, clusterMembers[cid], d.cfg.ClusterBits)
		if cErr != nil {
			return rep, cErr
		}
		if rolled {
			rep.ClusterNodes++
		} else {
			rep.ShortCircuits++
		}
	}

	// 4. Cleanup: GC orphaned L1 nodes so a delete collapses the tree. A scope
	//    node with no current L0 members, or a cluster node whose bucket fell
	//    below the noise floor (including to zero), is deleted; its remaining
	//    members must not retain a stale parent link. Run BEFORE the tenant-root
	//    build so the root's children reflect only L1 nodes that still exist.
	// linkedClusters maps each cluster id to the L0s linking it as of the start of
	// this pass (l0s carries pre-pass ParentIDs). That is exactly what cleanup needs:
	// a cluster being collapsed existed in a prior pass, so its members' links are
	// already present here; clusters newly above-floor this pass are not collapsed.
	linkedClusters := map[string][]string{}
	for _, n := range l0s {
		for _, pid := range n.ParentIDs {
			if isClusterID(pid) {
				linkedClusters[pid] = append(linkedClusters[pid], n.ID)
			}
		}
	}
	survivors, cerr := d.cleanupL1(ctx, scopeMembers, linkedClusters, cluster)
	if cerr != nil {
		return rep, cerr
	}

	// If no L0 nodes remain, the whole tenant tree is empty — delete the
	// vestigial tenant root rather than summarizing empty children. deleteNode
	// tolerates a missing root, so this is idempotent across repeated sweeps.
	if len(l0s) == 0 {
		if derr := d.deleteNode(ctx, TenantRootID()); derr != nil {
			return rep, derr
		}
		return rep, nil
	}

	// 5. Tenant root: children = all current L1 (scope + cluster) nodes.
	// Add survivors (the freshly-written L1 nodes) to the index so the root roll
	// sees current ContentHashes without additional getNode reads.
	for _, n := range survivors {
		idx[n.ID] = n
	}
	l1IDs := make([]string, 0, len(survivors))
	for _, n := range survivors {
		l1IDs = append(l1IDs, n.ID)
	}
	rolled, err := d.rollupGroup(ctx, persistArgs{
		id: TenantRootID(), level: LevelTenant, kind: KindTenantNode,
	}, idx, l1IDs, d.cfg.ClusterBits)
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

// childRef is a child node's identity + hashes, loaded WITHOUT its CAS summary.
// The summary bytes are fetched lazily in rollupNode only when a re-summarize is
// actually needed (i.e. after the Merkle short-circuit has been ruled out).
type childRef struct {
	ID          string
	Kind        string
	SummaryHash string
	ContentHash string
}

// childRefsFrom resolves child refs from an already-loaded node index, skipping
// ids absent from the index (race / unmaterialized), preserving id order. This
// avoids a per-child getNode round-trip when Rollup has already listed the nodes.
func childRefsFrom(idx map[string]digestRow, ids []string) []childRef {
	refs := make([]childRef, 0, len(ids))
	for _, id := range ids {
		node, ok := idx[id]
		if !ok {
			continue
		}
		refs = append(refs, childRef{
			ID: node.ID, Kind: node.Kind,
			SummaryHash: node.SummaryHash, ContentHash: node.ContentHash,
		})
	}
	return refs
}

// rollupFromIndex resolves child refs from a preloaded node index and runs the
// shared short-circuit/summarize/persist path. Identical output to a getNode-per-child
// approach; eliminates per-child round-trips when Rollup has already listed the nodes.
func (d *Distiller) rollupFromIndex(ctx context.Context, args persistArgs, idx map[string]digestRow, memberIDs []string) (bool, error) {
	return d.rollupNode(ctx, args, childRefsFrom(idx, memberIDs))
}

// intermediateID derives a stable, collision-free id for an adaptive-depth
// sub-node: parent id + "#" + the sub-bucket prefix. Distinct from the flat
// ClusterID scheme so a depth sub-cluster (grouping L0s) can never collide with
// a tenant-root super-cluster (grouping L1 nodes).
func intermediateID(parentID string, subPrefix uint64) string {
	return parentID + "#" + FormatSemHash(subPrefix)
}

// groupFits reports whether a set of children can be summarized as one node:
// child-summary tokens within SummarizerInputBudget AND count within MaxFanIn.
// Tokens come from the cached digest row (idx) — never from CAS.
func (d *Distiller) groupFits(refs []childRef, idx map[string]digestRow) bool {
	if len(refs) > d.cfg.MaxFanIn {
		return false
	}
	total := 0
	for _, r := range refs {
		total += int(idx[r.ID].Tokens)
	}
	return total <= d.cfg.SummarizerInputBudget
}

// rollupGroup summarizes `members` under `args.id`, inserting intermediate
// `cluster`-kind nodes when the group overflows the fan-in cap. On overflow it
// partitions members by a longer SimHash prefix (prefixBits+Δ) into parent-scoped
// sub-nodes, recurses, then summarizes args.id over the sub-nodes. Reuses
// rollupNode (Merkle short-circuit + lazy CAS) at every node. Terminates when the
// group fits, the partition makes no progress, or the prefix reaches 64 bits.
func (d *Distiller) rollupGroup(ctx context.Context, args persistArgs, idx map[string]digestRow, members []string, prefixBits int) (bool, error) {
	refs := childRefsFrom(idx, members)
	if len(refs) == 0 || prefixBits >= 64 || d.groupFits(refs, idx) {
		return d.rollupNode(ctx, args, refs)
	}
	subBits := prefixBits + d.cfg.ClusterSubBits
	if subBits > 64 {
		subBits = 64
	}
	sub := map[uint64][]string{}
	var order []uint64
	for _, r := range refs {
		sp := ClusterPrefix(parseSemOrZero(idx[r.ID].SemHash), subBits)
		if _, seen := sub[sp]; !seen {
			order = append(order, sp)
		}
		sub[sp] = append(sub[sp], r.ID)
	}
	if len(sub) <= 1 {
		// No progress: members are identical in the top subBits — summarize as-is
		// rather than recurse forever.
		return d.rollupNode(ctx, args, refs)
	}
	if d.obs != nil {
		d.obs.Split()
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	childGroupIDs := make([]string, 0, len(order))
	for _, sp := range order {
		subID := intermediateID(args.id, sp)
		subArgs := persistArgs{
			id: subID, level: LevelScope, kind: KindClusterNode,
			scopeName: args.scopeName, scopeID: args.scopeID,
			parents: []string{args.id},
		}
		if _, err := d.rollupGroup(ctx, subArgs, idx, sub[sp], subBits); err != nil {
			return false, err
		}
		// Make the freshly-built sub-node visible to the parent roll.
		if row, ok, err := d.getNode(ctx, subID); err != nil {
			return false, err
		} else if ok {
			idx[subID] = row
		}
		childGroupIDs = append(childGroupIDs, subID)
	}
	return d.rollupNode(ctx, args, childRefsFrom(idx, childGroupIDs))
}

// rollupNode is the shared internal-node path: Merkle short-circuit, then (only
// if not short-circuited) fetch child summaries from CAS, summarize, guard-emit,
// persist. Returns rolled=true only when a new summary was written; false on a
// short-circuit or a guard block (fail-closed).
func (d *Distiller) rollupNode(ctx context.Context, args persistArgs, refs []childRef) (bool, error) {
	childHashes := make([]string, len(refs))
	for i, r := range refs {
		childHashes[i] = r.ContentHash
	}
	ch := RollupContentHash(d.cfg.RecipeVersion, childHashes)

	// Merkle short-circuit: an internal node whose sorted child hashes are
	// unchanged is not re-summarized — and pays NO CAS I/O.
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

	// We are re-summarizing: only now read each child's summary text from CAS.
	children := make([]ChildDigest, 0, len(refs))
	for _, r := range refs {
		summary, err := d.retrieveSummary(ctx, r.SummaryHash)
		if err != nil {
			return false, err
		}
		children = append(children, ChildDigest{ID: r.ID, Kind: r.Kind, Summary: summary})
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
	ge := applyGuard(ctx, d.guard, GuardInput{
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
	args.children = make([]string, 0, len(refs))
	for _, r := range refs {
		args.children = append(args.children, r.ID)
	}
	if err := d.persistSummary(ctx, args); err != nil {
		return false, err
	}
	if d.obs != nil {
		d.obs.NodeBuilt()
	}
	return true, nil
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

// cleanupL1 garbage-collects orphaned scope and cluster (L1) nodes after a
// rollup pass so a delete actually collapses the tree.
//
//   - A scope node whose id is not in scopeMembers (zero current L0 members) is
//     deleted.
//   - A cluster node whose bucket is now below the noise floor (members <
//     NoiseFloor, including a vanished bucket with zero members) is deleted, and
//     the stale cluster parent link is stripped from any remaining members so the
//     next pass does not re-derive a phantom membership.
//
// scopeMembers maps live scope ids -> member L0 ids; linkedClusters maps every
// cluster id currently referenced in L0 ParentIDs -> the L0 ids that link it
// (primary OR probe); cluster is the set of above-floor cluster ids that were
// (re)summarized this pass.
func (d *Distiller) cleanupL1(ctx context.Context, scopeMembers, linkedClusters map[string][]string, cluster map[string]bool) ([]digestRow, error) {
	l1s, err := d.listNodes(ctx, LevelScope)
	if err != nil {
		return nil, fmt.Errorf("agent: rollup cleanup list L1: %w", err)
	}
	survivors := make([]digestRow, 0, len(l1s))
	for _, n := range l1s {
		switch {
		case isScopeID(n.ID):
			if _, live := scopeMembers[n.ID]; !live {
				if err := d.deleteNode(ctx, n.ID); err != nil {
					return nil, fmt.Errorf("agent: rollup collapse scope %s: %w", n.ID, err)
				}
				continue // deleted → not a survivor
			}
		case isClusterID(n.ID):
			if !cluster[n.ID] {
				for _, mid := range linkedClusters[n.ID] {
					if err := d.removeParent(ctx, mid, n.ID); err != nil {
						return nil, fmt.Errorf("agent: rollup unlink cluster %s: %w", n.ID, err)
					}
				}
				if err := d.deleteNode(ctx, n.ID); err != nil {
					return nil, fmt.Errorf("agent: rollup collapse cluster %s: %w", n.ID, err)
				}
				continue // deleted → not a survivor
			}
			// Intermediate adaptive-depth nodes (id contains "#") are neither isScopeID
			// nor isClusterID, so this switch falls through to survivors. They are
			// rebuilt only when the partition geometry is unchanged (same member set and
			// SemHashes). When a node's members or SemHashes drift across passes, the
			// OLD parentID#oldPrefix node is neither rebuilt nor GC'd here — it lingers
			// as an orphan survivor (still a valid digest, harmless to recall, but can
			// transiently appear in the tenant-root child set). GC of orphaned
			// intermediates is a documented follow-up.
		}
		survivors = append(survivors, n)
	}
	return survivors, nil
}

// removeParent strips parentID from a member node's ParentIDs (idempotently),
// re-upserting the member only when the link was present. The inverse of
// ensureParent; used when a cluster node collapses below the noise floor.
func (d *Distiller) removeParent(ctx context.Context, memberID, parentID string) error {
	node, ok, err := d.getNode(ctx, memberID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	filtered := filterOut(node.ParentIDs, parentID)
	if len(filtered) == len(node.ParentIDs) {
		return nil // not linked
	}
	node.ParentIDs = filtered
	return d.upsertNode(ctx, node)
}

// probePrefixes returns every prefix within Hamming `radius` of `prefix` over the
// top `bits` bits (excluding `prefix` itself). Bounded by Σ_{i=1..radius} C(bits,i).
// radius<=0 or bits<=0 → nil (probing disabled).
func probePrefixes(prefix uint64, bits, radius int) []uint64 {
	if radius <= 0 || bits <= 0 {
		return nil
	}
	if radius > bits {
		radius = bits
	}
	var out []uint64
	var combo func(start, depth int, mask uint64)
	combo = func(start, depth int, mask uint64) {
		if depth > 0 {
			out = append(out, prefix^mask)
		}
		if depth == radius {
			return
		}
		for i := start; i < bits; i++ {
			combo(i+1, depth+1, mask|(uint64(1)<<uint(63-i)))
		}
	}
	combo(0, 0, 0)
	return out
}

// isClusterID reports whether id is a cluster (L1) node id (digest:1:cluster:*).
func isClusterID(id string) bool {
	return strings.HasPrefix(id, "digest:1:cluster:")
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
