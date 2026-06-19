// core/agent/digesttools.go
package agent

import (
	"context"
	"sort"
)

// MapRequest parameters for Toolkit.Map.
type MapRequest struct {
	// Scope, when non-empty, restricts the outline to nodes whose scope_id matches
	// plus the tenant root. Empty means include all nodes.
	Scope string `json:"scope,omitempty"`
	// KnownHashes is the caller's local snapshot: map[nodeID]contentHash.
	// Nodes whose ContentHash matches the known value are marked Unchanged=true
	// (Merkle-diff re-grounding — transfer only the diff).
	KnownHashes map[string]string `json:"knownHashes,omitempty"`
}

// MapLine is one line of the bird's-eye digest-tree outline returned by Map.
type MapLine struct {
	ID          string `json:"id"`
	Level       int    `json:"level"`
	Kind        string `json:"kind"`
	Scope       string `json:"scope,omitempty"`
	ContentHash string `json:"contentHash"`
	SemHash     string `json:"semHash"`
	Unchanged   bool   `json:"unchanged,omitempty"`
	Summary     string `json:"summary,omitempty"`
}

// mapBatch is the page size used when listing digest nodes in Map.
const mapBatch = 500

// Map returns a compact outline of the tenant's context-distillation Merkle tree.
// Each line carries its ContentHash + SemHash. When req.KnownHashes is provided,
// nodes whose ContentHash matches the caller's known value are marked Unchanged=true
// (Merkle-diff re-grounding).
//
// If req.Scope is non-empty, only nodes whose scope_id equals req.Scope and the
// tenant root are included. If the DigestEntity is not registered, (nil, nil) is
// returned.
func (t *Toolkit) Map(ctx context.Context, req MapRequest) ([]MapLine, error) {
	ent, ok := t.reg.Get(DigestEntity)
	if !ok {
		return nil, nil
	}

	// Page through all digest_node rows for this tenant.
	var all []digestRow
	for offset := 0; ; offset += mapBatch {
		rows, err := listEntityVals(ctx, t.fab.Relational(), ent, mapBatch, offset)
		if err != nil {
			return nil, err
		}
		for _, m := range rows {
			all = append(all, digestRowFromVals(m))
		}
		if len(rows) < mapBatch {
			break
		}
	}

	// Build outline, optionally filtering by scope.
	lines := make([]MapLine, 0, len(all))
	for _, row := range all {
		if req.Scope != "" {
			// Include only the tenant root or nodes scoped to the requested scope_id.
			if row.ID != TenantRootID() && row.ScopeID != req.Scope {
				continue
			}
		}
		line := MapLine{
			ID:          row.ID,
			Level:       row.Level,
			Kind:        row.Kind,
			Scope:       row.ScopeID,
			ContentHash: row.ContentHash,
			SemHash:     row.SemHash,
		}
		if known, ok := req.KnownHashes[row.ID]; ok && known == row.ContentHash {
			line.Unchanged = true
		}
		lines = append(lines, line)
	}

	// Sort deterministically: Level ascending, then ID ascending.
	sort.Slice(lines, func(i, j int) bool {
		if lines[i].Level != lines[j].Level {
			return lines[i].Level < lines[j].Level
		}
		return lines[i].ID < lines[j].ID
	})

	return lines, nil
}
