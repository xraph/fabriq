// core/agent/digesttools.go
package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/xraph/fabriq/core/fabriqerr"
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

// resolveHammingThreshold is the maximum Hamming distance for a node to be
// included in the Near result set of Toolkit.Resolve.
const resolveHammingThreshold = 8

// listDigestRows pages through all digest_node rows for the tenant in ctx and
// returns them as []digestRow. It is shared by Map and Resolve to avoid
// duplicating the paging logic. Returns (nil, nil) when DigestEntity is not
// registered.
func (t *Toolkit) listDigestRows(ctx context.Context) ([]digestRow, error) {
	ent, ok := t.reg.Get(DigestEntity)
	if !ok {
		return nil, nil
	}
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
	return all, nil
}

// Map returns a compact outline of the tenant's context-distillation Merkle tree.
// Each line carries its ContentHash + SemHash. When req.KnownHashes is provided,
// nodes whose ContentHash matches the caller's known value are marked Unchanged=true
// (Merkle-diff re-grounding).
//
// If req.Scope is non-empty, only nodes whose scope_id equals req.Scope and the
// tenant root are included. If the DigestEntity is not registered, (nil, nil) is
// returned.
func (t *Toolkit) Map(ctx context.Context, req MapRequest) ([]MapLine, error) {
	// Page through all digest_node rows for this tenant.
	all, err := t.listDigestRows(ctx)
	if err != nil {
		return nil, err
	}
	if all == nil {
		return nil, nil
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
		line := mapLineFromRow(row)
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

// DigestChild is one child entry in a DigestView: the child's id, kind, and
// Merkle hashes. Summary is populated only when the Toolkit was configured with
// a CAS (otherwise empty).
type DigestChild struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Summary     string `json:"summary"`
	ContentHash string `json:"contentHash"`
	SemHash     string `json:"semHash"`
}

// DigestView is the drill-down result returned by Toolkit.Digest: the node's
// own MapLine, its summary text (from CAS), and its immediate children.
type DigestView struct {
	Node     MapLine       `json:"node"`
	Summary  string        `json:"summary"`
	Children []DigestChild `json:"children"`
}

// ResolveMatch is one entry in the Near list returned by Toolkit.Resolve.
type ResolveMatch struct {
	Node        MapLine `json:"node"`
	HammingBits int     `json:"hammingBits"`
}

// ResolveResult is the value returned by Toolkit.Resolve. Exact is set when a
// node's ContentHash equals the queried hash. Near holds nodes whose SemHash is
// within resolveHammingThreshold bits of the queried SemHash (sorted ascending
// by HammingBits, tiebroken by ID).
type ResolveResult struct {
	Exact *MapLine       `json:"exact,omitempty"`
	Near  []ResolveMatch `json:"near,omitempty"`
}

// Resolve performs a quick reference lookup without re-embedding:
//
//   - Exact: if any node's ContentHash equals hash, Exact is set to that node.
//   - Near: if hash parses as a 16-hex SemHash (via ParseSemHash), every node
//     whose SemHash is within resolveHammingThreshold Hamming bits is included in
//     Near, sorted ascending by HammingBits and then by ID.
//
// Both lookups are attempted independently. A node may appear in both Exact and
// Near if its ContentHash happens to be all-hex and its SemHash is also close.
// Returns (zero, nil) when DigestEntity is not registered.
func (t *Toolkit) Resolve(ctx context.Context, hash string) (ResolveResult, error) {
	all, err := t.listDigestRows(ctx)
	if err != nil {
		return ResolveResult{}, err
	}

	var res ResolveResult

	// Try to parse hash as a SemHash for the Near scan.
	parsed, semErr := ParseSemHash(hash)

	for _, row := range all {
		// Exact ContentHash match.
		if row.ContentHash == hash {
			line := mapLineFromRow(row)
			res.Exact = &line
		}

		// Near SemHash scan (only when hash parses as a valid SemHash).
		if semErr == nil {
			nodeParsed, parseErr := ParseSemHash(row.SemHash)
			if parseErr != nil {
				continue
			}
			dist := HammingDistance(parsed, nodeParsed)
			if dist <= resolveHammingThreshold {
				res.Near = append(res.Near, ResolveMatch{
					Node:        mapLineFromRow(row),
					HammingBits: dist,
				})
			}
		}
	}

	// Sort Near deterministically: ascending HammingBits, tiebreak by ID.
	sort.Slice(res.Near, func(i, j int) bool {
		if res.Near[i].HammingBits != res.Near[j].HammingBits {
			return res.Near[i].HammingBits < res.Near[j].HammingBits
		}
		return res.Near[i].Node.ID < res.Near[j].Node.ID
	})

	return res, nil
}

// mapLineFromRow converts a digestRow into a MapLine (no KnownHashes diff).
func mapLineFromRow(row digestRow) MapLine {
	return MapLine{
		ID:          row.ID,
		Level:       row.Level,
		Kind:        row.Kind,
		Scope:       row.ScopeID,
		ContentHash: row.ContentHash,
		SemHash:     row.SemHash,
	}
}

// getDigestRow reads a single digest_node row by id using the Toolkit's fabric.
// Returns (row, true, nil) on success, (zero, false, nil) when the row is not
// found, and (zero, false, err) for any other error.
func (t *Toolkit) getDigestRow(ctx context.Context, id string) (digestRow, bool, error) {
	ent, ok := t.reg.Get(DigestEntity)
	if !ok {
		return digestRow{}, false, fmt.Errorf("agent: digest: %q not registered", DigestEntity)
	}
	model := ent.Binding.NewModel()
	if err := t.fab.Relational().Get(ctx, DigestEntity, id, model); err != nil {
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

// retrieveSummary fetches the summary text for a content hash from CAS.
// Returns an empty string when t.cas is nil or when the hash is empty
// (graceful degradation — never an error in those two cases).
func (t *Toolkit) retrieveSummary(ctx context.Context, hash string) (string, error) {
	if t.cas == nil || hash == "" {
		return "", nil
	}
	rc, err := t.cas.Retrieve(ctx, hash)
	if err != nil {
		return "", fmt.Errorf("agent: digest: cas retrieve %q: %w", hash, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("agent: digest: read summary %q: %w", hash, err)
	}
	return string(b), nil
}

// Digest drills into one context-distillation node: it returns the node's
// MapLine, its summary text (retrieved from CAS by SummaryHash), and its
// immediate children with their own hashes and summaries.
//
// When the Toolkit was created without a CAS (Config.CAS == nil), Summary and
// child Summary fields are always empty — this is graceful degradation, not an
// error.
//
// Returns an error if nodeID is not found in the digest tree, or on any
// storage error.
func (t *Toolkit) Digest(ctx context.Context, nodeID string) (DigestView, error) {
	// 1. Load the target node.
	row, ok, err := t.getDigestRow(ctx, nodeID)
	if err != nil {
		return DigestView{}, err
	}
	if !ok {
		return DigestView{}, fmt.Errorf("agent: digest: node %q not found", nodeID)
	}

	// 2. Retrieve the node's summary text from CAS.
	summary, err := t.retrieveSummary(ctx, row.SummaryHash)
	if err != nil {
		return DigestView{}, err
	}

	// 3. Build children: load each child row, retrieve its summary.
	children := make([]DigestChild, 0, len(row.ChildIDs))
	for _, cid := range row.ChildIDs {
		crow, ok, err := t.getDigestRow(ctx, cid)
		if err != nil {
			return DigestView{}, err
		}
		if !ok {
			continue // tolerate dangling child references
		}
		childSummary, err := t.retrieveSummary(ctx, crow.SummaryHash)
		if err != nil {
			return DigestView{}, err
		}
		children = append(children, DigestChild{
			ID:          crow.ID,
			Kind:        crow.Kind,
			Summary:     childSummary,
			ContentHash: crow.ContentHash,
			SemHash:     crow.SemHash,
		})
	}

	return DigestView{
		Node:     mapLineFromRow(row),
		Summary:  summary,
		Children: children,
	}, nil
}
