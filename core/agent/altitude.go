// core/agent/altitude.go
package agent

import (
	"encoding/json"
	"fmt"
)

// Altitude controls which layer of the distillation tree is surfaced during
// recall. Higher altitudes surface coarser (more-summarised) nodes.
type Altitude int

const (
	// AltAuto lets the budget decide: descend to entities when affordable,
	// climb to the tenant digest when the budget is tight.
	AltAuto Altitude = iota
	// AltEntity surfaces raw source entities only (no digest nodes).
	AltEntity
	// AltScope surfaces scope/cluster digest nodes.
	AltScope
	// AltTenant surfaces the single tenant-root digest node.
	AltTenant
)

// resolveAltitude returns the effective Altitude. For AltAuto the budget drives
// descent: entities when they fit, else the backbone (scope/cluster) tier when it
// fits, else the single tenant digest. Non-auto altitudes pass through unchanged.
func resolveAltitude(req Altitude, entityTokens, backboneTokens, budget int) Altitude {
	if req != AltAuto {
		return req
	}
	if entityTokens <= budget {
		return AltEntity
	}
	if backboneTokens <= budget {
		return AltScope
	}
	return AltTenant
}

// isDigest reports whether the given entity name is the digest_node entity.
func isDigest(entity string) bool {
	return entity == DigestEntity
}

// digestLevel parses the "level" field from a digest node's row JSON.
// Returns 0 if the field is absent or the JSON is unparseable.
func digestLevel(row json.RawMessage) int {
	if len(row) == 0 {
		return 0
	}
	var v struct {
		Level int `json:"level"`
	}
	if err := json.Unmarshal(row, &v); err != nil {
		return 0
	}
	return v.Level
}

// digestCovers reports whether the digest node (its row JSON) covers the entity
// item — i.e. whether surfacing the digest makes that entity row redundant.
// Coverage is computed purely from the rows:
//
//   - tenant digest (level 2): covers every entity.
//   - L0 entity digest (level 0): covers exactly its source row
//     (sourceKind == ent.Entity && sourceId == ent.ID).
//   - scope digest (level 1, kind "scope"): covers an entity whose row carries
//     the scope's non-empty scopeId as one of its field values. This is a
//     heuristic — scope ids are specific enough that a value match is a reliable
//     proxy for "this entity belongs to that scope" without a join.
//   - cluster digest (level 1, kind "cluster"): coverage would need the entity's
//     SemHash bucket, which entity rows don't carry, so a cluster digest is
//     treated as NOT covering any entity (it never prunes on its own).
//
// An unparseable digest row covers nothing. The function is total: it never
// panics on nil/empty rows.
func digestCovers(digestRow json.RawMessage, ent ContextItem) bool {
	var d struct {
		Level      int    `json:"level"`
		Kind       string `json:"kind"`
		ScopeID    string `json:"scopeId"`
		SourceID   string `json:"sourceId"`
		SourceKind string `json:"sourceKind"`
	}
	if len(digestRow) == 0 {
		return false
	}
	if err := json.Unmarshal(digestRow, &d); err != nil {
		return false
	}

	switch d.Level {
	case LevelTenant:
		return true
	case LevelEntity:
		return ent.Entity == d.SourceKind && ent.ID == d.SourceID
	case LevelScope:
		if d.Kind != KindScopeNode || d.ScopeID == "" {
			return false // cluster (or unknown) L1 nodes do not prune entities
		}
		return rowHasValue(ent.Row, d.ScopeID)
	default:
		return false
	}
}

// rowHasValue reports whether the entity row JSON (an object) carries target as
// one of its top-level field values, compared as a formatted string. An
// unparseable or non-object row carries nothing.
func rowHasValue(row json.RawMessage, target string) bool {
	if len(row) == 0 {
		return false
	}
	var m map[string]any
	if err := json.Unmarshal(row, &m); err != nil {
		return false
	}
	for _, v := range m {
		if fmt.Sprintf("%v", v) == target {
			return true
		}
	}
	return false
}

// dedupeByAltitude filters items so that only the layer matching alt is kept.
//
//   - AltEntity: drop all digest_node items; keep only real entity rows.
//   - AltScope / AltTenant: keep every digest item, and drop an entity item only
//     if some present digest actually covers it (see digestCovers); entities no
//     digest covers are kept. If no digest item is present at all, items are left
//     as-is so the caller still gets something useful.
//   - AltAuto: pass through unchanged (callers must resolve AltAuto first).
//
// Coverage-aware pruning (AltScope/AltTenant) is exact for tenant and L0 digests
// and heuristic for scope digests (scopeId value-match). Limitation: cluster
// digests (level 1, kind "cluster") never prune entities because entity rows
// carry no SemHash bucket to test membership against — so an entity that is only
// represented by a cluster digest survives.
//
// The function is total: it never panics on nil/empty input or rows.
func dedupeByAltitude(items []ContextItem, alt Altitude) []ContextItem {
	if len(items) == 0 {
		return items
	}

	switch alt {
	case AltEntity:
		out := items[:0:0] // shared-backing-array-safe empty slice
		for _, it := range items {
			if !isDigest(it.Entity) {
				out = append(out, it)
			}
		}
		return out

	case AltScope, AltTenant:
		// Collect the present digest items.
		var digests []ContextItem
		for _, it := range items {
			if isDigest(it.Entity) {
				digests = append(digests, it)
			}
		}
		if len(digests) == 0 {
			// No digest available — leave items as-is so the caller still gets
			// something useful.
			return items
		}
		// Keep all digests; drop an entity only if a present digest covers it.
		out := items[:0:0]
		for _, it := range items {
			if isDigest(it.Entity) {
				out = append(out, it)
				continue
			}
			if !coveredByAny(digests, it) {
				out = append(out, it)
			}
		}
		return out

	default: // AltAuto or unknown — no-op pass-through
		return items
	}
}

// coveredByAny reports whether any of the digest items covers ent.
func coveredByAny(digests []ContextItem, ent ContextItem) bool {
	for _, d := range digests {
		if digestCovers(d.Row, ent) {
			return true
		}
	}
	return false
}
