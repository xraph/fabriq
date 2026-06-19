// core/agent/altitude.go
package agent

import "encoding/json"

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

// resolveAltitude returns the effective Altitude to use.
// For AltAuto: returns AltEntity when entityTokens fits within budget, otherwise
// AltTenant. Non-auto altitudes pass through unchanged.
func resolveAltitude(req Altitude, entityTokens, budget int) Altitude {
	if req != AltAuto {
		return req
	}
	if entityTokens <= budget {
		return AltEntity
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

// dedupeByAltitude filters items so that only the layer matching alt is kept.
//
//   - AltEntity: drop all digest_node items; keep only real entity rows.
//   - AltScope / AltTenant: drop all non-digest items WHEN at least one digest
//     item is present; if no digest items exist leave everything as-is.
//   - AltAuto: pass through unchanged (callers must resolve AltAuto first).
//
// The function is total: it never panics on nil/empty input.
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
		// Check whether at least one digest item is present.
		hasDigest := false
		for _, it := range items {
			if isDigest(it.Entity) {
				hasDigest = true
				break
			}
		}
		if !hasDigest {
			// No digest available — leave items as-is so the caller still gets
			// something useful.
			return items
		}
		// At least one digest present: drop all non-digest items.
		out := items[:0:0]
		for _, it := range items {
			if isDigest(it.Entity) {
				out = append(out, it)
			}
		}
		return out

	default: // AltAuto or unknown — no-op pass-through
		return items
	}
}
