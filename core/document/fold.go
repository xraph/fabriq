package document

import (
	"github.com/xraph/grove/crdt"
)

// FoldChange applies one change record onto state.Fields[c.Field] — the
// single fold every document store (postgres adapter, fakes) shares.
//
// Application rides grove's canonical crdt.ApplyChange, DEGRADING rather
// than failing when a record cannot be applied: logs written by older
// producers (the pre-ApplyChange lossy fold, older clients) may hold
// records with missing typed payloads, and one such row must never poison
// a document (a hard error here would make Sync/Snapshot/materialization
// fail forever — the log is already durable). Degradation ladder:
//
//  1. crdt.ApplyChange — full typed application;
//  2. legacy value-level LWW-style merge (the old fold's behavior);
//  3. deterministic skip (same outcome on every replica folding this log).
//
// The returned bool reports whether the record contributed state (false =
// skipped); callers may meter or log skips but must not fail on them.
func FoldChange(engine *crdt.MergeEngine, state *crdt.State, c *crdt.ChangeRecord) bool {
	merged, err := crdt.ApplyChange(engine, state.Fields[c.Field], c)
	if err == nil {
		state.Fields[c.Field] = merged
		return true
	}
	if len(c.Value) == 0 {
		return false
	}
	remote := &crdt.FieldState{Type: c.CRDTType, HLC: c.HLC, NodeID: c.NodeID, Value: c.Value}
	merged, err = engine.MergeField(state.Fields[c.Field], remote)
	if err != nil {
		return false
	}
	state.Fields[c.Field] = merged
	return true
}

// ValidateUpdate structurally validates one decoded update before it is
// durably appended: every record must be applicable (dry-run ApplyChange
// against empty state), so malformed payloads are rejected at the door
// instead of degrading later folds.
func ValidateUpdate(engine *crdt.MergeEngine, changes []crdt.ChangeRecord) error {
	probe := crdt.NewState("fabriq_validate", "probe")
	for i := range changes {
		merged, err := crdt.ApplyChange(engine, probe.Fields[changes[i].Field], &changes[i])
		if err != nil {
			return err
		}
		probe.Fields[changes[i].Field] = merged
	}
	return nil
}
