// conformance/ledger.go
package conformance

// ledgerEntry documents one capability: which backends support it exactly,
// and the justification each absent backend has for legitimately diverging.
// Editing this table is the required, reviewed step for any fake↔real
// divergence.
type ledgerEntry struct {
	Cap    Capability
	Havers []string          // backend Name()s that support it exactly
	Absent map[string]string // backend Name() -> justification for NOT having it
}

// allCapabilities lists every Capability constant. Keep in sync with the
// consts in backend.go; TestLedgerComplete cross-checks against ledger().
func allCapabilities() []Capability {
	return []Capability{
		CapConcurrentTx,
		CapBucketedAgg,
		CapRelevanceScore,
		CapRawSQL,
		CapRawCypher,
		CapPersistence,
	}
}

// ledger is the single source of truth for fake↔real divergence.
func ledger() []ledgerEntry {
	return []ledgerEntry{
		{
			Cap:    CapConcurrentTx,
			Havers: []string{"postgres"},
			Absent: map[string]string{"fake": "FakeStore serializes transactions one at a time; real concurrency conflicts belong to the Postgres integration suite"},
		},
		{
			Cap:    CapBucketedAgg,
			Havers: []string{"postgres"},
			Absent: map[string]string{"fake": "FakeTS stores raw points only; time_bucket aggregation is Timescale-specific"},
		},
		{
			Cap:    CapRelevanceScore,
			Havers: []string{"elasticsearch"},
			Absent: map[string]string{"fake": "FakeSearch has no scorer; it returns id-order"},
		},
		{
			Cap:    CapRawSQL,
			Havers: []string{"postgres"},
			Absent: map[string]string{"fake": "FakeRelational.Query has no SQL engine"},
		},
		{
			Cap:    CapRawCypher,
			Havers: []string{"falkordb"},
			Absent: map[string]string{"fake": "FakeGraph.Query serves canned responses only"},
		},
		{
			Cap:    CapPersistence,
			Havers: []string{"postgres", "falkordb", "elasticsearch"},
			Absent: map[string]string{"fake": "the fakes are in-memory and reset on teardown"},
		},
	}
}

// ledgerHas reports whether c is documented in the ledger.
func ledgerHas(c Capability) bool {
	for _, e := range ledger() {
		if e.Cap == c {
			return true
		}
	}
	return false
}
