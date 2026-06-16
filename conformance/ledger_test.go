// conformance/ledger_test.go
package conformance

import "testing"

// TestLedgerComplete is the structural gate that makes divergence reviewed:
// every Capability constant must appear in the ledger, and every ledger
// entry must name a known constant. A new capability with no ledger entry
// fails the build here.
func TestLedgerComplete(t *testing.T) {
	for _, c := range allCapabilities() {
		if !ledgerHas(c) {
			t.Errorf("capability %q has no ledger entry — document why each backend does or does not support it", c)
		}
	}
	known := make(map[Capability]bool, len(allCapabilities()))
	for _, c := range allCapabilities() {
		known[c] = true
	}
	for _, e := range ledger() {
		if !known[e.Cap] {
			t.Errorf("ledger references unknown capability %q", e.Cap)
		}
		if len(e.Havers) == 0 && len(e.Absent) == 0 {
			t.Errorf("ledger entry %q documents neither havers nor absences", e.Cap)
		}
	}
}
