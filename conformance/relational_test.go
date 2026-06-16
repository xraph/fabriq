package conformance

import "testing"

// TestRelationalCasesWellFormed guards the table itself: unique names, a
// non-nil Run, and any case with a Degrade also declares Requires (a
// degradation only triggers when a required capability is absent).
func TestRelationalCasesWellFormed(t *testing.T) {
	cases := RelationalCases()
	if len(cases) == 0 {
		t.Fatal("RelationalCases() is empty")
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		if tc.Name == "" {
			t.Error("case with empty Name")
		}
		if seen[tc.Name] {
			t.Errorf("duplicate case name %q", tc.Name)
		}
		seen[tc.Name] = true
		if tc.Run == nil {
			t.Errorf("case %q has nil Run", tc.Name)
		}
		if tc.Degrade != nil && len(tc.Requires) == 0 {
			t.Errorf("case %q declares Degrade but no Requires", tc.Name)
		}
	}
}
