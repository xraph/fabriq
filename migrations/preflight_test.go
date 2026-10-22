package migrations

import "testing"

func TestPreloadedLibraries(t *testing.T) {
	tests := []struct {
		setting string
		want    string
		has     bool
	}{
		{"timescaledb", "timescaledb", true},
		{"pg_stat_statements, timescaledb", "timescaledb", true},
		{`"timescaledb",pgaudit`, "timescaledb", true},
		{"$libdir/timescaledb.so", "timescaledb", true},
		{"pg_stat_statements", "timescaledb", false},
		{"", "timescaledb", false},
	}
	for _, tc := range tests {
		if got := preloadedLibraries(tc.setting)[tc.want]; got != tc.has {
			t.Errorf("preloadedLibraries(%q)[%q] = %v, want %v", tc.setting, tc.want, got, tc.has)
		}
	}
}
