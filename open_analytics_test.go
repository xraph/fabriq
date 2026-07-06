package fabriq

import "testing"

func TestAnalyticsConfig_EnabledByDSN(t *testing.T) {
	if (AnalyticsConfig{}).Enabled() {
		t.Fatal("empty DSN must be disabled")
	}
	if !(AnalyticsConfig{DSN: "postgres://x"}).Enabled() {
		t.Fatal("DSN present must be enabled")
	}
}
