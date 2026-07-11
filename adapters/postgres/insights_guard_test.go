package postgres

import "testing"

func TestPrecheckInsightsReadOnly(t *testing.T) {
	ok := []string{
		"SELECT count(*) FROM fabriq_insights_events",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"select props ->> 'a' from fabriq_insights_events;",
	}
	for _, s := range ok {
		if err := precheckInsightsReadOnly(s); err != nil {
			t.Errorf("want allow, got %v for %q", err, s)
		}
	}
	bad := []string{
		"DELETE FROM fabriq_insights_events",
		"INSERT INTO fabriq_insights_events VALUES (1)",
		"SELECT 1; DROP TABLE fabriq_insights_events",
		"WITH x AS (DELETE FROM fabriq_insights_events RETURNING *) SELECT * FROM x",
		"SELECT read_csv('/etc/passwd')",
		"UPDATE fabriq_insights_facts SET version = 0",
	}
	for _, s := range bad {
		if err := precheckInsightsReadOnly(s); err == nil {
			t.Errorf("want reject for %q", s)
		}
	}
}
