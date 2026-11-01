package main

import (
	"testing"
)

func TestAnalyticsJobConfig(t *testing.T) {
	env := map[string]string{"FABRIQ_ANALYTICS_DSN": "postgres://analytics/db", "FABRIQ_ANALYTICS_HASH_SALT": "stable-secret", "FABRIQ_DOMAIN_PACK": "none"}
	cfg, r, err := analyticsJobConfig("postgres://source/db", "", func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Dynamic.Persist || cfg.Analytics.HashSalt != "stable-secret" || cfg.Analytics.DSN != env["FABRIQ_ANALYTICS_DSN"] || len(r.All()) != 0 {
		t.Fatal("operator config must preserve hosted sink, salt, dynamic-only registry and persisted definitions")
	}
	cfg, _, err = analyticsJobConfig("postgres://source/db", "duckdb://job.db", func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Analytics.DSN != "duckdb://job.db" {
		t.Fatal("explicit sink override lost")
	}
	env["FABRIQ_DYNAMIC_PERSIST"] = "false"
	cfg, _, err = analyticsJobConfig("postgres://source/db", "", func(key string) string { return env[key] })
	if err != nil || cfg.Dynamic.Persist {
		t.Fatal("explicit persistence setting lost")
	}
	env["FABRIQ_DOMAIN_PACK"] = "invalid"
	if _, _, err := analyticsJobConfig("postgres://source/db", "", func(key string) string { return env[key] }); err == nil {
		t.Fatal("invalid pack accepted")
	}
}
