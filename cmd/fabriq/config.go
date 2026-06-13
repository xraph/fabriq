package main

import (
	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
)

// loadFabriqConfig builds the datastore Config from forge's config manager,
// which forge populates by auto-discovering config.yaml (+ config.local.yaml)
// from the search paths and overlaying FABRIQ_* environment variables on top
// (env wins — see setup in main.go and docs/.../configuration). Keys map
// straight onto the Config tree: FABRIQ_POSTGRES_DSN -> postgres.dsn,
// FABRIQ_REDIS_ADDR -> redis.addr, FABRIQ_ELASTICSEARCH_ADDRS ->
// elasticsearch.addrs, and so on.
//
// It binds each present top-level key onto a zero Config (so unset fields
// keep their zero value and fabriq.Open's defaults apply). Slice fields are
// pulled through the typed getter: confy decodes scalars and maps into a
// struct but does not split a comma-joined env string into a Go slice.
func loadFabriqConfig(cm forge.ConfigManager) fabriq.Config {
	var cfg fabriq.Config
	if cm == nil {
		return cfg
	}
	bind := func(key string, target any) {
		if cm.IsSet(key) {
			_ = cm.Bind(key, target)
		}
	}
	bind("postgres", &cfg.Postgres)
	bind("shards", &cfg.Shards)
	bind("redis", &cfg.Redis)
	bind("falkordb", &cfg.FalkorDB)
	bind("elasticsearch", &cfg.Elasticsearch)
	bind("projections", &cfg.Projections)
	bind("subscriptions", &cfg.Subscriptions)
	if cm.IsSet("elasticsearch.addrs") {
		cfg.Elasticsearch.Addrs = cm.GetStringSlice("elasticsearch.addrs")
	}
	return cfg
}
