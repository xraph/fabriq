package forgeext

import (
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/projection"
)

// Config is the fabriq forge extension's configuration: the data-fabric config
// plus worker knobs. Build it with options; the extension overlays values from
// the config manager under extensions.fabriq.* at Register (options win).
type Config struct {
	Fabriq            fabriq.Config
	RunWorker         bool
	ReconcileInterval time.Duration
}

// Option is a functional option for Config.
type Option func(*Config)

// WithConfig sets the underlying fabriq data-fabric configuration.
func WithConfig(c fabriq.Config) Option { return func(o *Config) { o.Fabriq = c } }

// WithWorker enables or disables the background reconcile worker.
func WithWorker(on bool) Option { return func(o *Config) { o.RunWorker = on } }

// WithReconcileInterval sets the interval at which the background worker
// reconciles projection state.
func WithReconcileInterval(d time.Duration) Option {
	return func(o *Config) { o.ReconcileInterval = d }
}

// WithCustomAppliers appends consumer-supplied projection appliers to the
// fabriq config. They are unioned after the built-in declarative applier for
// their Target and MUST be pure (see projection.CustomApplier).
func WithCustomAppliers(a ...projection.CustomApplier) Option {
	return func(o *Config) { o.Fabriq.CustomAppliers = append(o.Fabriq.CustomAppliers, a...) }
}

// LoadConfig builds a fabriq.Config from a forge ConfigManager. prefix is ""
// for the top-level key contract (cmd/fabriq serve) or "extensions.fabriq."
// for the first-class host-app convention. Relocated and parameterized from
// cmd/fabriq's loadFabriqConfig; the elasticsearch.addrs GetStringSlice
// handling is preserved (confy does not split a comma env string into a Go
// slice).
func LoadConfig(cm forge.ConfigManager, prefix string) fabriq.Config {
	var cfg fabriq.Config
	if cm == nil {
		return cfg
	}
	bind := func(key string, target any) {
		if cm.IsSet(prefix + key) {
			_ = cm.Bind(prefix+key, target)
		}
	}
	bind("postgres", &cfg.Postgres)
	bind("shards", &cfg.Shards)
	bind("redis", &cfg.Redis)
	bind("falkordb", &cfg.FalkorDB)
	bind("elasticsearch", &cfg.Elasticsearch)
	bind("projections", &cfg.Projections)
	bind("subscriptions", &cfg.Subscriptions)
	bind("storage", &cfg.Storage)
	if cm.IsSet(prefix + "elasticsearch.addrs") {
		cfg.Elasticsearch.Addrs = cm.GetStringSlice(prefix + "elasticsearch.addrs")
	}
	return cfg
}
