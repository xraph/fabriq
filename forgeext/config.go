package forgeext

import (
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/projection"
)

// Config is the fabriq forge extension's configuration: the data-fabric config
// plus worker knobs. Build it with options; the extension overlays values from
// the config manager under extensions.fabriq.* at Register (options win).
type Config struct {
	Fabriq            fabriq.Config
	RunWorker         bool
	ReconcileInterval time.Duration
	// BlobGCGrace protects freshly-created CAS entries and orphan bytes from
	// collection for this window. Zero falls back to 1h at run time.
	BlobGCGrace time.Duration
	// Embedder enables the embedding worker: each write to an entity with an
	// EmbedSpec is embedded + vector-upserted asynchronously. Nil = disabled.
	Embedder agent.Embedder
	// Summarizer enables the distillation worker: each write to an entity with
	// a DistillSpec is summarized into its digest tree asynchronously (debounced,
	// per-tenant single-flight). Nil = distillation disabled.
	Summarizer agent.Summarizer
	// Guard is the optional PII/guardrail seam for distillation (nil = identity).
	Guard agent.Guard
	// DistillFailOpenGuard flips the guard from fail-closed (default) to fail-open.
	DistillFailOpenGuard bool
	// DistillRecipeVersion salts the digest ContentHash; bump to rebuild the tree.
	DistillRecipeVersion string
	// DistillDebounce is the per-tenant coalescing window for L0+rollup sweeps.
	DistillDebounce time.Duration
	// DistillMaxWait caps how long a continuously-written tenant's sweep can be
	// deferred by debounce resets. Zero falls back to 10×DistillDebounce.
	// A value smaller than DistillDebounce is clamped up to DistillDebounce.
	DistillMaxWait time.Duration
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

// WithBlobGCGrace sets the grace window before an unreferenced CAS entry or
// orphan byte becomes GC-eligible. Defaults to 1h when zero.
func WithBlobGCGrace(d time.Duration) Option {
	return func(o *Config) { o.BlobGCGrace = d }
}

// WithEmbedder enables the embedding worker: each write to an entity with an
// EmbedSpec is embedded + vector-upserted asynchronously. Nil = disabled.
func WithEmbedder(e agent.Embedder) Option { return func(o *Config) { o.Embedder = e } }

// WithSummarizer enables the distillation worker: each write to an entity with
// a DistillSpec is summarized into its digest tree asynchronously. Nil = disabled.
func WithSummarizer(s agent.Summarizer) Option { return func(o *Config) { o.Summarizer = s } }

// WithGuard sets the optional PII/guardrail seam for distillation.
func WithGuard(g agent.Guard) Option { return func(o *Config) { o.Guard = g } }

// WithDistillFailOpenGuard flips the guard from fail-closed (default) to fail-open.
func WithDistillFailOpenGuard(v bool) Option { return func(o *Config) { o.DistillFailOpenGuard = v } }

// WithDistillRecipeVersion salts the digest ContentHash; bump to rebuild the tree.
func WithDistillRecipeVersion(v string) Option { return func(o *Config) { o.DistillRecipeVersion = v } }

// WithDistillDebounce sets the per-tenant coalescing window for L0+rollup sweeps.
func WithDistillDebounce(d time.Duration) Option { return func(o *Config) { o.DistillDebounce = d } }

// WithDistillMaxWait caps how long a continuously-written tenant's sweep can be
// deferred by debounce resets. Zero falls back to 10×DistillDebounce.
// A value smaller than DistillDebounce is clamped up to DistillDebounce.
func WithDistillMaxWait(d time.Duration) Option { return func(o *Config) { o.DistillMaxWait = d } }

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
