package registry

// ServiceName is the dependency-injection name the entity registry is
// registered under by fabriq's forge extension.
//
// It lives here, in the port layer, rather than next to the registration in
// forgeext, so a consumer can resolve the registry without importing fabriq's
// composition root. That import is what links every adapter — ClickHouse,
// Elasticsearch, pgx, Redis, Trove, FalkorDB — into a binary that only wanted
// to read entity specs.
const ServiceName = "fabriq-registry"
