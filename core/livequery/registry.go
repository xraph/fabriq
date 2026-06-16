package livequery

import "context"

// Registration is one durably-recorded live subscription: the descriptor a
// reassigned matcher shard rebuilds from after a node failure or rebalance.
type Registration struct {
	SubID     string
	TenantID  string
	Entity    string
	Partition int // data partition (cluster.PartitionOf); set by the caller
	Mode      Mode
	Query     LiveQuery
	GatewayID string // the gateway terminating this subscription's connection
	Watermark string // last delivered stream position, for gapless resume
}

// SubscriptionRegistry durably records live subscriptions so the sharded matcher
// tier can recover them. When a partition is (re)assigned to a shard, the shard
// loads its subscriptions with ByPartition and re-snapshots them — the failover
// path that keeps a client's live query alive across a server restart. Single-
// node deployments do not need it.
type SubscriptionRegistry interface {
	// Put records or updates a subscription (idempotent on SubID).
	Put(ctx context.Context, r Registration) error
	// Delete removes a subscription on clean unsubscribe.
	Delete(ctx context.Context, subID string) error
	// ByPartition lists the subscriptions for a (tenant, entity) — convenience
	// for single-entity inspection.
	ByPartition(ctx context.Context, tenantID, entity string) ([]Registration, error)
	// ByPartitionNum lists every subscription in data partition p, across all
	// tenants/entities — the query a reassigned shard runs to rebuild what it
	// now owns after a failover or rebalance.
	ByPartitionNum(ctx context.Context, p int) ([]Registration, error)
	// ByGateway lists a gateway's subscriptions — for gateway recovery.
	ByGateway(ctx context.Context, gatewayID string) ([]Registration, error)
}
