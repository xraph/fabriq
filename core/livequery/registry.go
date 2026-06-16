package livequery

import "context"

// Registration is one durably-recorded live subscription: the descriptor a
// reassigned matcher shard rebuilds from after a node failure or rebalance.
type Registration struct {
	SubID     string
	TenantID  string
	Entity    string
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
	// ByPartition lists the subscriptions for a (tenant, entity) partition —
	// the failover rebuild query.
	ByPartition(ctx context.Context, tenantID, entity string) ([]Registration, error)
	// ByGateway lists a gateway's subscriptions — for gateway recovery.
	ByGateway(ctx context.Context, gatewayID string) ([]Registration, error)
}
