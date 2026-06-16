package cluster

import (
	"context"

	"github.com/xraph/fabriq/core/livequery"
)

// Control op kinds carried gateway→shard.
const (
	OpSubscribe   = "subscribe"
	OpUnsubscribe = "unsubscribe"
	OpReanchor    = "reanchor"
)

// Control is a gateway→shard request, routed to the shard that owns the
// subscription's partition over that shard's control stream.
type Control struct {
	Op        string
	SubID     string
	GatewayID string
	TenantID  string
	Query     livequery.LiveQuery
	Cursor    *livequery.Cursor // OpReanchor
	Limit     int               // OpReanchor
}

// ControlBus routes control messages to the owning shard. Redis backs it in
// production (per-shard request streams); an in-memory implementation drives
// the multi-process harness.
type ControlBus interface {
	SendControl(ctx context.Context, shardID string, c Control) error
	Control(ctx context.Context, shardID string) (<-chan Control, func(), error)
}

// GatewayDelta is one delta tagged with its subscription, en route to a gateway.
type GatewayDelta struct {
	SubID string
	Delta livequery.LiveDelta
}

// DeltaBus routes deltas from shards back to gateways (per-gateway channels).
type DeltaBus interface {
	SendDelta(ctx context.Context, gatewayID string, d GatewayDelta) error
	Deltas(ctx context.Context, gatewayID string) (<-chan GatewayDelta, func(), error)
}
