package query

import (
	"encoding/json"
	"time"

	"github.com/xraph/fabriq/core/event"
)

// Delta is what Subscribe channels carry: one change notification, small
// enough to conflate, complete enough that simple UIs can patch state
// without a refetch (and rich UIs can refetch on demand).
type Delta struct {
	// StreamID is the transport position (Redis stream entry ID). It maps
	// 1:1 onto the SSE "id:" field so Last-Event-ID resume works.
	StreamID string `json:"stream_id"`

	// Channel the delta was delivered on.
	Channel string `json:"channel"`

	TenantID  string          `json:"tenant_id"`
	Aggregate string          `json:"aggregate"`
	AggID     string          `json:"agg_id"`
	Version   int64           `json:"version"`
	Type      string          `json:"type"`
	At        time.Time       `json:"at"`
	Payload   json.RawMessage `json:"payload"`
}

// DeltaFromEnvelope projects an event envelope onto a channel.
func DeltaFromEnvelope(channel, streamID string, env event.Envelope) Delta {
	return Delta{
		StreamID:  streamID,
		Channel:   channel,
		TenantID:  env.TenantID,
		Aggregate: env.Aggregate,
		AggID:     env.AggID,
		Version:   env.Version,
		Type:      env.Type,
		At:        env.At,
		Payload:   env.Payload,
	}
}

// SubscribeScope is a subscription request. The channel is always resolved
// server-side from the validated scope plus the context tenant — client
// input never names a channel or tenant directly.
type SubscribeScope struct {
	// Entity is the registry entity name.
	Entity string `json:"entity"`

	// Scope is a scope name declared in the entity's spec ("id", "site",
	// "tenant", ...).
	Scope string `json:"scope"`

	// ID is the scope id (aggregate id for "id" scopes, container id for
	// field scopes). Ignored for tenant scope — the tenant always comes
	// from the authenticated context.
	ID string `json:"id,omitempty"`
}
