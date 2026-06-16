// Package event defines fabriq's versioned event envelope, its codec, and
// the upcaster chain that migrates old payload schemas at decode time.
//
// Exactly one envelope is appended to the transactional outbox per command;
// the relay publishes envelopes to Redis Streams; projections and the
// subscription hub consume them. The traceparent field carries the W3C
// trace context across the async hop so one trace spans
// command → outbox → relay → projection apply.
package event

import (
	"crypto/rand"
	"encoding/json"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// Envelope is the wire shape of one domain event.
type Envelope struct {
	// ID is a ULID: lexically sortable, globally unique, minted once at
	// command time.
	ID string `json:"id"`

	// TenantID scopes the event; consumers must never re-derive it from
	// the payload.
	TenantID string `json:"tenant_id"`

	// ScopeID is the optional secondary scope (sub-tenant partition). Empty when
	// unscoped. Carried so projections can stamp scope_id on derived rows.
	ScopeID string `json:"scopeId,omitempty"`

	// Aggregate is the registry entity name, e.g. "asset".
	Aggregate string `json:"aggregate"`

	// AggID identifies the aggregate instance.
	AggID string `json:"agg_id"`

	// Version is the aggregate's monotonic version after this event.
	Version int64 `json:"version"`

	// Type is the derived event type, e.g. "asset.updated".
	Type string `json:"type"`

	// At is the commit-side timestamp.
	At time.Time `json:"at"`

	// PayloadSchemaVersion versions the payload shape; upcasters migrate
	// old shapes forward at decode.
	PayloadSchemaVersion int `json:"payload_schema_version"`

	// Payload is the column-keyed JSON of the aggregate after the change
	// (empty object for deletes).
	Payload json.RawMessage `json:"payload"`

	// Traceparent is the W3C traceparent active when the command executed.
	Traceparent string `json:"traceparent,omitempty"`
}

// entropy is a monotonic ULID source: IDs minted in-process are strictly
// increasing, which the outbox relay relies on for ordered publishing.
var entropy = &ulidSource{r: ulid.Monotonic(rand.Reader, 0)}

type ulidSource struct {
	mu sync.Mutex
	r  *ulid.MonotonicEntropy
}

// NewID mints a ULID string.
func NewID() string {
	entropy.mu.Lock()
	defer entropy.mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy.r).String()
}
