package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
)

// Relay is the outbox relay: it drains unpublished outbox rows (FOR UPDATE
// SKIP LOCKED, ULID order) and publishes them through an event.Publisher,
// woken by LISTEN/NOTIFY with interval polling as the safety net.
//
// Delivery is at-least-once: rows are published before being marked, so a
// crash between the two replays the event; consumers are version-gated
// idempotent by contract. Run exactly one active relay (wrap Run in an
// Elector) — multiple relays are safe (SKIP LOCKED) but waste publishes.
type Relay struct {
	pg           *pgdriver.PgDB
	reg          *registry.Registry
	pub          event.Publisher
	batch        int
	pollInterval time.Duration
	onPublish    func(n int)
}

// RelayOption tunes the relay.
type RelayOption func(*Relay)

// WithRelayBatch sets the per-transaction drain batch (default 128).
func WithRelayBatch(n int) RelayOption {
	return func(r *Relay) {
		if n > 0 {
			r.batch = n
		}
	}
}

// WithRelayPollInterval sets the fallback poll cadence (default 1s; NOTIFY
// normally wakes the relay first).
func WithRelayPollInterval(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.pollInterval = d
		}
	}
}

// WithRelayOnPublish installs a per-batch callback (metrics).
func WithRelayOnPublish(fn func(n int)) RelayOption {
	return func(r *Relay) { r.onPublish = fn }
}

// NewRelay builds a relay on the adapter's pool.
func NewRelay(a *Adapter, reg *registry.Registry, pub event.Publisher, opts ...RelayOption) *Relay {
	r := &Relay{
		pg:           a.pg,
		reg:          reg,
		pub:          pub,
		batch:        128,
		pollInterval: time.Second,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run drains until ctx ends.
func (r *Relay) Run(ctx context.Context) error {
	wake := make(chan struct{}, 1)
	go notifyLoop(ctx, r.pg, "fabriq_outbox", wake)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		// Drain until empty: a full batch means more may be waiting.
		for {
			n, err := r.drainOnce(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			if r.onPublish != nil && n > 0 {
				r.onPublish(n)
			}
			if n < r.batch {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-ticker.C:
		}
	}
}

// DrainAll drains the outbox until empty (batches of the configured size)
// and returns the number of envelopes published — the single-pass form the
// catalog-mode sweeper runs under its per-tenant-database claim, where
// Run's LISTEN/NOTIFY loop would hold a connection per tenant forever.
func (r *Relay) DrainAll(ctx context.Context) (int, error) {
	total := 0
	for {
		n, err := r.drainOnce(ctx)
		if err != nil {
			return total, err
		}
		if r.onPublish != nil && n > 0 {
			r.onPublish(n)
		}
		total += n
		if n < r.batch {
			return total, nil
		}
	}
}

// outboxScanRow mirrors the relay's SELECT below.
type outboxScanRow struct {
	ID                   string `grove:"id"`
	TenantID             string `grove:"tenant_id"`
	Aggregate            string `grove:"aggregate"`
	AggID                string `grove:"agg_id"`
	Version              int64  `grove:"version"`
	Type                 string `grove:"type"`
	At                   string `grove:"at"`
	PayloadSchemaVersion int    `grove:"payload_schema_version"`
	Payload              string `grove:"payload"`
	Traceparent          string `grove:"traceparent"`
}

// drainOnce publishes one batch inside one transaction.
func (r *Relay) drainOnce(ctx context.Context) (int, error) {
	tx, err := r.pg.BeginTxQuery(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fabriq: relay begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var rows []outboxScanRow
	const sel = `SELECT id, tenant_id, aggregate, agg_id, version, type, at::text AS at,
			payload_schema_version, payload::text AS payload, traceparent
		FROM fabriq_outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`
	if err := tx.NewRaw(sel, r.batch).Scan(ctx, &rows); err != nil {
		return 0, fmt.Errorf("fabriq: relay select: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	for _, row := range rows {
		env, err := row.envelope()
		if err != nil {
			return 0, err
		}
		channels, err := subscribe.ChannelsForEnvelope(r.reg, env)
		if err != nil {
			return 0, err
		}
		streamID, err := r.pub.Publish(ctx, env, channels)
		if err != nil {
			return 0, fmt.Errorf("fabriq: relay publish %s: %w", env.ID, err)
		}
		if _, err := tx.NewRaw(
			`UPDATE fabriq_outbox SET published_at = now(), stream_id = $2 WHERE id = $1`,
			row.ID, streamID,
		).Exec(ctx); err != nil {
			return 0, fmt.Errorf("fabriq: relay mark published %s: %w", row.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("fabriq: relay commit: %w", err)
	}
	return len(rows), nil
}

// Backlog reports the unpublished outbox depth (metrics).
func (r *Relay) Backlog(ctx context.Context) (int64, error) {
	row := r.pg.QueryRow(ctx, `SELECT count(*) FROM fabriq_outbox WHERE published_at IS NULL`)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (row outboxScanRow) envelope() (event.Envelope, error) {
	at, err := parsePGTime(row.At)
	if err != nil {
		return event.Envelope{}, err
	}
	return event.Envelope{
		ID:                   row.ID,
		TenantID:             row.TenantID,
		Aggregate:            row.Aggregate,
		AggID:                row.AggID,
		Version:              row.Version,
		Type:                 row.Type,
		At:                   at,
		PayloadSchemaVersion: row.PayloadSchemaVersion,
		Payload:              json.RawMessage(row.Payload),
		Traceparent:          row.Traceparent,
	}, nil
}
