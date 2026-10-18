package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/pathctx"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/internal/tracing"
)

// maxBatchLinks caps the links one "publish outbox batch" span carries, at
// the default batch size. A larger WithRelayBatch still publishes every row;
// only the links past the cap are dropped.
const maxBatchLinks = 128

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
	onBatch      func(result string, elapsed time.Duration)
	// sharedSchema is set in schema-per-tenant consolidation mode; when a
	// tenant schema is on ctx the drain stamps SET LOCAL search_path so the
	// bare fabriq_outbox resolves to that tenant's schema.
	sharedSchema string
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

// WithRelayOnBatch installs a per-batch duration callback (metrics). It fires
// once for every batch that found rows, with result "ok" or "error"; an empty
// poll reports nothing.
func WithRelayOnBatch(fn func(result string, elapsed time.Duration)) RelayOption {
	return func(r *Relay) { r.onBatch = fn }
}

// NewRelay builds a relay on the adapter's pool.
func NewRelay(a *Adapter, reg *registry.Registry, pub event.Publisher, opts ...RelayOption) *Relay {
	r := &Relay{
		pg:           a.pg,
		reg:          reg,
		pub:          pub,
		batch:        128,
		pollInterval: time.Second,
		sharedSchema: a.SharedSchema(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run drains until ctx ends.
func (r *Relay) Run(ctx context.Context) error {
	// The relay outlives the span its ctx was started under (forge's boot
	// span, or a caller's). Drop it so idle polls trace nothing; each batch
	// that finds rows starts its own root span.
	ctx = telemetry.Detach(ctx)
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

// drainOnce publishes one batch inside one transaction. The rows are
// selected before any span exists, so an empty poll records nothing; a batch
// that finds rows is published under a root "publish outbox batch" span.
func (r *Relay) drainOnce(ctx context.Context) (int, error) {
	tx, err := r.pg.BeginTxQuery(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fabriq: relay begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Schema-per-tenant: stamp the tenant's search_path so the bare
	// fabriq_outbox below resolves to its schema (the sweeper puts the schema
	// on ctx). Inert in database/single/shards mode.
	if r.sharedSchema != "" {
		if schema := pathctx.SchemaOrEmpty(ctx); schema != "" {
			if _, err := tx.NewRaw(`SELECT set_config('search_path', $1, true)`, schema+", "+r.sharedSchema).Exec(ctx); err != nil {
				return 0, fmt.Errorf("fabriq: relay stamp search_path: %w", err)
			}
		}
	}

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

	bctx, span := startBatchSpan(ctx, rows)
	start := time.Now()
	class, err := r.publishBatch(bctx, tx, rows)
	result := telemetry.ResultOK
	if err != nil {
		result = telemetry.ResultError
		telemetry.Fail(span, class, err)
	}
	span.End()
	if r.onBatch != nil {
		r.onBatch(result, time.Since(start))
	}
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// startBatchSpan starts the root Producer span for one outbox batch, linked
// to the span that wrote each row (its stored traceparent). Rows without a
// traceparent, and links past maxBatchLinks, are skipped.
func startBatchSpan(ctx context.Context, rows []outboxScanRow) (context.Context, trace.Span) {
	links := make([]trace.Link, 0, min(len(rows), maxBatchLinks))
	for _, row := range rows {
		if len(links) == maxBatchLinks {
			break
		}
		if l, ok := telemetry.LinkFromTraceparent(row.Traceparent); ok {
			links = append(links, l)
		}
	}
	return tracing.StartRoot(ctx, "publish outbox batch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithLinks(links...),
		trace.WithAttributes(
			telemetry.AttrMessagingSystem.String("redis"),
			attribute.Int("messaging.batch.message_count", len(rows)),
		),
	)
}

// publishBatch publishes and marks every row, then commits. On failure it
// returns a short class for the span status alongside the error.
func (r *Relay) publishBatch(ctx context.Context, tx *pgdriver.PgTx, rows []outboxScanRow) (string, error) {
	for _, row := range rows {
		env, err := row.envelope()
		if err != nil {
			return "invalid outbox row", err
		}
		channels, err := subscribe.ChannelsForEnvelope(r.reg, env)
		if err != nil {
			return "invalid outbox row", err
		}
		streamID, err := r.pub.Publish(ctx, env, channels)
		if err != nil {
			return "publish failed", fmt.Errorf("fabriq: relay publish %s: %w", env.ID, err)
		}
		if _, err := tx.NewRaw(
			`UPDATE fabriq_outbox SET published_at = now(), stream_id = $2 WHERE id = $1`,
			row.ID, streamID,
		).Exec(ctx); err != nil {
			return "mark published failed", fmt.Errorf("fabriq: relay mark published %s: %w", row.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return "commit failed", fmt.Errorf("fabriq: relay commit: %w", err)
	}
	return "", nil
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
