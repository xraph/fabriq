package command

import (
	"context"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// ErasureRequest targets one aggregate's locally retained payloads. ExpectedVersion
// is the active row version, or zero when the caller expects no active row.
// ScopeID must exactly match current context, including an unscoped context.
type ErasureRequest struct {
	Entity          string `json:"entity"`
	AggregateID     string `json:"aggregate_id"`
	ScopeID         string `json:"scope_id"`
	ExpectedVersion int64  `json:"expected_version"`
	ReasonCode      string `json:"reason_code"`
}

var erasureName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
var erasureReason = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// Validate requires exact current scope and bounded non-content audit fields.
func (r ErasureRequest) Validate(ctx context.Context) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	if !erasureName.MatchString(r.Entity) || strings.TrimSpace(r.AggregateID) == "" || len(r.AggregateID) > 512 || !utf8.ValidString(r.AggregateID) || strings.ContainsAny(r.AggregateID, "\x00\r\n") || r.ExpectedVersion < 0 || !erasureReason.MatchString(r.ReasonCode) || r.ScopeID != tenant.ScopeOrEmpty(ctx) {
		return fabriqerr.New(fabriqerr.CodeInvalidInput, "Invalid erasure request.")
	}
	return ctx.Err()
}

// ErasureResource is the exact host authorization target, derived from context.
type ErasureResource struct {
	TenantID string
	Request  ErasureRequest
}

// ErasureGrant contains trusted audit identity, never caller-supplied payload.
type ErasureGrant struct {
	Actor             string
	AuthorityRevision string
}

// ErasureAuthorizer holds current permission across the synchronous transaction.
// It must invoke apply exactly once and wait for its result before returning.
// Implementations resolve the caller from trusted context and fail closed when
// current authority cannot be established. The storage layer supplies no default.
type ErasureAuthorizer interface {
	WithErasureAuthorization(context.Context, ErasureResource, func(context.Context, ErasureGrant) error) error
}

// CleanupStatus reports one destination independently. Unconfirmed means that
// local erasure provides no evidence about copies already delivered there.
type CleanupStatus struct {
	Target string `json:"target"`
	State  string `json:"state"`
}

// ErasureReceipt attests only payloads retained by the local command store.
// It makes no assertion about external streams, projections, caches or backups.
type ErasureReceipt struct {
	TenantID            string          `json:"tenant_id"`
	Request             ErasureRequest  `json:"request"`
	Actor               string          `json:"actor"`
	AuthorityRevision   string          `json:"authority_revision"`
	LastVersion         int64           `json:"last_version"`
	ErasedAt            time.Time       `json:"erased_at"`
	LocalPayloadsErased bool            `json:"local_payloads_erased"`
	LocalTargets        []string        `json:"local_targets"`
	OutboxRowsRedacted  int64           `json:"outbox_rows_redacted"`
	Downstream          []CleanupStatus `json:"downstream"`
}

// ErasureTx is the optional concrete command-store retention capability. Both
// operations share the ordinary command transaction and aggregate identity lock.
type ErasureTx interface {
	FenceAggregate(context.Context, *registry.Entity, string) error
	EraseAggregate(context.Context, *registry.Entity, ErasureRequest, ErasureGrant, time.Time) (ErasureReceipt, error)
}

// AggregateEraser is an optional facade capability for authorized local erasure.
// Implementations require configured current host authority and durable tombstones.
type AggregateEraser interface {
	EraseAggregate(context.Context, ErasureRequest) (ErasureReceipt, error)
}
