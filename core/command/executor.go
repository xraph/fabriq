package command

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/tenant"
)

// Executor implements Exec/ExecBatch over a Store.
type Executor struct {
	reg         *registry.Registry
	store       Store
	now         func() time.Time
	traceparent func(context.Context) string
	hooks       []LifecycleHook
	postCommit  []PostCommitHook
	observer    telemetry.CommandObserver
}

// ExecutorOption customizes an Executor.
type ExecutorOption func(*Executor)

// WithClock overrides the envelope timestamp source (tests).
func WithClock(now func() time.Time) ExecutorOption {
	return func(x *Executor) { x.now = now }
}

// WithTraceparent supplies the W3C traceparent extractor used to stamp
// envelopes; internal/otel provides the production implementation.
func WithTraceparent(fn func(context.Context) string) ExecutorOption {
	return func(x *Executor) { x.traceparent = fn }
}

// WithHooks appends lifecycle hooks to the executor's ordered chain. Each runs
// inside the write transaction after every change is staged; they fire in
// registration order and the first error aborts the command (and any batch).
func WithHooks(hooks ...LifecycleHook) ExecutorOption {
	return func(x *Executor) { x.hooks = append(x.hooks, hooks...) }
}

// WithPostCommitHooks appends hooks that run after the transaction commits
// successfully, receiving every Change produced. They never run on rollback.
func WithPostCommitHooks(hooks ...PostCommitHook) ExecutorOption {
	return func(x *Executor) { x.postCommit = append(x.postCommit, hooks...) }
}

// WithObserver records one duration observation per ExecBatch on o
// (fabriq_command_duration_seconds in fabriq's forge extension). Nil records
// nothing.
func WithObserver(o telemetry.CommandObserver) ExecutorOption {
	return func(x *Executor) { x.observer = o }
}

// NewExecutor wires the command plane.
func NewExecutor(reg *registry.Registry, store Store, opts ...ExecutorOption) (*Executor, error) {
	if reg == nil || store == nil {
		return nil, fmt.Errorf("fabriq: executor needs a registry and a store")
	}
	x := &Executor{
		reg:         reg,
		store:       store,
		now:         time.Now,
		traceparent: func(context.Context) string { return "" },
	}
	for _, opt := range opts {
		opt(x)
	}
	return x, nil
}

// Exec runs one command in its own transaction.
func (x *Executor) Exec(ctx context.Context, cmd Command) (Result, error) {
	results, err := x.ExecBatch(ctx, []Command{cmd})
	if err != nil {
		return Result{}, err
	}
	return results[0], nil
}

// ExecBatch runs N commands in ONE transaction: ordered, all-or-nothing.
//
// It runs under an "execute command" span, a child of the caller's span, so
// the traceparent the executor stamps into each outbox envelope names this
// span and the relay and consumers link back to it.
func (x *Executor) ExecBatch(ctx context.Context, cmds []Command) (results []Result, err error) {
	if len(cmds) == 0 {
		if _, terr := tenant.Require(ctx); terr != nil {
			return nil, terr
		}
		return []Result{}, nil
	}

	op := batchOp(cmds)
	attrs := []attribute.KeyValue{telemetry.AttrOp.String(op), telemetry.AttrBatchSize.Int(len(cmds))}
	if entity := batchEntity(cmds); entity != "" {
		attrs = append(attrs, telemetry.AttrEntity.String(entity))
	}
	ctx, span := telemetry.Tracer().Start(ctx, "execute command", trace.WithAttributes(attrs...))
	start := time.Now()
	result := telemetry.ResultOK
	defer func() {
		if err != nil && result == telemetry.ResultOK {
			result = telemetry.Classify(err)
		}
		span.SetAttributes(telemetry.AttrResult.String(result))
		if result == telemetry.ResultError {
			telemetry.Fail(span, "command failed", err)
		}
		span.End()
		if x.observer != nil {
			x.observer.ObserveCommand(op, result, time.Since(start))
		}
	}()

	if _, terr := tenant.Require(ctx); terr != nil {
		result = telemetry.ResultInvalid
		return nil, terr
	}

	// Validate everything we can before opening a transaction.
	prepared := make([]*preparedCommand, len(cmds))
	for i, cmd := range cmds {
		p, perr := x.prepare(ctx, cmd)
		if perr != nil {
			result = telemetry.ResultInvalid
			return nil, fmt.Errorf("command %d (%s %s): %w", i, cmd.Op.Verb(), cmd.Entity, perr)
		}
		prepared[i] = p
	}

	results = make([]Result, len(cmds))
	changes := make([]Change, 0, len(cmds))
	err = x.store.InTenantTx(ctx, func(ctx context.Context, tx Tx) error {
		// Reset on retry: InTenantTx may invoke the closure more than once.
		changes = changes[:0]
		for i, p := range prepared {
			res, change, aerr := x.apply(ctx, tx, p)
			if aerr != nil {
				return aerr
			}
			results[i] = res
			changes = append(changes, change)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, h := range x.postCommit {
		h.AfterCommit(ctx, changes)
	}
	return results, nil
}

// opName is the metric and span label for one command op. An op value the
// executor doesn't define labels as "unknown"; prepare rejects it, so the
// batch's result is always "invalid".
func opName(op Op) string {
	switch op {
	case OpCreate:
		return "create"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	case OpUpsert:
		return "upsert"
	default:
		return "unknown"
	}
}

// batchOp labels a batch with its op, or "mixed" when it combines several.
func batchOp(cmds []Command) string {
	op := cmds[0].Op
	for _, c := range cmds[1:] {
		if c.Op != op {
			return "mixed"
		}
	}
	return opName(op)
}

// batchEntity names the entity a batch writes, or "" when it spans several.
func batchEntity(cmds []Command) string {
	entity := cmds[0].Entity
	for _, c := range cmds[1:] {
		if c.Entity != entity {
			return ""
		}
	}
	return entity
}

// resolveOp maps the command op and the stored version to the effective
// write op and the event verb. OpUpsert becomes create (absent) or update
// (present); every other op is unchanged.
func resolveOp(op Op, current int64) (eff Op, verb string) {
	if op == OpUpsert {
		if current == 0 {
			return OpCreate, registry.VerbCreated
		}
		return OpUpdate, registry.VerbUpdated
	}
	return op, op.Verb()
}

// apply runs one prepared command inside the transaction: version check,
// row write, exactly one outbox envelope. It returns the Change so the caller
// can collect changes for post-commit hooks.
func (x *Executor) apply(ctx context.Context, tx Tx, p *preparedCommand) (Result, Change, error) {
	current, err := tx.CurrentVersion(ctx, p.entity, p.aggID)
	if err != nil {
		return Result{}, Change{}, err
	}
	if vErr := checkVersion(p, current); vErr != nil {
		return Result{}, Change{}, vErr
	}
	next := current + 1
	op, verb := resolveOp(p.cmd.Op, current)

	vals := p.stampedValues(next)
	if aErr := tx.ApplyChange(ctx, p.entity, op, p.aggID, next, vals); aErr != nil {
		return Result{}, Change{}, aErr
	}

	env, err := newEnvelope(p, next, vals, verb, x.now(), x.traceparent(ctx))
	if err != nil {
		return Result{}, Change{}, err
	}
	if err := tx.AppendOutbox(ctx, env); err != nil {
		return Result{}, Change{}, err
	}

	change := Change{Entity: p.entity, Op: op, Envelope: env}

	// Lifecycle hooks run in-transaction after the change is staged: they may
	// participate (write atomically via tx) or veto (an error rolls the whole
	// transaction back). They fire in order; the first error short-circuits.
	if len(x.hooks) > 0 {
		for _, h := range x.hooks {
			if err := h.OnChange(ctx, tx, change); err != nil {
				return Result{}, Change{}, fmt.Errorf("fabriq: lifecycle hook: %w", err)
			}
		}
	}

	return Result{AggID: p.aggID, Version: next, EventID: env.ID}, change, nil
}
