package command

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/registry"
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
func (x *Executor) ExecBatch(ctx context.Context, cmds []Command) ([]Result, error) {
	if _, err := tenant.Require(ctx); err != nil {
		return nil, err
	}
	if len(cmds) == 0 {
		return []Result{}, nil
	}

	// Validate everything we can before opening a transaction.
	prepared := make([]*preparedCommand, len(cmds))
	for i, cmd := range cmds {
		p, err := x.prepare(ctx, cmd)
		if err != nil {
			return nil, fmt.Errorf("command %d (%s %s): %w", i, cmd.Op.Verb(), cmd.Entity, err)
		}
		prepared[i] = p
	}

	results := make([]Result, len(cmds))
	changes := make([]Change, 0, len(cmds))
	err := x.store.InTenantTx(ctx, func(ctx context.Context, tx Tx) error {
		// Reset on retry: InTenantTx may invoke the closure more than once.
		changes = changes[:0]
		for i, p := range prepared {
			res, change, err := x.apply(ctx, tx, p)
			if err != nil {
				return err
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

// resolveOp maps the command op and the stored version to the effective
// write op and the event verb. OpUpsert becomes create (absent) or update
// (present); every other op is unchanged.
func resolveOp(op Op, current int64) (Op, string) {
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
