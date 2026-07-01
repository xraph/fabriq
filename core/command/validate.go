package command

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// preparedCommand is a validated command with its registry entity and
// column-keyed payload values, ready to apply inside the transaction.
type preparedCommand struct {
	cmd      Command
	entity   *registry.Entity
	tenantID string
	scopeID  string
	aggID    string
	vals     map[string]any // nil for deletes
}

// prepare performs all pre-transaction validation: registry lookup, kind
// check, payload binding, spec-driven validation, structural stamping.
func (x *Executor) prepare(ctx context.Context, cmd Command) (*preparedCommand, error) {
	tenantID, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	ent, ok := x.reg.Get(cmd.Entity)
	if !ok {
		return nil, fabriqerr.New(fabriqerr.CodeInvalidInput,
			"Unknown entity type.", fabriqerr.WithEntity(cmd.Entity, ""))
	}
	if ent.Spec.Kind != registry.KindAggregate {
		return nil, fmt.Errorf("fabriq: entity %q is a %s; the command plane only writes aggregates (document writes go through the document plane)",
			cmd.Entity, ent.Spec.Kind)
	}

	p := &preparedCommand{cmd: cmd, entity: ent, tenantID: tenantID, scopeID: tenant.ScopeOrEmpty(ctx), aggID: cmd.AggID}

	switch cmd.Op {
	case OpCreate:
		if p.aggID == "" {
			p.aggID = event.NewID()
		}
	case OpUpdate, OpDelete, OpUpsert:
		if p.aggID == "" {
			return nil, fmt.Errorf("fabriq: %s requires AggID", cmd.Op.Verb())
		}
	default:
		return nil, fmt.Errorf("fabriq: unknown op %d", cmd.Op)
	}

	if cmd.Op == OpDelete {
		if cmd.Payload != nil {
			return nil, fmt.Errorf("fabriq: delete must not carry a payload")
		}
		return p, nil
	}

	if cmd.Payload == nil {
		return nil, fmt.Errorf("fabriq: %s requires a payload", cmd.Op.Verb())
	}
	vals, err := ent.Binding.ValuesByColumn(cmd.Payload)
	if err != nil {
		return nil, err
	}
	// Tenant forgery check: a payload may leave tenant_id empty (it will be
	// stamped) but must never carry a different tenant.
	if v, ok := vals[registry.ColumnTenant].(string); ok && v != "" && v != tenantID {
		return nil, fmt.Errorf("fabriq: payload tenant_id %q does not match context tenant %q", v, tenantID)
	}
	if err := validateRequired(ent, vals); err != nil {
		return nil, err
	}
	skipTypes := cmd.SkipTypeCheck || (ent.Spec.Schema != nil && ent.Spec.Schema.NoTypeCheck)
	if !skipTypes {
		if err := registry.CoerceRow(ent, vals); err != nil {
			return nil, err
		}
	}
	if ent.Spec.Validate != nil {
		if err := ent.Spec.Validate(vals); err != nil {
			return nil, fmt.Errorf("fabriq: entity %q validation: %w", ent.Spec.Name, err)
		}
	}
	p.vals = vals
	return p, nil
}

// validateRequired enforces the spec-driven v1 rule: NOT NULL columns
// without defaults must not be zero-valued strings.
func validateRequired(ent *registry.Entity, vals map[string]any) error {
	for _, col := range ent.Binding.Required() {
		v, ok := vals[col]
		if !ok || v == nil {
			return fmt.Errorf("fabriq: entity %q: required column %q missing", ent.Spec.Name, col)
		}
		if s, isStr := v.(string); isStr && s == "" {
			return fmt.Errorf("fabriq: entity %q: required column %q is empty", ent.Spec.Name, col)
		}
	}
	return nil
}

// stampedValues returns the column values with the structural columns
// forced: id, tenant_id and version always come from the executor, never
// from the caller.
func (p *preparedCommand) stampedValues(version int64) map[string]any {
	if p.cmd.Op == OpDelete {
		return nil
	}
	out := make(map[string]any, len(p.vals))
	for k, v := range p.vals {
		out[k] = v
	}
	out[registry.ColumnID] = p.aggID
	out[registry.ColumnTenant] = p.tenantID
	out[registry.ColumnVersion] = version
	if p.scopeID != "" && p.entity.Binding.HasColumn(registry.ColumnScope) {
		out[registry.ColumnScope] = p.scopeID
	}
	return out
}
