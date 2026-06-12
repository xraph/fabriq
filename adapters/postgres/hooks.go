package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/xraph/grove/hook"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// tenantBackstop is the grove pre-query/pre-mutation hook guarding the
// pool path. In fabriq's architecture every tenant-table operation runs
// inside a stamped transaction (where RLS enforces isolation); the only
// surface left is grove's pool-path builders on the exported handle — and
// any such access to a tenant table is, by definition, a bug. The hook
// denies it with the typed sentinel and counts the trip (exported as a
// metric: a non-zero counter in production means fabriq itself leaked).
type tenantBackstop struct {
	mu sync.RWMutex
	// tables: every tenant table (registry + extras) — pool-path access denied.
	tables map[string]struct{}
	// unprotected: tenant tables WITHOUT RLS (e.g. the Timescale
	// hypertable) — raw SQL must name tenant_id explicitly.
	unprotected map[string]struct{}
	trips       atomic.Int64
}

func newTenantBackstop(reg *registry.Registry, extra []string) *tenantBackstop {
	b := &tenantBackstop{
		tables:      make(map[string]struct{}),
		unprotected: make(map[string]struct{}),
	}
	for _, ent := range reg.All() {
		b.tables[ent.Binding.Table] = struct{}{}
	}
	for _, t := range extra {
		b.tables[t] = struct{}{}
		b.unprotected[t] = struct{}{}
	}
	return b
}

func (b *tenantBackstop) isTenantTable(table string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.tables[table]
	return ok
}

// BeforeQuery implements hook.PreQueryHook.
func (b *tenantBackstop) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	return b.deny(qc)
}

// BeforeMutation implements hook.PreMutationHook.
func (b *tenantBackstop) BeforeMutation(_ context.Context, qc *hook.QueryContext, _ any) (*hook.HookResult, error) {
	return b.deny(qc)
}

func (b *tenantBackstop) deny(qc *hook.QueryContext) (*hook.HookResult, error) {
	if qc == nil || !b.isTenantTable(qc.Table) {
		return nil, nil
	}
	// Transaction path: fabriq stamped the tenant with SET LOCAL and RLS
	// enforces isolation in the database — the hook observes, RLS guards.
	// (grove >= a01144a fires hooks inside transactions and marks the
	// path; before that fix, tx queries bypassed hooks entirely.)
	if qc.InTransaction {
		return nil, nil
	}
	b.trips.Add(1)
	return &hook.HookResult{
		Decision: hook.Deny,
		Error: fmt.Errorf("fabriq: pool-path %v on tenant table %q (tenant tables are only reachable through stamped transactions): %w",
			qc.Operation, qc.Table, tenant.ErrTenantHookTripped),
	}, nil
}

// guardRawSQL rejects raw SQL that references an unprotected tenant table
// (no RLS, e.g. the Timescale hypertable) without naming tenant_id.
// RLS-protected tables need no guard here — the stamped transaction
// contains arbitrary SQL on them. Crude on purpose: raw SQL is an escape
// hatch, and escape hatches get seatbelts, not parsers.
func (b *tenantBackstop) guardRawSQL(sql string) error {
	lower := strings.ToLower(sql)
	b.mu.RLock()
	defer b.mu.RUnlock()
	for table := range b.unprotected {
		if strings.Contains(lower, strings.ToLower(table)) && !strings.Contains(lower, "tenant_id") {
			b.trips.Add(1)
			return fmt.Errorf("fabriq: raw SQL touches %q without a tenant_id predicate: %w",
				table, tenant.ErrTenantHookTripped)
		}
	}
	return nil
}
