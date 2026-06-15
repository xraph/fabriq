package forgeext

import (
	"context"
	"fmt"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/migrations"
)

// primaryDSN returns the DSN for the primary Postgres instance.
// It prefers the explicit Postgres.DSN, falling back to the first shard.
func (e *Extension) primaryDSN() string {
	if e.cfg.Fabriq.Postgres.DSN != "" {
		return e.cfg.Fabriq.Postgres.DSN
	}
	if len(e.cfg.Fabriq.Shards) > 0 {
		return e.cfg.Fabriq.Shards[0].DSN
	}
	return ""
}

// Migrate runs fabriq's pending migrations forward (forge MigratableExtension).
//
// It opens a fresh grove Orchestrator against the primary DSN, runs all pending
// migrations, and translates the grove MigrateResult into a forge.MigrationResult.
func (e *Extension) Migrate(ctx context.Context) (*forge.MigrationResult, error) {
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, e.primaryDSN())
	if err != nil {
		return nil, err
	}
	defer func() { _ = closeFn() }()

	res, err := orch.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	// res.Applied is []*migrate.Migration — each has Group and Name.
	names := make([]string, 0, len(res.Applied))
	for _, m := range res.Applied {
		names = append(names, fmt.Sprintf("%s/%s", m.Group, m.Name))
	}

	return &forge.MigrationResult{
		Applied: len(res.Applied),
		Names:   names,
	}, nil
}

// Rollback rolls back the last applied fabriq migration (forge MigratableExtension).
//
// grove's Rollback rolls back exactly one migration (the most recently applied
// in the group). RolledBack will be 0 or 1.
func (e *Extension) Rollback(ctx context.Context) (*forge.MigrationResult, error) {
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, e.primaryDSN())
	if err != nil {
		return nil, err
	}
	defer func() { _ = closeFn() }()

	res, err := orch.Rollback(ctx)
	if err != nil {
		return nil, err
	}

	// res.Rollback is []*migrate.Migration — the rolled-back migrations.
	names := make([]string, 0, len(res.Rollback))
	for _, m := range res.Rollback {
		names = append(names, fmt.Sprintf("%s/%s", m.Group, m.Name))
	}

	return &forge.MigrationResult{
		RolledBack: len(res.Rollback),
		Names:      names,
	}, nil
}

// MigrationStatus returns the current state of fabriq's migrations grouped by
// migration group (forge MigratableExtension).
//
// grove's Status method returns []*migrate.GroupStatus, each with Applied and
// Pending []*migrate.MigrationStatus slices. This is translated to the forge
// type hierarchy: one *forge.MigrationGroupInfo per grove GroupStatus,
// with *forge.MigrationInfo entries in Applied and Pending.
func (e *Extension) MigrationStatus(ctx context.Context) ([]*forge.MigrationGroupInfo, error) {
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, e.primaryDSN())
	if err != nil {
		return nil, err
	}
	defer func() { _ = closeFn() }()

	statuses, err := orch.Status(ctx)
	if err != nil {
		return nil, err
	}

	groups := make([]*forge.MigrationGroupInfo, 0, len(statuses))
	for _, gs := range statuses {
		gi := &forge.MigrationGroupInfo{
			Name:    gs.Name,
			Applied: make([]*forge.MigrationInfo, 0, len(gs.Applied)),
			Pending: make([]*forge.MigrationInfo, 0, len(gs.Pending)),
		}

		for _, ms := range gs.Applied {
			gi.Applied = append(gi.Applied, &forge.MigrationInfo{
				Name:      ms.Migration.Name,
				Version:   ms.Migration.Version,
				Group:     ms.Migration.Group,
				Comment:   ms.Migration.Comment,
				Applied:   true,
				AppliedAt: ms.AppliedAt,
			})
		}

		for _, ms := range gs.Pending {
			gi.Pending = append(gi.Pending, &forge.MigrationInfo{
				Name:    ms.Migration.Name,
				Version: ms.Migration.Version,
				Group:   ms.Migration.Group,
				Comment: ms.Migration.Comment,
				Applied: false,
			})
		}

		groups = append(groups, gi)
	}

	return groups, nil
}
