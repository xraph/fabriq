//go:build integration

package migrations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/fabriq/migrations"
)

func TestAggregateRetentionRollback(t *testing.T) {
	for _, name := range []string{"unused", "permanent_fence", "redacted_outbox"} {
		used := name != "unused"
		t.Run(name, func(t *testing.T) {
			db := openPG(t)
			ctx := context.Background()
			orchestrator, err := migrations.NewOrchestrator(db)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = orchestrator.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if name == "permanent_fence" {
				if _, err = db.Exec(ctx, `INSERT INTO fabriq_aggregate_tombstones (tenant_id, entity, agg_id, scope_id, receipt) VALUES ('other-tenant', 'lessons', 'retired', 'private', '{"retained":"identity"}')`); err != nil {
					t.Fatal(err)
				}
			}
			if name == "redacted_outbox" {
				if _, err = db.Exec(ctx, `INSERT INTO fabriq_outbox (id, tenant_id, aggregate, agg_id, version, type, erased_at) VALUES ('redacted', 'other-tenant', 'lessons', 'retired', 1, 'created', now())`); err != nil {
					t.Fatal(err)
				}
			}
			_, err = orchestrator.Rollback(ctx)
			if used {
				if err == nil || !strings.Contains(err.Error(), "cannot be rolled back after erasure") {
					t.Fatalf("used retention rollback: %v", err)
				}
				if name == "redacted_outbox" {
					var erased bool
					if err = db.QueryRow(ctx, `SELECT erased_at IS NOT NULL FROM fabriq_outbox WHERE id='redacted'`).Scan(&erased); err != nil || !erased {
						t.Fatalf("redaction fence changed: %v %v", erased, err)
					}
					return
				}
				var receipt string
				if err = db.QueryRow(ctx, `SELECT receipt->>'retained' FROM fabriq_aggregate_tombstones WHERE agg_id='retired'`).Scan(&receipt); err != nil || receipt != "identity" {
					t.Fatalf("permanent fence changed: %q %v", receipt, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unused retention rollback: %v", err)
			}
			var exists bool
			if err = db.QueryRow(ctx, `SELECT to_regclass('fabriq_aggregate_tombstones') IS NOT NULL`).Scan(&exists); err != nil || exists {
				t.Fatalf("unused schema remained: %v %v", exists, err)
			}
			if _, err = orchestrator.Migrate(ctx); err != nil {
				t.Fatalf("reapply unused retention: %v", err)
			}
		})
	}
}

// retentionTransactionExecutor keeps Down on the transaction that owns the snapshot.
type retentionTransactionExecutor struct {
	migrate.Executor
	tx driver.Tx
}

func (e retentionTransactionExecutor) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	return e.tx.Exec(ctx, query, args...)
}

func TestAggregateRetentionRollbackRejectsStaleSnapshot(t *testing.T) {
	for _, isolation := range []driver.IsolationLevel{driver.LevelRepeatableRead, driver.LevelSerializable} {
		t.Run(isolation.String(), func(t *testing.T) {
			db := openPG(t)
			ctx := context.Background()
			orchestrator, err := migrations.NewOrchestrator(db)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = orchestrator.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			var retention *migrate.Migration
			for _, candidate := range migrations.Group().Migrations() {
				if candidate.Name == "aggregate_retention" {
					retention = candidate
					break
				}
			}
			if retention == nil {
				t.Fatal("aggregate retention migration missing")
			}
			tx, err := db.BeginTx(ctx, &driver.TxOptions{IsolationLevel: isolation})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			var count int
			if err = tx.QueryRow(ctx, `SELECT count(*) FROM fabriq_aggregate_tombstones`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("initial snapshot: count=%d, error=%v", count, err)
			}
			// The pool uses another connection while tx retains its empty snapshot.
			if _, err = db.Exec(ctx, `INSERT INTO fabriq_aggregate_tombstones (tenant_id, entity, agg_id, scope_id, receipt) VALUES ('other-tenant', 'lessons', 'retired', 'private', '{"retained":"identity"}')`); err != nil {
				t.Fatal(err)
			}
			if err = tx.QueryRow(ctx, `SELECT count(*) FROM fabriq_aggregate_tombstones`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("expected stale empty snapshot: count=%d, error=%v", count, err)
			}
			err = retention.Down(ctx, retentionTransactionExecutor{tx: tx})
			if err == nil || !strings.Contains(err.Error(), "rollback requires read committed isolation") {
				t.Fatalf("stale snapshot rollback: %v", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			var receipt string
			if err = db.QueryRow(ctx, `SELECT receipt->>'retained' FROM fabriq_aggregate_tombstones WHERE agg_id='retired'`).Scan(&receipt); err != nil || receipt != "identity" {
				t.Fatalf("permanent fence changed: %q %v", receipt, err)
			}
		})
	}
}
