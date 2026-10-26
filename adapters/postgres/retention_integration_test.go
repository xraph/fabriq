//go:build integration

package postgres_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

type retentionAuthority func(context.Context, command.ErasureResource, func(context.Context, command.ErasureGrant) error) error

func (f retentionAuthority) WithErasureAuthorization(ctx context.Context, r command.ErasureResource, next func(context.Context, command.ErasureGrant) error) error {
	return f(ctx, r, next)
}
func permitRetention(ctx context.Context, _ command.ErasureResource, next func(context.Context, command.ErasureGrant) error) error {
	return next(ctx, command.ErasureGrant{Actor: "retention-worker", AuthorityRevision: "grant-v1"})
}

type retentionPublisher struct {
	calls            atomic.Int32
	entered, release chan struct{}
}

func (p *retentionPublisher) Publish(ctx context.Context, _ event.Envelope, _ []string) (string, error) {
	p.calls.Add(1)
	if p.entered != nil {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "1-0", nil
}

type retentionHook func(context.Context, command.Tx, command.Change) error

func (f retentionHook) OnChange(ctx context.Context, tx command.Tx, c command.Change) error {
	return f(ctx, tx, c)
}

func retentionRegistry() *registry.Registry {
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{Name: "project", Kind: registry.KindAggregate, Schema: &registry.DynamicSchema{Table: scopeTestTable, Columns: []registry.DynamicColumn{{Name: "label", Type: registry.ColText}, {Name: "scope_id", Type: registry.ColText}}}})
	return reg
}

func TestAggregateRetentionPostgres(t *testing.T) {
	owner, app, _ := newScopeHarness(t)
	reg := retentionRegistry()
	x, err := command.NewExecutor(reg, app, command.WithErasureAuthorizer(retentionAuthority(permitRetention)))
	require.NoError(t, err)
	ctx := scopedCtx(t, "retention-tenant", "project-a")
	one := int64(1)
	request := func(id string, version int64) command.ErasureRequest {
		return command.ErasureRequest{Entity: "project", AggregateID: id, ScopeID: "project-a", ExpectedVersion: version, ReasonCode: "expired"}
	}
	raw := func(t *testing.T, sql string, args ...any) int64 {
		t.Helper()
		var rows []struct {
			Count int64 `grove:"count"`
		}
		require.NoError(t, owner.Driver().NewRaw(sql, args...).Scan(context.Background(), &rows))
		require.Len(t, rows, 1)
		return rows[0].Count
	}
	assertErased := func(t *testing.T, id string) {
		t.Helper()
		require.Zero(t, raw(t, `SELECT count(*) AS count FROM ds_scope_test WHERE tenant_id='retention-tenant' AND id=$1`, id))
		require.Zero(t, raw(t, `SELECT count(*) AS count FROM fabriq_outbox WHERE tenant_id='retention-tenant' AND aggregate='project' AND agg_id=$1 AND (payload<>'{}'::jsonb OR traceparent<>'' OR erased_at IS NULL)`, id))
		for _, table := range []string{"fabriq_embeddings", "fabriq_geometries"} {
			require.Zero(t, raw(t, `SELECT count(*) AS count FROM `+table+` WHERE tenant_id='retention-tenant' AND entity='project' AND id=$1`, id))
		}
	}
	t.Run("all local payloads and irreversible restart", func(t *testing.T) {
		id := createProject(t, x, ctx, "private-one")
		_, err = x.Exec(ctx, command.Command{Entity: "project", AggID: id, Op: command.OpUpdate, ExpectedVersion: &one, Payload: map[string]any{"label": "private-two"}})
		require.NoError(t, err)
		embedding := make([]float32, 768)
		embedding[0] = 1
		require.NoError(t, app.Upsert(ctx, "project", id, embedding, map[string]any{"secret": "private-vector"}))
		spatial := postgres.NewSpatialAdapter(app)
		require.NoError(t, spatial.Upsert(ctx, "project", id, query.Geometry{WKT: "POINT(1 2)", SRID: 4326}, map[string]any{"secret": "private-spatial"}))
		r := request(id, 2)
		receipt, eraseErr := x.EraseAggregate(ctx, r)
		require.NoError(t, eraseErr)
		require.True(t, receipt.LocalPayloadsErased)
		require.EqualValues(t, 2, receipt.OutboxRowsRedacted)
		require.Equal(t, []string{"aggregate_row", "command_outbox", "embedding", "spatial"}, receipt.LocalTargets)
		for _, target := range receipt.Downstream {
			require.Equal(t, "unconfirmed", target.State)
		}
		assertErased(t, id)
		restart, newErr := command.NewExecutor(reg, app, command.WithErasureAuthorizer(retentionAuthority(permitRetention)))
		require.NoError(t, newErr)
		retained, retryErr := restart.EraseAggregate(ctx, r)
		require.NoError(t, retryErr)
		require.Equal(t, receipt, retained)
		for _, op := range []command.Op{command.OpCreate, command.OpUpdate, command.OpUpsert, command.OpDelete} {
			cmd := command.Command{Entity: "project", AggID: id, Op: op}
			if op != command.OpDelete {
				cmd.Payload = map[string]any{"label": "resurrect"}
			}
			_, writeErr := restart.Exec(ctx, cmd)
			require.ErrorIs(t, writeErr, fabriqerr.ErrAggregateErased)
		}
		require.ErrorIs(t, app.Upsert(ctx, "project", id, embedding, nil), fabriqerr.ErrAggregateErased)
		require.ErrorIs(t, spatial.Upsert(ctx, "project", id, query.Geometry{WKT: "POINT(1 2)", SRID: 4326}, nil), fabriqerr.ErrAggregateErased)
		require.Error(t, app.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
			_, e := tx.NewRaw(`DELETE FROM fabriq_aggregate_tombstones WHERE tenant_id=$1 AND entity='project' AND agg_id=$2`, "retention-tenant", id).Exec(ctx)
			return e
		}))
		assertErased(t, id)
	})
	t.Run("hidden foreign scope and tenant isolation", func(t *testing.T) {
		id := createProject(t, x, ctx, "private-a")
		foreign := scopedCtx(t, "retention-tenant", "project-b")
		r := request(id, 0)
		r.ScopeID = "project-b"
		_, eraseErr := x.EraseAggregate(foreign, r)
		require.ErrorIs(t, eraseErr, fabriqerr.ErrVersionConflict)
		require.EqualValues(t, 1, raw(t, `SELECT count(*) AS count FROM ds_scope_test WHERE id=$1 AND label='private-a'`, id))
		embedding := make([]float32, 768)
		embedding[0] = 1
		require.NoError(t, app.Upsert(foreign, "project", id, embedding, map[string]any{"secret": "private-b"}))
		_, eraseErr = x.EraseAggregate(ctx, request(id, 1))
		require.ErrorIs(t, eraseErr, fabriqerr.ErrVersionConflict)
		require.EqualValues(t, 1, raw(t, `SELECT count(*) AS count FROM fabriq_embeddings WHERE id=$1 AND meta->>'secret'='private-b'`, id))
		require.EqualValues(t, 1, raw(t, `SELECT count(*) AS count FROM ds_scope_test WHERE id=$1 AND label='private-a'`, id))
		other := scopedCtx(t, "other-tenant", "project-a")
		_, eraseErr = x.EraseAggregate(other, request(id, 0))
		require.NoError(t, eraseErr)
		require.EqualValues(t, 1, raw(t, `SELECT count(*) AS count FROM ds_scope_test WHERE id=$1 AND label='private-a'`, id))
	})
	t.Run("already deleted aggregate history", func(t *testing.T) {
		id := createProject(t, x, ctx, "private-before-delete")
		_, deleteErr := x.Exec(ctx, command.Command{Entity: "project", AggID: id, Op: command.OpDelete, ExpectedVersion: &one})
		require.NoError(t, deleteErr)
		receipt, eraseErr := x.EraseAggregate(ctx, request(id, 0))
		require.NoError(t, eraseErr)
		require.EqualValues(t, 2, receipt.OutboxRowsRedacted)
		assertErased(t, id)
	})
	t.Run("current authority and rollback", func(t *testing.T) {
		id := createProject(t, x, ctx, "rollback-private")
		denied, makeErr := command.NewExecutor(reg, app, command.WithErasureAuthorizer(retentionAuthority(func(context.Context, command.ErasureResource, func(context.Context, command.ErasureGrant) error) error {
			return fabriqerr.New(fabriqerr.CodePermissionDenied, "Denied.")
		})))
		require.NoError(t, makeErr)
		_, eraseErr := denied.EraseAggregate(ctx, request(id, 1))
		require.Error(t, eraseErr)
		_, ddlErr := owner.Driver().Exec(context.Background(), `CREATE FUNCTION fail_retention_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.agg_id='`+id+`' THEN RAISE EXCEPTION 'test rollback'; END IF; RETURN NEW; END $$`)
		require.NoError(t, ddlErr)
		_, ddlErr = owner.Driver().Exec(context.Background(), `CREATE TRIGGER fail_retention_test BEFORE INSERT ON fabriq_aggregate_tombstones FOR EACH ROW EXECUTE FUNCTION fail_retention_test()`)
		require.NoError(t, ddlErr)
		_, eraseErr = x.EraseAggregate(ctx, request(id, 1))
		require.Error(t, eraseErr)
		require.EqualValues(t, 1, raw(t, `SELECT count(*) AS count FROM ds_scope_test WHERE id=$1 AND label='rollback-private'`, id))
		require.EqualValues(t, 1, raw(t, `SELECT count(*) AS count FROM fabriq_outbox WHERE agg_id=$1 AND payload->>'label'='rollback-private' AND erased_at IS NULL`, id))
		_, ddlErr = owner.Driver().Exec(context.Background(), `DROP TRIGGER fail_retention_test ON fabriq_aggregate_tombstones`)
		require.NoError(t, ddlErr)
	})
	t.Run("command version fence", func(t *testing.T) {
		id := createProject(t, x, ctx, "before")
		entered, release := make(chan struct{}), make(chan struct{})
		writer, makeErr := command.NewExecutor(reg, app, command.WithHooks(retentionHook(func(context.Context, command.Tx, command.Change) error { close(entered); <-release; return nil })))
		require.NoError(t, makeErr)
		wrote := make(chan error, 1)
		go func() {
			_, e := writer.Exec(ctx, command.Command{Entity: "project", AggID: id, Op: command.OpUpdate, ExpectedVersion: &one, Payload: map[string]any{"label": "latest"}})
			wrote <- e
		}()
		<-entered
		erased := make(chan error, 1)
		go func() { _, e := x.EraseAggregate(ctx, request(id, 1)); erased <- e }()
		select {
		case e := <-erased:
			t.Fatalf("erasure passed held command: %v", e)
		case <-time.After(30 * time.Millisecond):
		}
		close(release)
		require.NoError(t, <-wrote)
		require.ErrorIs(t, <-erased, fabriqerr.ErrVersionConflict)
		require.EqualValues(t, 1, raw(t, `SELECT count(*) AS count FROM ds_scope_test WHERE id=$1 AND label='latest'`, id))
		_, eraseErr := x.EraseAggregate(ctx, request(id, 2))
		require.NoError(t, eraseErr)
		assertErased(t, id)
	})
	t.Run("relay pending and in flight", func(t *testing.T) {
		// Clear unrelated pending fixtures so the barrier targets one exact event.
		_, updateErr := owner.Driver().Exec(context.Background(), `UPDATE fabriq_outbox SET published_at=now() WHERE erased_at IS NULL`)
		require.NoError(t, updateErr)
		id := createProject(t, x, ctx, "in-flight-private")
		publisher := &retentionPublisher{entered: make(chan struct{}), release: make(chan struct{})}
		relay := postgres.NewRelay(owner, reg, publisher, postgres.WithRelayBatch(1))
		drained := make(chan error, 1)
		go func() { _, e := relay.DrainAll(context.Background()); drained <- e }()
		<-publisher.entered
		erased := make(chan error, 1)
		go func() { _, e := x.EraseAggregate(ctx, request(id, 1)); erased <- e }()
		select {
		case e := <-erased:
			t.Fatalf("erasure returned before active relay drained: %v", e)
		case <-time.After(30 * time.Millisecond):
		}
		close(publisher.release)
		require.NoError(t, <-drained)
		require.NoError(t, <-erased)
		assertErased(t, id)
		pending := createProject(t, x, ctx, "never-publish-private")
		_, eraseErr := x.EraseAggregate(ctx, request(pending, 1))
		require.NoError(t, eraseErr)
		empty := &retentionPublisher{}
		n, drainErr := postgres.NewRelay(owner, reg, empty).DrainAll(context.Background())
		require.NoError(t, drainErr)
		require.Zero(t, n)
		require.Zero(t, empty.calls.Load())
	})
}
