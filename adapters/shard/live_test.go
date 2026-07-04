package shard_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/tenant"
)

type stubLive struct{ id string }

func (s stubLive) Snapshot(_ context.Context, _ livequery.LiveQuery, _ int) ([]livequery.Row, error) {
	return []livequery.Row{{AggID: s.id}}, nil
}
func (s stubLive) After(_ context.Context, _ livequery.LiveQuery, _ livequery.Cursor, _ int) ([]livequery.Row, error) {
	return []livequery.Row{{AggID: s.id}}, nil
}
func (s stubLive) Members(_ context.Context, _ livequery.LiveQuery) ([]string, error) {
	return []string{s.id}, nil
}

func TestLiveRouter_RoutesByTenant(t *testing.T) {
	set := shard.Single(shard.Shard{ID: "0", Live: stubLive{id: "row-0"}})
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	rows, err := shard.NewLive(set).Snapshot(ctx, livequery.LiveQuery{}, 10)
	if err != nil || len(rows) != 1 || rows[0].AggID != "row-0" {
		t.Fatalf("Snapshot = %v (%v)", rows, err)
	}
}
