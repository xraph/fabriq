//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

func TestSpatialDeleteByMetaRespectsTenant(t *testing.T) {
	s := newSpatialHarness(t)
	a, _ := tenant.WithTenant(context.Background(), "a")
	b, _ := tenant.WithTenant(context.Background(), "b")
	for _, ctx := range []context.Context{a, b} {
		if err := s.Upsert(ctx, "thing", "same", query.Geometry{WKT: "POINT (1 2)", SRID: 4326}, nil); err != nil {
			t.Fatalf("%v cause: %v", err, errors.Unwrap(err))
		}
	}
	if err := s.DeleteByMeta(a, "thing", nil); err != nil {
		t.Fatalf("%v cause: %v", err, errors.Unwrap(err))
	}
	if _, _, ok, err := s.Get(a, "thing", "same"); err != nil || ok {
		t.Fatal(ok, err)
	}
	if _, _, ok, err := s.Get(b, "thing", "same"); err != nil || !ok {
		t.Fatal(ok, err)
	}
}

func TestSpatialDeleteByMetaFiltersAndScopes(t *testing.T) {
	s := newSpatialHarness(t)
	ctx := scopedCtx(t, "cleanup", "")
	projectA := scopedCtx(t, "cleanup", "project-a")
	projectB := scopedCtx(t, "cleanup", "project-b")
	for _, row := range []struct {
		ctx                context.Context
		entity, id, source string
	}{
		{projectA, "thing", "matching", "drawing"},
		{projectA, "thing", "retained-source", "other"},
		{projectB, "thing", "retained-scope", "drawing"},
		{projectA, "other", "retained-entity", "drawing"},
	} {
		if err := s.Upsert(row.ctx, row.entity, row.id, query.Geometry{WKT: "POINT (1 2)", SRID: 4326}, map[string]any{"source_id": row.source}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteByMeta(context.Background(), "thing", nil); !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("missing tenant: got %v", err)
	}
	if err := s.DeleteByMeta(projectA, "thing", map[string]any{"source_id": make(chan int)}); err == nil {
		t.Fatal("invalid JSON filter accepted")
	}
	if err := s.DeleteByMeta(projectA, "thing", map[string]any{"source_id": "drawing"}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		entity, id string
		want       bool
	}{
		{"thing", "matching", false},
		{"thing", "retained-source", true},
		{"thing", "retained-scope", true},
		{"other", "retained-entity", true},
	} {
		if _, _, found, err := s.Get(ctx, row.entity, row.id); err != nil || found != row.want {
			t.Errorf("Get(%s, %s): found=%v, want=%v, err=%v", row.entity, row.id, found, row.want, err)
		}
	}
}
