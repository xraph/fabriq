// Package catalogtest is the catalog contract suite: the behaviors every
// catalog.Catalog implementation must exhibit, run against
// fabriqtest.FakeCatalog (unit) and the Postgres control store
// (integration) — the conformance-kit discipline: drift between fake and
// adapter is a failing test.
package catalogtest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// Factory returns a fresh, empty catalog per test.
type Factory func(t *testing.T) catalog.Catalog

func code(err error) fabriqerr.Code {
	var fe *fabriqerr.Error
	if errors.As(err, &fe) {
		return fe.Code
	}
	return ""
}

func entry(tenant string) catalog.Entry {
	return catalog.Entry{
		TenantID:  tenant,
		ClusterID: "cluster-a",
		Database:  "fabriq_" + tenant,
		State:     catalog.StatePending,
	}
}

// Run executes the full contract against the implementation.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	ctx := context.Background()

	t.Run("GetUnknownIsNotFound", func(t *testing.T) {
		cat := factory(t)
		_, err := cat.Get(ctx, "ghost")
		if code(err) != fabriqerr.CodeNotFound {
			t.Fatalf("err = %v, want CodeNotFound", err)
		}
	})

	t.Run("CreateThenGetRoundTrips", func(t *testing.T) {
		cat := factory(t)
		created, err := cat.Put(ctx, entry("acme"))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if created.UpdatedAt.IsZero() {
			t.Fatal("create must stamp UpdatedAt")
		}
		got, err := cat.Get(ctx, "acme")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.ClusterID != "cluster-a" || got.Database != "fabriq_acme" ||
			got.State != catalog.StatePending || !got.UpdatedAt.Equal(created.UpdatedAt) {
			t.Fatalf("round-trip mismatch: %+v vs created %+v", got, created)
		}
	})

	t.Run("CreateExistingIsAlreadyExists", func(t *testing.T) {
		cat := factory(t)
		if _, err := cat.Put(ctx, entry("acme")); err != nil {
			t.Fatal(err)
		}
		_, err := cat.Put(ctx, entry("acme")) // zero UpdatedAt again = create
		if code(err) != fabriqerr.CodeAlreadyExists {
			t.Fatalf("err = %v, want CodeAlreadyExists", err)
		}
	})

	t.Run("UpdateCASRejectsStale", func(t *testing.T) {
		cat := factory(t)
		v1, err := cat.Put(ctx, entry("acme"))
		if err != nil {
			t.Fatal(err)
		}
		// A successful CAS update…
		v1.State = catalog.StateCreating
		v2, err := cat.Put(ctx, v1)
		if err != nil {
			t.Fatalf("cas update: %v", err)
		}
		if !v2.UpdatedAt.After(v1.UpdatedAt) {
			t.Fatal("update must advance UpdatedAt")
		}
		// …makes the old token stale.
		v1.State = catalog.StateActive
		if _, err := cat.Put(ctx, v1); code(err) != fabriqerr.CodeVersionConflict {
			t.Fatalf("stale put err = %v, want CodeVersionConflict", err)
		}
		got, _ := cat.Get(ctx, "acme")
		if got.State != catalog.StateCreating {
			t.Fatalf("stale put must not apply; state = %s", got.State)
		}
	})

	t.Run("StateTransitionsPersist", func(t *testing.T) {
		cat := factory(t)
		cur, err := cat.Put(ctx, entry("acme"))
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range []catalog.State{
			catalog.StateCreating, catalog.StateMigrating, catalog.StateActive,
			catalog.StateSuspended, catalog.StateFailed,
		} {
			cur.State = s
			cur.Version = "202607030030"
			next, err := cat.Put(ctx, cur)
			if err != nil {
				t.Fatalf("transition to %s: %v", s, err)
			}
			got, err := cat.Get(ctx, "acme")
			if err != nil {
				t.Fatal(err)
			}
			if got.State != s || got.Version != "202607030030" {
				t.Fatalf("after %s: got %+v", s, got)
			}
			cur = next
		}
	})

	t.Run("PutInvalidIsInvalidInput", func(t *testing.T) {
		cat := factory(t)
		bad := entry("acme")
		bad.Database = ""
		if _, err := cat.Put(ctx, bad); code(err) != fabriqerr.CodeInvalidInput {
			t.Fatalf("err = %v, want CodeInvalidInput", err)
		}
		bad = entry("acme")
		bad.State = "warp"
		if _, err := cat.Put(ctx, bad); code(err) != fabriqerr.CodeInvalidInput {
			t.Fatalf("err = %v, want CodeInvalidInput", err)
		}
	})

	t.Run("ListPaginatesInTenantOrder", func(t *testing.T) {
		cat := factory(t)
		const n = 25
		for i := 0; i < n; i++ {
			if _, err := cat.Put(ctx, entry(fmt.Sprintf("t-%03d", i))); err != nil {
				t.Fatal(err)
			}
		}
		var all []catalog.Entry
		cursor := catalog.Cursor("")
		pages := 0
		for {
			page, next, err := cat.List(ctx, cursor, 10)
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, page...)
			pages++
			if next == "" {
				break
			}
			if len(page) == 0 {
				t.Fatal("non-terminal page must not be empty")
			}
			cursor = next
			if pages > n {
				t.Fatal("cursor does not terminate")
			}
		}
		if len(all) != n {
			t.Fatalf("listed %d entries, want %d", len(all), n)
		}
		for i := 1; i < len(all); i++ {
			if all[i-1].TenantID >= all[i].TenantID {
				t.Fatalf("list not in stable tenant order: %q before %q", all[i-1].TenantID, all[i].TenantID)
			}
		}
	})
}
