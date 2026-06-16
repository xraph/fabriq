package conformance

import (
	"errors"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// SeedAsset is one asset to create before a relational case runs. Ids are
// minted by the command path; cases address rows by Name.
type SeedAsset struct {
	Name, Kind, Serial, SiteID string
}

// RelationalCase is one conformance scenario for query.RelationalQuerier.
type RelationalCase struct {
	Name     string
	Requires []Capability // capabilities the happy path needs
	Degrade  *Degradation // when a Required capability is absent, assert this instead of skipping
	Seed     []SeedAsset
	// Run executes the read under test. ids maps SeedAsset.Name -> minted id.
	Run       func(env *Env, ids map[string]string) ([]*domain.Asset, error)
	WantNames []string // expected asset Names, in order
}

// RelationalCases is the canonical relational conformance table. Universal
// cases (no Requires) must pass on EVERY backend; capability-gated cases run
// where supported and skip/assert-degrade elsewhere.
func RelationalCases() []RelationalCase {
	fleet := []SeedAsset{
		{Name: "Main Pump", Kind: "pump", Serial: "P-1"},
		{Name: "Backup Pump", Kind: "pump", Serial: "P-2"},
		{Name: "Inlet Valve", Kind: "valve", Serial: "V-1"},
	}
	return []RelationalCase{
		{
			Name: "get by id",
			Seed: fleet,
			Run: func(env *Env, ids map[string]string) ([]*domain.Asset, error) {
				var a domain.Asset
				if err := env.Relational.Get(env.Ctx, "asset", ids["Main Pump"], &a); err != nil {
					return nil, err
				}
				return []*domain.Asset{&a}, nil
			},
			WantNames: []string{"Main Pump"},
		},
		{
			Name: "getmany preserves id order and skips missing",
			Seed: fleet,
			Run: func(env *Env, ids map[string]string) ([]*domain.Asset, error) {
				var got []*domain.Asset
				err := env.Relational.GetMany(env.Ctx, "asset",
					[]string{ids["Inlet Valve"], ids["Main Pump"], "missing"}, &got)
				return got, err
			},
			WantNames: []string{"Inlet Valve", "Main Pump"},
		},
		{
			Name: "list LIKE filter ordered by name",
			Seed: fleet,
			Run: func(env *Env, ids map[string]string) ([]*domain.Asset, error) {
				var got []*domain.Asset
				err := env.Relational.List(env.Ctx, "asset", query.ListQuery{
					Where: query.Where{query.Like("name", "%Pump")}, OrderBy: "name",
				}, &got)
				return got, err
			},
			WantNames: []string{"Backup Pump", "Main Pump"},
		},
		{
			Name: "list equality filter on kind",
			Seed: fleet,
			Run: func(env *Env, ids map[string]string) ([]*domain.Asset, error) {
				var got []*domain.Asset
				err := env.Relational.List(env.Ctx, "asset", query.ListQuery{
					Where: query.Where{query.Eq("kind", "valve")},
				}, &got)
				return got, err
			},
			WantNames: []string{"Inlet Valve"},
		},
		{
			Name: "list order desc with limit",
			Seed: fleet,
			Run: func(env *Env, ids map[string]string) ([]*domain.Asset, error) {
				var got []*domain.Asset
				err := env.Relational.List(env.Ctx, "asset", query.ListQuery{
					OrderBy: "name DESC", Limit: 2,
				}, &got)
				return got, err
			},
			WantNames: []string{"Main Pump", "Inlet Valve"},
		},
		{
			Name: "tenant isolation: foreign tenant cannot read",
			Seed: fleet,
			Run: func(env *Env, ids map[string]string) ([]*domain.Asset, error) {
				var a domain.Asset
				err := env.Relational.Get(env.ForeignCtx, "asset", ids["Main Pump"], &a)
				if errors.Is(err, fabriqerr.ErrNotFound) {
					return []*domain.Asset{}, nil // isolation holds
				}
				if err != nil {
					return nil, err
				}
				return []*domain.Asset{&a}, nil // leak → one row, fails want=0
			},
			WantNames: []string{},
		},
		{
			Name:     "raw SQL escape hatch",
			Requires: []Capability{CapRawSQL},
			Degrade:  &Degradation{ExpectErrContains: "does not execute raw SQL"},
			Seed:     fleet,
			Run: func(env *Env, ids map[string]string) ([]*domain.Asset, error) {
				var got []*domain.Asset
				// $1 is the Postgres placeholder dialect; Postgres is the only CapRawSQL backend today.
				err := env.Relational.Query(env.Ctx, &got,
					`SELECT * FROM assets WHERE kind = $1 ORDER BY name`, "pump")
				return got, err
			},
			WantNames: []string{"Backup Pump", "Main Pump"},
		},
	}
}

// RunRelational runs the relational conformance suite against b. It skips the
// whole suite when the backend does not implement the relational port, and
// gates each capability-requiring case via skip-or-assert-degrade.
func RunRelational(t *testing.T, b Backend) {
	t.Helper()
	if b.Setup(t).Relational == nil {
		t.Skipf("conformance: %s does not implement the relational port", b.Name())
		return
	}
	for _, tc := range RelationalCases() {
		tc := tc
		t.Run("relational/"+tc.Name, func(t *testing.T) {
			env := b.Setup(t)
			if miss := b.Capabilities().missing(tc.Requires); len(miss) > 0 {
				assertDegraded(t, b, env, tc, miss)
				return
			}
			ids := seedAssets(t, env, tc.Seed)
			got, err := tc.Run(env, ids)
			if err != nil {
				t.Fatalf("conformance: %s: %v", b.Name(), err)
			}
			assertNames(t, got, tc.WantNames)
		})
	}
}

// assertDegraded handles a case whose required capability the backend lacks:
// either assert the documented degradation, or skip (recorded, never silent).
func assertDegraded(t *testing.T, b Backend, env *Env, tc RelationalCase, miss []Capability) {
	t.Helper()
	if tc.Degrade == nil {
		t.Skipf("conformance: %s lacks %v — case skipped", b.Name(), miss)
		return
	}
	ids := seedAssets(t, env, tc.Seed)
	_, err := tc.Run(env, ids)
	if err == nil {
		t.Fatalf("conformance: %s lacks %v but %q did not degrade (nil error)", b.Name(), miss, tc.Name)
	}
	if tc.Degrade.ExpectErrIs != nil && !errors.Is(err, tc.Degrade.ExpectErrIs) {
		t.Fatalf("conformance: %s degradation: got %v, want errors.Is %v", b.Name(), err, tc.Degrade.ExpectErrIs)
	}
	if tc.Degrade.ExpectErrContains != "" && !strings.Contains(err.Error(), tc.Degrade.ExpectErrContains) {
		t.Fatalf("conformance: %s degradation: got %q, want contains %q", b.Name(), err.Error(), tc.Degrade.ExpectErrContains)
	}
}
