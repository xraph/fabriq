package conformance

import (
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/domain"
)

// RunAll runs every applicable port suite against b. Ports the backend does
// not implement (nil Env field) are skipped wholesale. Follow-on plans add
// RunGraph, RunSearch, RunVector, RunSpatial, RunTimeseries,
// RunCommandStore and RunProjectionState here.
func RunAll(t *testing.T, b Backend) {
	t.Helper()
	RunRelational(t, b)
}

// seedAssets creates each asset via the command path and returns a map from
// SeedAsset.Name to the minted aggregate id, so cases can address rows by a
// stable name even though ids are minted ULIDs.
func seedAssets(t *testing.T, env *Env, seeds []SeedAsset) map[string]string {
	t.Helper()
	ids := make(map[string]string, len(seeds))
	for _, s := range seeds {
		res, err := env.Exec.Exec(env.Ctx, command.Command{
			Entity:  "asset",
			Op:      command.OpCreate,
			Payload: &domain.Asset{Name: s.Name, Kind: s.Kind, Serial: s.Serial, SiteID: s.SiteID},
		})
		if err != nil {
			t.Fatalf("conformance: seed asset %q: %v", s.Name, err)
		}
		ids[s.Name] = res.AggID
	}
	return ids
}

// names projects assets to their Name, for assertion messages.
func names(rows []*domain.Asset) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	return out
}

// assertNames fails unless got's Names equal want in order.
func assertNames(t *testing.T, got []*domain.Asset, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows %v, want %d %v", len(got), names(got), len(want), want)
	}
	for i := range got {
		if got[i].Name != want[i] {
			t.Fatalf("row %d = %q, want %q (full order %v, want %v)", i, got[i].Name, want[i], names(got), want)
		}
	}
}
