//go:build integration

package postgres_test

// TestRollup_* proves (*postgres.Adapter)'s rollup maintainer (Task 4:
// ReadRollupWatermark/AdvanceRollupWatermark/RollupRange/MaintainRollup)
// against a real Postgres: sealed buckets are aggregated into the metric's
// rollup table, the current (unsealed) bucket is never rolled up, a second
// pass is idempotent, a late arrival within RerollWindow is absorbed by the
// trailing re-roll, and per-scope aggregates land in separate rows with NULL
// preserved for the unscoped/shared case — reusing newInsightsHarness (the
// same Postgres-container + migrate-to-head + owner/app-role split
// insights_integration_test.go and insights_rollup_ddl_integration_test.go
// already use).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// checkoutRollupMetric is the fixture MetricSpec shared by every TestRollup_*
// test: one dimension (status), one count measure, one sum measure, and one
// avg measure (decomposed into avgamt__sum/avgamt__count by
// rollupMeasureColumns) — enough measure variety to exercise all three
// additive column shapes RollupRange emits, against the fixed 1-hour Bucket
// insights_rollup_maintain.go's Truncate-based sealing math is written
// against. sealGrace/reroll are supplied per-test so each test can control
// exactly which buckets are sealed without waiting on defaultSealGrace.
func checkoutRollupMetric(sealGrace, reroll time.Duration) *registry.MetricSpec {
	return &registry.MetricSpec{
		Name:       "checkout",
		Source:     "checkout",
		Dimensions: []string{"status"},
		Measures: []registry.MetricMeasure{
			{Kind: "count", As: "n"},
			{Kind: "sum", Field: "amount", As: "rev"},
			{Kind: "avg", Field: "amount", As: "avgamt"},
		},
		Rollup: &registry.RollupSpec{
			Bucket:       time.Hour,
			SealGrace:    sealGrace,
			RerollWindow: reroll,
		},
	}
}

// trackCheckout tracks one "checkout" event at (at, status, amount) under
// ctx, via the app-role adapter a — a thin wrapper so every TestRollup_*
// test can build its fixture events in one line.
func trackCheckout(t *testing.T, a *postgres.Adapter, ctx context.Context, at time.Time, status string, amount float64) {
	t.Helper()
	err := a.Track(ctx, []query.AnalyticsEvent{{
		Name:  "checkout",
		At:    at,
		Props: map[string]any{"status": status, "amount": amount},
	}})
	if err != nil {
		t.Fatalf("track checkout event at %s status=%s amount=%v: %v", at, status, amount, err)
	}
}

// rollupRow is one row's additive measures read back from
// fabriq_insights_rollup_checkout, matching checkoutRollupMetric's columns.
type rollupRow struct {
	n       float64
	rev     float64
	avgSum  float64
	avgCnt  float64
	present bool
}

// readCheckoutRollupRow reads back one row of fabriq_insights_rollup_checkout
// via the RLS-bypassing owner connection, keyed by the exact
// (tenant, scope, bucket, status) upsert conflict target. scopeID == nil
// means "match scope_id IS NULL" (the shared/unscoped convention); a non-nil
// scopeID matches that literal scope. rollupRow.present is false (with all
// measures zeroed) when no row exists for that key — the ordinary "this
// bucket was never sealed / never had this status" case, not a test failure
// by itself.
func readCheckoutRollupRow(t *testing.T, owner *postgres.Adapter, tenantID string, scopeID *string, bucket time.Time, status string) rollupRow {
	t.Helper()
	var (
		row  rollupRow
		err  error
		args []any
		sql  string
	)
	if scopeID == nil {
		sql = `SELECT n, rev, avgamt__sum, avgamt__count FROM fabriq_insights_rollup_checkout
			WHERE tenant_id = $1 AND scope_id IS NULL AND bucket_start = $2 AND status = $3`
		args = []any{tenantID, bucket, status}
	} else {
		sql = `SELECT n, rev, avgamt__sum, avgamt__count FROM fabriq_insights_rollup_checkout
			WHERE tenant_id = $1 AND scope_id = $2 AND bucket_start = $3 AND status = $4`
		args = []any{tenantID, *scopeID, bucket, status}
	}
	dbRow := owner.Driver().QueryRow(context.Background(), sql, args...)
	err = dbRow.Scan(&row.n, &row.rev, &row.avgSum, &row.avgCnt)
	if err != nil {
		if isNoRowsForTest(err) {
			return rollupRow{}
		}
		t.Fatalf("scan fabriq_insights_rollup_checkout row (tenant=%s bucket=%s status=%s): %v", tenantID, bucket, status, err)
	}
	row.present = true
	return row
}

// countCheckoutRollupRows counts every row in fabriq_insights_rollup_checkout
// for tenantID, via the owner connection — used by TestRollup_Idempotent to
// prove a second pass does not create duplicate rows.
func countCheckoutRollupRows(t *testing.T, owner *postgres.Adapter, tenantID string) int {
	t.Helper()
	var n int
	row := owner.Driver().QueryRow(context.Background(),
		`SELECT count(*) FROM fabriq_insights_rollup_checkout WHERE tenant_id = $1`, tenantID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count fabriq_insights_rollup_checkout rows for %q: %v", tenantID, err)
	}
	return n
}

// isNoRowsForTest mirrors the unexported postgres.isNoRows check (adapter.go)
// from this external test package: pgx surfaces a missing single-row result
// as an error whose text contains "no rows" (pgx.ErrNoRows is "no rows in
// result set"), which is what QueryRow's Scan propagates here.
func isNoRowsForTest(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no rows")
}

// TestRollup_AggregatesSealedBuckets tracks two statuses across three hourly
// buckets, runs one MaintainRollup pass with now set well past all three
// buckets' SealGrace, and asserts every (bucket, status) rollup row's
// additive measures match the known inputs exactly, and the watermark
// advances to the boundary just past the last sealed bucket.
func TestRollup_AggregatesSealedBuckets(t *testing.T) {
	a, owner := newInsightsHarness(t, registry.New())
	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m := checkoutRollupMetric(time.Minute, 2*time.Hour)
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2024, 3, 10, 8, 0, 0, 0, time.UTC)

	// Bucket i covers [base+i*1h, base+(i+1)*1h). Two "ok" events and one
	// "err" event per bucket, with amounts that vary by bucket so a bug that
	// mixed up buckets would produce a visibly wrong sum.
	for i := 0; i < 3; i++ {
		bucketStart := base.Add(time.Duration(i) * time.Hour)
		trackCheckout(t, a, acmeCtx, bucketStart.Add(10*time.Minute), "ok", 10+float64(i))
		trackCheckout(t, a, acmeCtx, bucketStart.Add(20*time.Minute), "ok", 20+float64(i))
		trackCheckout(t, a, acmeCtx, bucketStart.Add(30*time.Minute), "err", 5+float64(i))
	}

	// now = end of the 3rd bucket + SealGrace + a little slack, so all three
	// buckets are sealed but nothing beyond them exists anyway.
	now := base.Add(3*time.Hour + 2*time.Minute)
	if err := a.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	wantWatermark := base.Add(3 * time.Hour)
	gotWatermark, ok, err := a.ReadRollupWatermark(acmeCtx, "checkout")
	if err != nil {
		t.Fatalf("ReadRollupWatermark: %v", err)
	}
	if !ok {
		t.Fatalf("ReadRollupWatermark: expected a watermark row after MaintainRollup")
	}
	if !gotWatermark.Equal(wantWatermark) {
		t.Fatalf("watermark = %s, want %s (boundary just past the last sealed bucket)", gotWatermark, wantWatermark)
	}

	for i := 0; i < 3; i++ {
		bucketStart := base.Add(time.Duration(i) * time.Hour)

		okRow := readCheckoutRollupRow(t, owner, "acme", nil, bucketStart, "ok")
		if !okRow.present {
			t.Fatalf("bucket %d status=ok: expected a rolled-up row, found none", i)
		}
		wantOkRev := (10 + float64(i)) + (20 + float64(i))
		if okRow.n != 2 || okRow.rev != wantOkRev || okRow.avgSum != wantOkRev || okRow.avgCnt != 2 {
			t.Fatalf("bucket %d status=ok: got n=%v rev=%v avgSum=%v avgCnt=%v, want n=2 rev=%v avgSum=%v avgCnt=2",
				i, okRow.n, okRow.rev, okRow.avgSum, okRow.avgCnt, wantOkRev, wantOkRev)
		}

		errRow := readCheckoutRollupRow(t, owner, "acme", nil, bucketStart, "err")
		if !errRow.present {
			t.Fatalf("bucket %d status=err: expected a rolled-up row, found none", i)
		}
		wantErrRev := 5 + float64(i)
		if errRow.n != 1 || errRow.rev != wantErrRev || errRow.avgSum != wantErrRev || errRow.avgCnt != 1 {
			t.Fatalf("bucket %d status=err: got n=%v rev=%v avgSum=%v avgCnt=%v, want n=1 rev=%v avgSum=%v avgCnt=1",
				i, errRow.n, errRow.rev, errRow.avgSum, errRow.avgCnt, wantErrRev, wantErrRev)
		}
	}
}

// TestRollup_Idempotent proves that running MaintainRollup a second time with
// the same `now` (nothing new tracked in between) leaves the rollup rows and
// row count unchanged — the ON CONFLICT DO UPDATE overwrites each row with
// the same recomputed values rather than duplicating or corrupting it.
func TestRollup_Idempotent(t *testing.T) {
	a, owner := newInsightsHarness(t, registry.New())
	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m := checkoutRollupMetric(time.Minute, 2*time.Hour)
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	trackCheckout(t, a, acmeCtx, base.Add(10*time.Minute), "ok", 42)
	trackCheckout(t, a, acmeCtx, base.Add(70*time.Minute), "ok", 7) // 2nd bucket

	now := base.Add(2*time.Hour + 2*time.Minute)
	if err := a.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup (1st pass): %v", err)
	}
	firstRow0 := readCheckoutRollupRow(t, owner, "acme", nil, base, "ok")
	firstRow1 := readCheckoutRollupRow(t, owner, "acme", nil, base.Add(time.Hour), "ok")
	firstCount := countCheckoutRollupRows(t, owner, "acme")
	if !firstRow0.present || !firstRow1.present {
		t.Fatalf("expected both bucket rows present after the 1st pass: bucket0=%+v bucket1=%+v", firstRow0, firstRow1)
	}

	if err := a.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup (2nd pass): %v", err)
	}
	secondRow0 := readCheckoutRollupRow(t, owner, "acme", nil, base, "ok")
	secondRow1 := readCheckoutRollupRow(t, owner, "acme", nil, base.Add(time.Hour), "ok")
	secondCount := countCheckoutRollupRows(t, owner, "acme")

	if secondRow0 != firstRow0 {
		t.Fatalf("bucket0 row changed across an idempotent 2nd pass: got %+v, want %+v", secondRow0, firstRow0)
	}
	if secondRow1 != firstRow1 {
		t.Fatalf("bucket1 row changed across an idempotent 2nd pass: got %+v, want %+v", secondRow1, firstRow1)
	}
	if secondCount != firstCount {
		t.Fatalf("row count changed across an idempotent 2nd pass: got %d, want %d (no duplicate rows)", secondCount, firstCount)
	}

	gotWatermark, ok, err := a.ReadRollupWatermark(acmeCtx, "checkout")
	if err != nil {
		t.Fatalf("ReadRollupWatermark: %v", err)
	}
	if !ok || !gotWatermark.Equal(base.Add(2*time.Hour)) {
		t.Fatalf("watermark after 2nd pass = (%s, %v), want (%s, true) — GREATEST must not move it backwards or duplicate progress", gotWatermark, ok, base.Add(2*time.Hour))
	}
}

// TestRollup_RerollAbsorbsLateArrival seals a bucket, tracks a late event
// into that already-sealed bucket, and re-runs MaintainRollup with a
// RerollWindow wide enough to cover it — the re-rolled row must reflect the
// late event, proving the maintainer doesn't treat a sealed bucket as
// permanently frozen within its RerollWindow.
func TestRollup_RerollAbsorbsLateArrival(t *testing.T) {
	a, owner := newInsightsHarness(t, registry.New())
	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	// RerollWindow=2h: after the watermark has advanced past bucket0, a
	// second pass still re-rolls at least 2 buckets back, so bucket0 stays
	// reachable for a late arrival.
	m := checkoutRollupMetric(time.Minute, 2*time.Hour)
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC) // bucket0 = [base, base+1h)
	trackCheckout(t, a, acmeCtx, base.Add(5*time.Minute), "ok", 10)

	now := base.Add(1*time.Hour + 2*time.Minute)
	if err := a.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup (seal bucket0): %v", err)
	}
	sealed := readCheckoutRollupRow(t, owner, "acme", nil, base, "ok")
	if !sealed.present || sealed.n != 1 || sealed.rev != 10 {
		t.Fatalf("bucket0 after sealing: got %+v, want n=1 rev=10", sealed)
	}

	// A late event lands in bucket0 AFTER it was already sealed and rolled up.
	trackCheckout(t, a, acmeCtx, base.Add(40*time.Minute), "ok", 90)

	// Re-run with the same `now` (no new bucket has sealed) so the pass is
	// driven purely by the trailing re-roll, not a fresh seal boundary.
	if err := a.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup (re-roll pass): %v", err)
	}
	rerolled := readCheckoutRollupRow(t, owner, "acme", nil, base, "ok")
	if !rerolled.present {
		t.Fatalf("bucket0 after re-roll: expected the row to still be present")
	}
	if rerolled.n != 2 || rerolled.rev != 100 || rerolled.avgSum != 100 || rerolled.avgCnt != 2 {
		t.Fatalf("bucket0 after re-roll: got %+v, want n=2 rev=100 avgSum=100 avgCnt=2 (late arrival absorbed)", rerolled)
	}
}

// TestRollup_DoesNotSealUngraceBuckets tracks one event in an already-sealed
// prior bucket and one event in the current, still-open bucket, then asserts
// a MaintainRollup pass rolls up only the prior bucket — the current
// bucket's event must not appear in the rollup at all, because its bucket
// has not yet passed SealGrace past its END.
func TestRollup_DoesNotSealUngraceBuckets(t *testing.T) {
	a, owner := newInsightsHarness(t, registry.New())
	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m := checkoutRollupMetric(time.Minute, 2*time.Hour)
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2024, 7, 1, 9, 0, 0, 0, time.UTC) // current bucket = [base, base+1h)
	priorBucket := base.Add(-time.Hour)                 // already fully sealed by `now` below

	trackCheckout(t, a, acmeCtx, priorBucket.Add(10*time.Minute), "ok", 7)
	trackCheckout(t, a, acmeCtx, base.Add(5*time.Minute), "ok", 999) // still in the open bucket

	// now is 45 minutes into the current bucket: well before it ends, so
	// SealGrace (1m) past `now` still lands inside the current bucket, not
	// past its end.
	now := base.Add(45 * time.Minute)
	if err := a.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	priorRow := readCheckoutRollupRow(t, owner, "acme", nil, priorBucket, "ok")
	if !priorRow.present || priorRow.n != 1 || priorRow.rev != 7 {
		t.Fatalf("prior (sealed) bucket: got %+v, want n=1 rev=7", priorRow)
	}

	currentRow := readCheckoutRollupRow(t, owner, "acme", nil, base, "ok")
	if currentRow.present {
		t.Fatalf("current (unsealed) bucket: expected NO rollup row, got %+v", currentRow)
	}
}

// TestRollup_PerScope tracks the same bucket and status under two different
// scopes — an explicit scope "s1" and unscoped (NULL, meaning shared) — and
// asserts MaintainRollup (run under an unscoped tenant ctx, so it can see
// every scope) produces two SEPARATE rollup rows rather than merging them:
// scope_id is part of the upsert conflict key, and NULL must stay NULL
// (never coalesced to a sentinel), matching fabriq_insights_events's own
// per-scope convention.
func TestRollup_PerScope(t *testing.T) {
	a, owner := newInsightsHarness(t, registry.New())
	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	scopedCtx, err := tenant.WithScope(acmeCtx, "s1")
	if err != nil {
		t.Fatal(err)
	}

	m := checkoutRollupMetric(time.Minute, 2*time.Hour)
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2024, 8, 1, 6, 0, 0, 0, time.UTC)
	trackCheckout(t, a, acmeCtx, base.Add(10*time.Minute), "ok", 10)   // unscoped/shared
	trackCheckout(t, a, scopedCtx, base.Add(20*time.Minute), "ok", 20) // scope "s1"

	now := base.Add(1*time.Hour + 2*time.Minute)
	// Run the maintainer pass under the UNSCOPED tenant ctx, so its RLS
	// predicate's "app.scope_id = ''" branch is satisfied and it sees both
	// scopes' events in one pass — the design's documented convention for a
	// maintainer pass (see insights_rollup_maintain.go's header comment).
	if err := a.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	unscopedRow := readCheckoutRollupRow(t, owner, "acme", nil, base, "ok")
	if !unscopedRow.present || unscopedRow.n != 1 || unscopedRow.rev != 10 {
		t.Fatalf("unscoped/shared row: got %+v, want n=1 rev=10", unscopedRow)
	}

	s1 := "s1"
	scopedRow := readCheckoutRollupRow(t, owner, "acme", &s1, base, "ok")
	if !scopedRow.present || scopedRow.n != 1 || scopedRow.rev != 20 {
		t.Fatalf("scope=s1 row: got %+v, want n=1 rev=20", scopedRow)
	}
}

// TestRollup_RejectsUnconfiguredMetric is a small guard test: calling
// MaintainRollup/RollupRange on a MetricSpec with no RollupSpec (or a
// zero/negative Bucket) must fail fast with a clear error rather than
// silently doing nothing or panicking — mirrors rollupTableDDL's own
// defensive checks (insights_rollup_ddl.go).
func TestRollup_RejectsUnconfiguredMetric(t *testing.T) {
	a, _ := newInsightsHarness(t, registry.New())
	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	notRollup := &registry.MetricSpec{
		Name:       "no_rollup",
		Source:     "checkout",
		Dimensions: []string{"status"},
		Measures:   []registry.MetricMeasure{{Kind: "count", As: "n"}},
	}
	if err := a.MaintainRollup(acmeCtx, notRollup, time.Now()); err == nil {
		t.Fatalf("MaintainRollup on a metric with no RollupSpec: expected an error, got nil")
	}
	if err := a.RollupRange(acmeCtx, notRollup, time.Time{}, time.Now()); err == nil {
		t.Fatalf("RollupRange on a metric with no RollupSpec: expected an error, got nil")
	}
}

// TestRollup_PerTenantWatermarkIsolation is a regression test for a bug
// ReadRollupWatermark's query used to have: it read
// `SELECT watermark_bucket FROM fabriq_insights_rollup_state WHERE metric =
// $1` with NO explicit tenant_id filter, relying entirely on RLS to scope
// the read. Every other TestRollup_* test in this file only ever exercises
// one tenant ("acme") through the RLS-ENFORCED app-role adapter `a`, which
// never surfaces this: RLS silently does the tenant filtering for it. But
// forgeext's rollup:insights maintainer (Task 6) enumerates tenants and runs
// through the OWNER/pool-path connection for cross-tenant operations — a
// deployment where that connection is a superuser (or any BYPASSRLS role,
// which is exactly what `owner` here is) bypasses RLS entirely, so an
// un-filtered query returns SOME row matching the metric, not necessarily
// the calling tenant's — silently borrowing another tenant's watermark and
// permanently excluding the caller's own historical buckets from ever being
// rolled up (no error, no missing-row signal — just quietly wrong/empty
// results forever after). This test runs MaintainRollup for TWO tenants
// back to back through `owner` (RLS-bypassing) and asserts each tenant's
// rollup row and watermark are correct and mutually isolated — the exact
// scenario that exposed the bug when Task 6's forgeext integration test used
// a superuser DSN for its own maintainer.
func TestRollup_PerTenantWatermarkIsolation(t *testing.T) {
	_, owner := newInsightsHarness(t, registry.New())

	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	globexCtx, err := tenant.WithTenant(context.Background(), "globex")
	if err != nil {
		t.Fatal(err)
	}

	m := checkoutRollupMetric(0, 0) // defaults: 5m grace, 2x-bucket reroll
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2024, 3, 10, 8, 0, 0, 0, time.UTC)
	// acme: 3 "ok" checkouts; globex: 2 "ok" checkouts — different counts so
	// a tenant mix-up in the watermark read would be caught, not just an
	// empty-vs-nonempty check.
	for i := 0; i < 3; i++ {
		trackCheckout(t, owner, acmeCtx, base.Add(10*time.Minute), "ok", 10)
	}
	for i := 0; i < 2; i++ {
		trackCheckout(t, owner, globexCtx, base.Add(10*time.Minute), "ok", 10)
	}

	now := base.Add(2 * time.Hour) // well past SealGrace for the base+10m bucket
	if err := owner.MaintainRollup(acmeCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup acme: %v", err)
	}
	if err := owner.MaintainRollup(globexCtx, m, now); err != nil {
		t.Fatalf("MaintainRollup globex: %v", err)
	}

	acmeRow := readCheckoutRollupRow(t, owner, "acme", nil, base, "ok")
	if !acmeRow.present || acmeRow.n != 3 {
		t.Fatalf("acme rollup row: got %+v, want present n=3", acmeRow)
	}
	globexRow := readCheckoutRollupRow(t, owner, "globex", nil, base, "ok")
	if !globexRow.present || globexRow.n != 2 {
		t.Fatalf("globex rollup row: got %+v, want present n=2 (this is the bug: an un-tenant-filtered "+
			"ReadRollupWatermark borrows acme's already-advanced watermark for globex, excluding globex's "+
			"own historical bucket from ever being rolled up)", globexRow)
	}

	acmeWM, acmeOK, err := owner.ReadRollupWatermark(acmeCtx, m.Name)
	if err != nil {
		t.Fatalf("ReadRollupWatermark acme: %v", err)
	}
	globexWM, globexOK, err := owner.ReadRollupWatermark(globexCtx, m.Name)
	if err != nil {
		t.Fatalf("ReadRollupWatermark globex: %v", err)
	}
	if !acmeOK || !globexOK {
		t.Fatalf("expected both watermarks to exist: acme ok=%v, globex ok=%v", acmeOK, globexOK)
	}
	if !acmeWM.Equal(globexWM) {
		// Both tenants ran MaintainRollup with the same `now`/metric, so their
		// sealed-to boundary is the same value — but each MUST read back its
		// OWN row (not literally the same row), which this same-value
		// assertion cannot distinguish by itself; the per-tenant row/count
		// assertions above are the ones that actually catch cross-tenant
		// borrowing.
		t.Fatalf("acme watermark %s != globex watermark %s (both ran the same pass, expected equal boundaries)", acmeWM, globexWM)
	}
}
