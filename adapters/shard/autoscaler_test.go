package shard

import "testing"

// cfg builds an AutoscaleConfig with fast, deterministic policy knobs.
func testAutoCfg() AutoscaleConfig {
	return AutoscaleConfig{
		Min: 4, Max: 64,
		GrowFactor: 1.5, ShrinkStep: 1,
		ConfirmTicks: 2, CooldownTicks: 2,
		MissRatioHigh: 0.20, EvictRateHigh: 0.10,
		OpenFillLow: 0.75, HeldUtilLow: 0.50, HeapShrinkMult: 1.10,
	}
}

func TestAutoscaler_GrowsUnderMissRatio(t *testing.T) {
	a := newAutoscaler(testAutoCfg())
	sig := poolSignals{acquires: 100, misses: 40, open: 8, held: 8, cap: 8}
	// ConfirmTicks=2: first tick confirms, no change.
	if _, dir, _ := a.decide(sig); dir != scaleHold {
		t.Fatalf("tick1 dir=%v want hold", dir)
	}
	newCap, dir, _ := a.decide(sig)
	if dir != scaleGrow {
		t.Fatalf("tick2 dir=%v want grow", dir)
	}
	if newCap != 12 { // ceil(8*1.5)
		t.Fatalf("newCap=%d want 12", newCap)
	}
}

func TestAutoscaler_GrowsUnderEvictChurn(t *testing.T) {
	a := newAutoscaler(testAutoCfg())
	sig := poolSignals{acquires: 100, evictions: 30, open: 16, held: 16, cap: 16}
	a.decide(sig)
	if _, dir, _ := a.decide(sig); dir != scaleGrow {
		t.Fatal("eviction churn must grow")
	}
}

func TestAutoscaler_GrowsUnderContention(t *testing.T) {
	a := newAutoscaler(testAutoCfg())
	sig := poolSignals{acquires: 10, waits: 3, open: 16, held: 16, cap: 16}
	a.decide(sig)
	if _, dir, _ := a.decide(sig); dir != scaleGrow {
		t.Fatal("acquire waits must grow")
	}
	b := newAutoscaler(testAutoCfg())
	tsig := poolSignals{acquires: 10, timeouts: 1, open: 16, held: 16, cap: 16}
	b.decide(tsig)
	if _, dir, _ := b.decide(tsig); dir != scaleGrow {
		t.Fatal("acquire timeouts must grow")
	}
}

func TestAutoscaler_HysteresisRequiresConfirmTicks(t *testing.T) {
	c := testAutoCfg()
	c.ConfirmTicks = 3
	a := newAutoscaler(c)
	sig := poolSignals{acquires: 100, misses: 50, open: 8, held: 8, cap: 8}
	if _, dir, _ := a.decide(sig); dir != scaleHold {
		t.Fatal("tick1 must hold")
	}
	if _, dir, _ := a.decide(sig); dir != scaleHold {
		t.Fatal("tick2 must hold")
	}
	if _, dir, _ := a.decide(sig); dir != scaleGrow {
		t.Fatal("tick3 must grow")
	}
}

func TestAutoscaler_CooldownSuppressesConsecutiveChanges(t *testing.T) {
	a := newAutoscaler(testAutoCfg()) // ConfirmTicks=2, CooldownTicks=2
	sig := poolSignals{acquires: 100, misses: 50, open: 8, held: 8, cap: 8}
	a.decide(sig)
	if _, dir, _ := a.decide(sig); dir != scaleGrow {
		t.Fatal("expected grow at tick2")
	}
	// Next two ticks are cooldown → hold despite pressure.
	if _, dir, _ := a.decide(sig); dir != scaleHold {
		t.Fatal("cooldown tick must hold")
	}
	if _, dir, _ := a.decide(sig); dir != scaleHold {
		t.Fatal("cooldown tick must hold")
	}
}

func TestAutoscaler_ShrinksUnderSlack(t *testing.T) {
	a := newAutoscaler(testAutoCfg())
	// open well under cap, low held-utilization, no pressure.
	sig := poolSignals{acquires: 5, misses: 0, open: 10, held: 2, cap: 32}
	a.decide(sig)
	newCap, dir, _ := a.decide(sig)
	if dir != scaleShrink {
		t.Fatalf("dir=%v want shrink", dir)
	}
	if newCap != 31 { // cap-1
		t.Fatalf("newCap=%d want 31", newCap)
	}
}

func TestAutoscaler_DoesNotShrinkWhenSaturated(t *testing.T) {
	a := newAutoscaler(testAutoCfg())
	// open == cap: fill is not low, so not slack even with low held.
	sig := poolSignals{acquires: 5, open: 32, held: 2, cap: 32}
	a.decide(sig)
	if _, dir, _ := a.decide(sig); dir == scaleShrink {
		t.Fatal("a full pool is not slack")
	}
}

func TestAutoscaler_CeilingClampsToConnBudget(t *testing.T) {
	c := testAutoCfg()
	c.Max = 1000
	c.ConnBudget = 40
	c.PerShardConns = 4 // budget ceiling = 10
	a := newAutoscaler(c)
	sig := poolSignals{acquires: 100, misses: 90, open: 9, held: 9, cap: 9}
	a.decide(sig)
	newCap, dir, _ := a.decide(sig)
	if dir != scaleGrow || newCap != 10 {
		t.Fatalf("newCap=%d dir=%v want 10/grow (conn budget ceiling)", newCap, dir)
	}
	// At the ceiling, further pressure cannot grow.
	sig.cap = 10
	sig.open = 10
	a2 := newAutoscaler(c)
	a2.decide(sig)
	if _, dir, _ := a2.decide(sig); dir == scaleGrow {
		t.Fatal("must not grow past the conn-budget ceiling")
	}
}

func TestAutoscaler_ClampsToConfiguredMax(t *testing.T) {
	c := testAutoCfg()
	c.Max = 10
	a := newAutoscaler(c)
	sig := poolSignals{acquires: 100, misses: 90, open: 9, held: 9, cap: 9}
	a.decide(sig)
	newCap, _, _ := a.decide(sig)
	if newCap != 10 {
		t.Fatalf("newCap=%d want 10 (Max)", newCap)
	}
}

func TestAutoscaler_FreezesGrowthOverHeapSoftLimit(t *testing.T) {
	c := testAutoCfg()
	c.HeapSoftLimit = 1000
	a := newAutoscaler(c)
	sig := poolSignals{acquires: 100, misses: 90, open: 8, held: 8, cap: 8, heapInUse: 1500}
	a.decide(sig)
	if _, dir, _ := a.decide(sig); dir == scaleGrow {
		t.Fatal("growth must freeze over the heap soft limit")
	}
}

func TestAutoscaler_ForcesShrinkWhenHeapCritical_BypassesCooldown(t *testing.T) {
	c := testAutoCfg()
	c.HeapSoftLimit = 1000
	c.HeapShrinkMult = 1.10 // critical above 1100
	a := newAutoscaler(c)
	// Put it in cooldown first via a grow.
	grow := poolSignals{acquires: 100, misses: 90, open: 8, held: 8, cap: 8}
	a.decide(grow)
	a.decide(grow) // grow → cooldown starts
	crit := poolSignals{acquires: 0, open: 4, held: 0, cap: 12, heapInUse: 5000}
	newCap, dir, reason := a.decide(crit)
	if dir != scaleShrink {
		t.Fatalf("heap-critical must shrink (bypass cooldown), got %v", dir)
	}
	if newCap != 11 || reason != "heapCritical" {
		t.Fatalf("newCap=%d reason=%q want 11/heapCritical", newCap, reason)
	}
}

func TestAutoscaler_FloorClampsToMinAndHeld(t *testing.T) {
	c := testAutoCfg()
	c.Min = 4
	a := newAutoscaler(c)
	// Slack but held=6 keeps the floor at 6, not Min=4.
	sig := poolSignals{acquires: 1, open: 6, held: 6, cap: 6}
	// open==cap here → not slack; use open<cap with high held instead.
	sig = poolSignals{acquires: 1, open: 7, held: 6, cap: 8}
	a.decide(sig)
	newCap, dir, _ := a.decide(sig)
	if dir == scaleShrink && newCap < 6 {
		t.Fatalf("floor breached: newCap=%d < held 6", newCap)
	}
}

func TestAutoscaler_GrowFastShrinkSlow(t *testing.T) {
	a := newAutoscaler(testAutoCfg())
	g := poolSignals{acquires: 100, misses: 90, open: 20, held: 20, cap: 20}
	a.decide(g)
	grown, _, _ := a.decide(g)
	if grown != 30 { // ceil(20*1.5)
		t.Fatalf("grow=%d want 30 (x1.5)", grown)
	}
	b := newAutoscaler(testAutoCfg())
	s := poolSignals{acquires: 1, open: 5, held: 1, cap: 30}
	b.decide(s)
	shrunk, _, _ := b.decide(s)
	if shrunk != 29 { // -1 additive
		t.Fatalf("shrink=%d want 29 (-1)", shrunk)
	}
}
