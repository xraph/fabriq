package shard

import (
	"fmt"
	"math"
	"runtime/metrics"
	"time"
)

// scaleDir is the outcome of one autoscaler tick.
type scaleDir int

const (
	scaleHold scaleDir = iota
	scaleGrow
	scaleShrink
)

// String is the {direction} metric label value.
func (d scaleDir) String() string {
	switch d {
	case scaleGrow:
		return "grow"
	case scaleShrink:
		return "shrink"
	default:
		return "hold"
	}
}

// AutoscaleConfig fully parameterizes the pool autoscaler. The user-facing
// config (fabriq.AdaptivePoolConfig) sets the load-bearing few; the policy
// knobs default in withDefaults so tests and callers can omit them.
type AutoscaleConfig struct {
	Min, Max      int           // hard floor / ceiling
	Interval      time.Duration // controller tick (default 5s)
	ConnBudget    int           // total server-side conns; 0 = no budget clamp
	PerShardConns int           // conns per shard pool (default 4)
	HeapSoftLimit uint64        // heap-in-use bytes above which growth freezes; 0 = off

	GrowFactor    float64 // default 1.5
	ShrinkStep    int     // default 1
	ConfirmTicks  int     // consecutive same-direction ticks before acting (default 3)
	CooldownTicks int     // ticks suppressed after a change (default 3)

	MissRatioHigh  float64 // grow above this (default 0.20)
	EvictRateHigh  float64 // evictions/acquires above this (default 0.10)
	OpenFillLow    float64 // open/cap below this = oversized (default 0.75)
	HeldUtilLow    float64 // held/open below this = idle (default 0.50)
	HeapShrinkMult float64 // heap > softLimit*this ⇒ force shrink (default 1.10)
}

func (c AutoscaleConfig) withDefaults() AutoscaleConfig {
	if c.Min <= 0 {
		c.Min = 8
	}
	if c.Max <= 0 {
		c.Max = 128
	}
	if c.Max < c.Min {
		c.Max = c.Min
	}
	if c.Interval <= 0 {
		c.Interval = 5 * time.Second
	}
	if c.PerShardConns <= 0 {
		c.PerShardConns = 4
	}
	if c.GrowFactor < 1.1 {
		c.GrowFactor = 1.5
	}
	if c.ShrinkStep <= 0 {
		c.ShrinkStep = 1
	}
	if c.ConfirmTicks <= 0 {
		c.ConfirmTicks = 3
	}
	if c.CooldownTicks <= 0 {
		c.CooldownTicks = 3
	}
	if c.MissRatioHigh <= 0 {
		c.MissRatioHigh = 0.20
	}
	if c.EvictRateHigh <= 0 {
		c.EvictRateHigh = 0.10
	}
	if c.OpenFillLow <= 0 {
		c.OpenFillLow = 0.75
	}
	if c.HeldUtilLow <= 0 {
		c.HeldUtilLow = 0.50
	}
	if c.HeapShrinkMult < 1.0 {
		c.HeapShrinkMult = 1.10
	}
	return c
}

// ScaleEvent is emitted after each cap change (OnScale hook).
type ScaleEvent struct {
	OldCap, NewCap int
	Direction      scaleDir
	Reason         string
	Signals        poolSignals
}

// autoscaler holds the hysteresis + cooldown state across ticks.
type autoscaler struct {
	cfg            AutoscaleConfig
	pressureStreak int
	slackStreak    int
	cooldown       int
}

func newAutoscaler(cfg AutoscaleConfig) *autoscaler {
	return &autoscaler{cfg: cfg.withDefaults()}
}

// effectiveCeiling clamps Max by the connection budget; heapCritical reports
// heap far enough over the soft limit to force a shrink.
func (a *autoscaler) effectiveCeiling(heap uint64) (ceiling int, heapCritical bool) {
	ceiling = a.cfg.Max
	if a.cfg.ConnBudget > 0 {
		if budget := a.cfg.ConnBudget / a.cfg.PerShardConns; budget < ceiling {
			ceiling = budget
		}
	}
	if a.cfg.HeapSoftLimit > 0 && heap > uint64(float64(a.cfg.HeapSoftLimit)*a.cfg.HeapShrinkMult) {
		heapCritical = true
	}
	if ceiling < a.cfg.Min {
		ceiling = a.cfg.Min
	}
	return ceiling, heapCritical
}

// decide advances one tick and returns the target cap. Pure: no clock, no
// runtime; all timing state is tick-counted.
func (a *autoscaler) decide(s poolSignals) (newCap int, dir scaleDir, reason string) {
	cfg := a.cfg
	ceiling, heapCritical := a.effectiveCeiling(s.heapInUse)
	lo := cfg.Min
	if s.held > lo {
		lo = s.held
	}
	if lo > ceiling {
		lo = ceiling
	}
	heapOverSoft := cfg.HeapSoftLimit > 0 && s.heapInUse > cfg.HeapSoftLimit

	// Heap critical wins over everything: shrink now, bypass cooldown.
	if heapCritical && s.cap > lo {
		newCap = s.cap - cfg.ShrinkStep
		if newCap < lo {
			newCap = lo
		}
		a.pressureStreak, a.slackStreak, a.cooldown = 0, 0, cfg.CooldownTicks
		return newCap, scaleShrink, "heapCritical"
	}

	if a.cooldown > 0 {
		a.cooldown--
		a.pressureStreak, a.slackStreak = 0, 0
		return s.cap, scaleHold, "cooldown"
	}

	var missRatio, evictRate float64
	if s.acquires > 0 {
		missRatio = float64(s.misses) / float64(s.acquires)
		evictRate = float64(s.evictions) / float64(s.acquires)
	}
	contended := s.waits > 0 || s.timeouts > 0
	pressure := missRatio > cfg.MissRatioHigh || evictRate > cfg.EvictRateHigh || contended

	if pressure {
		a.slackStreak = 0
		a.pressureStreak++
		canGrow := s.cap < ceiling && !heapOverSoft
		if a.pressureStreak >= cfg.ConfirmTicks && canGrow {
			grown := int(math.Ceil(float64(s.cap) * cfg.GrowFactor))
			if grown <= s.cap {
				grown = s.cap + 1
			}
			if grown > ceiling {
				grown = ceiling
			}
			if grown < lo {
				grown = lo
			}
			a.pressureStreak, a.cooldown = 0, cfg.CooldownTicks
			return grown, scaleGrow, fmt.Sprintf("pressure missRatio=%.2f evictRate=%.2f contended=%v", missRatio, evictRate, contended)
		}
		return s.cap, scaleHold, "pressure-confirming"
	}

	fillLow := float64(s.open) < float64(s.cap)*cfg.OpenFillLow
	utilLow := s.open == 0 || float64(s.held) < float64(s.open)*cfg.HeldUtilLow
	if fillLow && utilLow {
		a.pressureStreak = 0
		a.slackStreak++
		if a.slackStreak >= cfg.ConfirmTicks && s.cap > lo {
			shrunk := s.cap - cfg.ShrinkStep
			if shrunk < lo {
				shrunk = lo
			}
			a.slackStreak, a.cooldown = 0, cfg.CooldownTicks
			return shrunk, scaleShrink, fmt.Sprintf("slack open=%d cap=%d held=%d", s.open, s.cap, s.held)
		}
		return s.cap, scaleHold, "slack-confirming"
	}

	a.pressureStreak, a.slackStreak = 0, 0
	return s.cap, scaleHold, "steady"
}

// heapInUse reads live heap-object bytes via runtime/metrics (no
// stop-the-world, unlike runtime.ReadMemStats).
func heapInUse() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	if s[0].Value.Kind() == metrics.KindUint64 {
		return s[0].Value.Uint64()
	}
	return 0
}

// defaultTicker is the production ticker seam.
func defaultTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}
