package subscribe

import (
	"sort"
	"time"

	"github.com/xraph/fabriq/core/query"
)

// conflator is a per-channel last-write-wins buffer. Deltas offered within
// the flush window are collapsed per aggregate; on flush the survivors are
// delivered in stream order. It is owned and locked by the Hub.
type conflator struct {
	window  time.Duration
	pending map[string]query.Delta // (aggregate, aggID) -> latest delta
	timer   *time.Timer            // armed while pending is non-empty
}

func newConflator(window time.Duration) *conflator {
	return &conflator{window: window, pending: make(map[string]query.Delta)}
}

// offer buffers a delta, keeping only the latest per aggregate key.
// It reports whether the flush timer needs arming (first pending entry).
func (c *conflator) offer(d query.Delta) (arm bool) {
	arm = len(c.pending) == 0
	key := d.Aggregate + "/" + d.AggID
	if prev, ok := c.pending[key]; !ok || d.Version >= prev.Version {
		c.pending[key] = d
	}
	return arm
}

// drain returns the buffered deltas in stream order and resets the buffer.
func (c *conflator) drain() []query.Delta {
	if len(c.pending) == 0 {
		return nil
	}
	out := make([]query.Delta, 0, len(c.pending))
	for _, d := range c.pending {
		out = append(out, d)
	}
	c.pending = make(map[string]query.Delta)
	sort.Slice(out, func(i, j int) bool {
		if out[i].StreamID != out[j].StreamID {
			return out[i].StreamID < out[j].StreamID
		}
		return out[i].Version < out[j].Version
	})
	return out
}

func (c *conflator) depth() int { return len(c.pending) }
