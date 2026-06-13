package subscribe_test

import (
	"context"

	"testing"
	"time"

	"github.com/xraph/fabriq/core/subscribe"
)

// Document sync frames must arrive COMPLETE and IN ORDER — the raw path
// shares the hub's connection layer but never its conflation.

func TestHub_RawChannelDeliversEveryFrameInOrder(t *testing.T) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(10 * time.Hour)) // window would eat frames if conflated
	defer h.Close()

	ch, cancel, err := h.SubscribeRaw(context.Background(), "doc:acme:page/D1", 64)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	// A burst on ONE aggregate: conflation would collapse this to 1.
	for v := int64(1); v <= 5; v++ {
		d := delta("page", "D1", v)
		d.Channel = "doc:acme:page/D1"
		h.Publish("doc:acme:page/D1", d) // the pump path calls Publish
	}

	got := receiveN(t, ch, 5, time.Second)
	for i, d := range got {
		if d.Version != int64(i+1) {
			t.Fatalf("frame %d out of order: %+v", i, got)
		}
	}
}

func TestHub_RawAndConflatedChannelsCoexist(t *testing.T) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(20 * time.Millisecond))
	defer h.Close()

	raw, cancelRaw, err := h.SubscribeRaw(context.Background(), "doc:acme:page/D1", 64)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelRaw()
	conflated, cancelC, err := h.Subscribe(context.Background(), "changes:acme:id:A1", 64)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelC()

	for v := int64(1); v <= 3; v++ {
		dd := delta("page", "D1", v)
		dd.Channel = "doc:acme:page/D1"
		h.Publish("doc:acme:page/D1", dd)
		h.Publish("changes:acme:id:A1", delta("asset", "A1", v))
	}

	if got := receiveN(t, raw, 3, time.Second); len(got) != 3 {
		t.Fatalf("raw channel conflated: %d frames", len(got))
	}
	got := receiveN(t, conflated, 1, time.Second)
	if got[0].Version != 3 {
		t.Fatalf("conflated channel must deliver only the latest: %+v", got)
	}
}

func TestHub_SubscribeRawRejectsMixedModes(t *testing.T) {
	h := subscribe.NewHub()
	defer h.Close()

	if _, _, err := h.Subscribe(context.Background(), "c1", 4); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.SubscribeRaw(context.Background(), "c1", 4); err == nil {
		t.Fatal("raw subscribe on a conflated channel must fail (mode is per channel)")
	}
	if _, _, err := h.SubscribeRaw(context.Background(), "c2", 4); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.Subscribe(context.Background(), "c2", 4); err == nil {
		t.Fatal("conflated subscribe on a raw channel must fail")
	}
}

func TestHub_RawOverflowClosesSubscriber(t *testing.T) {
	h := subscribe.NewHub()
	defer h.Close()

	// Buffer of 1, nobody reading: the raw contract is "complete and in
	// order, or nothing" — overflow must CLOSE the channel (forcing a
	// re-Sync), never silently drop a frame.
	ch, cancel, err := h.SubscribeRaw(context.Background(), "doc:acme:page/D9", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	for v := int64(1); v <= 3; v++ {
		h.Publish("doc:acme:page/D9", delta("page", "D9", v))
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed — correct
			}
		case <-deadline:
			t.Fatal("overflowed raw subscriber was not closed")
		}
	}
}
