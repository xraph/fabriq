package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/command"
)

func TestLifecycleHook_FiresWithChangeAndParticipates(t *testing.T) {
	store := newFakeStore()
	var seen []command.Change
	hook := command.HookFunc(func(ctx context.Context, tx command.Tx, ch command.Change) error {
		seen = append(seen, ch)
		return tx.Exec(ctx, "INSERT INTO audit (id, version) VALUES ($1, $2)", ch.Envelope.AggID, ch.Envelope.Version)
	})
	x, err := command.NewExecutor(cmdRegistry(t), store, command.WithHooks(hook))
	if err != nil {
		t.Fatal(err)
	}
	ctx := acmeCtx(t)

	res, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "Plant"}})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("hook fired %d times, want 1", len(seen))
	}
	ch := seen[0]
	if ch.Entity.Spec.Name != "site" || ch.Op != command.OpCreate {
		t.Fatalf("change entity/op wrong: %s/%v", ch.Entity.Spec.Name, ch.Op)
	}
	if ch.Envelope.AggID != res.AggID || ch.Envelope.Version != 1 || ch.Envelope.Type != "site.created" {
		t.Fatalf("change envelope wrong: %+v", ch.Envelope)
	}
	execs := store.Execs()
	if len(execs) != 1 || !strings.Contains(execs[0].SQL, "INSERT INTO audit") {
		t.Fatalf("hook participation not committed: %+v", execs)
	}
}

func TestLifecycleHook_VetoRollsBack(t *testing.T) {
	store := newFakeStore()
	boom := errors.New("veto")
	hook := command.HookFunc(func(ctx context.Context, tx command.Tx, ch command.Change) error {
		_ = tx.Exec(ctx, "INSERT INTO audit (id) VALUES ($1)", ch.Envelope.AggID)
		return boom
	})
	x, _ := command.NewExecutor(cmdRegistry(t), store, command.WithHooks(hook))

	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "Plant"}})
	if !errors.Is(err, boom) {
		t.Fatalf("want veto error, got %v", err)
	}
	if len(store.outbox) != 0 {
		t.Fatalf("outbox should be empty after veto, got %d", len(store.outbox))
	}
	if len(store.Execs()) != 0 {
		t.Fatalf("hook writes should roll back on veto, got %d execs", len(store.Execs()))
	}
}

func TestLifecycleHook_OrderedChainShortCircuits(t *testing.T) {
	store := newFakeStore()
	var order []string
	h1 := command.HookFunc(func(_ context.Context, _ command.Tx, _ command.Change) error { order = append(order, "h1"); return nil })
	boom := errors.New("stop")
	h2 := command.HookFunc(func(_ context.Context, _ command.Tx, _ command.Change) error { order = append(order, "h2"); return boom })
	h3 := command.HookFunc(func(_ context.Context, _ command.Tx, _ command.Change) error { order = append(order, "h3"); return nil })
	x, _ := command.NewExecutor(cmdRegistry(t), store, command.WithHooks(h1, h2, h3))

	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "P"}})
	if !errors.Is(err, boom) {
		t.Fatalf("want stop, got %v", err)
	}
	if strings.Join(order, ",") != "h1,h2" {
		t.Fatalf("hook order = %v, want [h1 h2] (h3 skipped)", order)
	}
}

func TestLifecycleHook_FiresPerCommandInBatch(t *testing.T) {
	store := newFakeStore()
	var count int
	hook := command.HookFunc(func(_ context.Context, _ command.Tx, _ command.Change) error { count++; return nil })
	x, _ := command.NewExecutor(cmdRegistry(t), store, command.WithHooks(hook))

	_, err := x.ExecBatch(acmeCtx(t), []command.Command{
		{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}},
		{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "B"}},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if count != 2 {
		t.Fatalf("hook fired %d times, want 2 (once per command)", count)
	}
}
