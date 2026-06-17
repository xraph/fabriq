package command_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/command"
)

func TestPostCommitHookRunsOnSuccess(t *testing.T) {
	store := newFakeStore()
	var got []command.Change
	hook := command.PostCommitFunc(func(_ context.Context, changes []command.Change) {
		got = append(got, changes...)
	})
	x, err := command.NewExecutor(cmdRegistry(t), store, command.WithPostCommitHooks(hook))
	if err != nil {
		t.Fatal(err)
	}
	ctx := acmeCtx(t)
	if _, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "Plant"}}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 post-commit change, got %d", len(got))
	}
	if got[0].Entity.Spec.Name != "site" || got[0].Op != command.OpCreate {
		t.Fatalf("unexpected change: entity=%s op=%v", got[0].Entity.Spec.Name, got[0].Op)
	}
}

func TestPostCommitHookSkippedOnRollback(t *testing.T) {
	store := newFakeStore()
	called := false
	hook := command.PostCommitFunc(func(context.Context, []command.Change) { called = true })
	// A veto hook forces the transaction to roll back.
	veto := command.HookFunc(func(_ context.Context, _ command.Tx, _ command.Change) error {
		return errors.New("veto")
	})
	x, err := command.NewExecutor(cmdRegistry(t), store,
		command.WithHooks(veto), command.WithPostCommitHooks(hook))
	if err != nil {
		t.Fatal(err)
	}
	_, err = x.Exec(acmeCtx(t), command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "Plant"}})
	if err == nil {
		t.Fatal("expected the veto to abort the command")
	}
	if called {
		t.Fatal("post-commit hook must NOT run when the transaction rolled back")
	}
}
