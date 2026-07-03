package fabriq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// TestFsMoveConcurrentCycleGuard forces the guard/command interleaving the
// pre-command ancestry walk cannot see: two moves (a under b ∥ b under a)
// both pass the walk before either command executes. Without an in-tx
// re-check both persist and the parent_id graph gains a cycle; exactly one
// must fail. The fake store cannot run the SQL guard, so this exercises the
// fsMoveCycleWalk fallback end to end.
func TestFsMoveConcurrentCycleGuard(t *testing.T) {
	f := newFakeFabriq(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatalf("WithTenant: %v", err)
	}

	a, err := f.CreateFolder(ctx, "", "a")
	if err != nil {
		t.Fatalf("CreateFolder(a): %v", err)
	}
	b, err := f.CreateFolder(ctx, "", "b")
	if err != nil {
		t.Fatalf("CreateFolder(b): %v", err)
	}

	// Rendezvous: both goroutines finish MoveNode's pre-command guard before
	// either issues its command. Timed out (with a test failure) rather than
	// waited forever, so an unexpected pre-barrier error in one move cannot
	// hang the sibling until the package deadline.
	arrived := make(chan struct{}, 2)
	proceed := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(proceed) }) }
	defer release() // unblock the sibling goroutine even on barrier timeout
	bctx := context.WithValue(ctx, fsMoveBarrierKey{}, func() {
		arrived <- struct{}{}
		<-proceed
	})

	errs := make(chan error, 2)
	go func() {
		_, merr := f.MoveNode(bctx, a.ID, b.ID)
		errs <- merr
	}()
	go func() {
		_, merr := f.MoveNode(bctx, b.ID, a.ID)
		errs <- merr
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-arrived:
		case <-time.After(30 * time.Second):
			t.Fatal("barrier timeout: a move never reached the guard/command seam")
		}
	}
	release()

	var rejected []error
	for i := 0; i < 2; i++ {
		if merr := <-errs; merr != nil {
			rejected = append(rejected, merr)
			t.Logf("move rejected: %v", merr)
		}
	}
	if len(rejected) != 1 {
		t.Errorf("want exactly 1 of 2 racing moves rejected, got %d", len(rejected))
	}
	// The fallback veto surfaces through the same structured taxonomy as the
	// SQL guard: CodeConstraintViolation.
	var fe *fabriqerr.Error
	if len(rejected) == 1 {
		if !errors.As(rejected[0], &fe) || fe.Code != fabriqerr.CodeConstraintViolation {
			t.Errorf("rejected move error = %v, want fabriqerr code %s", rejected[0], fabriqerr.CodeConstraintViolation)
		}
	}

	// Whatever the outcome order, the tree must still be acyclic: both chains
	// resolve to a root instead of tripping the fsMaxDepth backstop.
	na, err := f.GetNode(ctx, a.ID)
	if err != nil {
		t.Fatalf("GetNode(a): %v", err)
	}
	nb, err := f.GetNode(ctx, b.ID)
	if err != nil {
		t.Fatalf("GetNode(b): %v", err)
	}
	if na.ParentID == b.ID && nb.ParentID == a.ID {
		t.Fatalf("persisted parent cycle: a.parent=%s b.parent=%s", na.ParentID, nb.ParentID)
	}
	if _, err := f.NodePath(ctx, a.ID); err != nil {
		t.Errorf("NodePath(a) after race: %v", err)
	}
	if _, err := f.NodePath(ctx, b.ID); err != nil {
		t.Errorf("NodePath(b) after race: %v", err)
	}
}
