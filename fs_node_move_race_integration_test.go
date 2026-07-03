//go:build integration

package fabriq_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// TestFsMoveConcurrentCycleGuard forces the interleaving MoveNode's
// pre-command ancestry walk cannot see: two moves (a under b ∥ b under a)
// both pass the guard before either command's transaction runs. The in-tx
// re-check (per-tenant advisory lock + ancestor walk) must reject exactly
// one, so no parent_id cycle is ever persisted.
func TestFsMoveConcurrentCycleGuard(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	a, err := f.CreateFolder(tctx, "", "a")
	if err != nil {
		t.Fatalf("CreateFolder(a): %v", err)
	}
	b, err := f.CreateFolder(tctx, "", "b")
	if err != nil {
		t.Fatalf("CreateFolder(b): %v", err)
	}

	// Rendezvous: both goroutines finish the pre-command guard before either
	// opens its command transaction. Timed out (with a test failure) rather
	// than waited forever, so an unexpected pre-barrier error in one move
	// cannot hang the sibling until the package deadline.
	arrived := make(chan struct{}, 2)
	proceed := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(proceed) }) }
	defer release() // unblock the sibling goroutine even on barrier timeout
	bctx := fabriq.WithFsMoveBarrier(tctx, func() {
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
	// The veto must surface through the structured taxonomy (RAISE
	// check_violation -> translatePg -> CodeConstraintViolation), not as an
	// opaque internal error.
	var fe *fabriqerr.Error
	if len(rejected) == 1 {
		if !errors.As(rejected[0], &fe) || fe.Code != fabriqerr.CodeConstraintViolation {
			t.Errorf("rejected move error = %v, want fabriqerr code %s", rejected[0], fabriqerr.CodeConstraintViolation)
		}
	}

	// Whatever the commit order, the tree must remain acyclic: both chains
	// resolve to a root instead of tripping the fsMaxDepth backstop.
	na, err := f.GetNode(tctx, a.ID)
	if err != nil {
		t.Fatalf("GetNode(a): %v", err)
	}
	nb, err := f.GetNode(tctx, b.ID)
	if err != nil {
		t.Fatalf("GetNode(b): %v", err)
	}
	if na.ParentID == b.ID && nb.ParentID == a.ID {
		t.Fatalf("persisted parent cycle: a.parent=%s b.parent=%s", na.ParentID, nb.ParentID)
	}
	if _, err := f.NodePath(tctx, a.ID); err != nil {
		t.Errorf("NodePath(a) after race: %v", err)
	}
	if _, err := f.NodePath(tctx, b.ID); err != nil {
		t.Errorf("NodePath(b) after race: %v", err)
	}
}
