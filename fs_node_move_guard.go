package fabriq

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/fabriq/core/command"
	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// fsMoveGuardKey is the ctx key carrying the moved-node/new-parent pair from
// a MoveNode facade call to the in-tx cycle re-check hook. MoveNode's own
// ancestry walk runs before the command's transaction, so two concurrent
// moves (a under b ∥ b under a) can both pass it; the hook re-validates
// inside the transaction where the check is serialized.
type fsMoveGuardKey struct{}

type fsMoveGuard struct {
	movedID     string
	newParentID string
}

// fsMoveLockSQL serializes fs_node moves per tenant for the rest of the
// transaction (pg_advisory_xact_lock releases on commit/rollback) and stashes
// the guard pair for fsMoveCycleCheckSQL — DO blocks cannot take bind
// parameters, so they ride through set_config. The lock statement completes
// before the check statement starts, so the check's snapshot always sees
// every move committed by earlier lock holders. InTenantTx guarantees
// app.tenant_id is set; coalesce keeps the lock from silently no-oping on a
// NULL key should another command.Store implementation ever relax that.
const fsMoveLockSQL = `SELECT
	pg_advisory_xact_lock(hashtextextended('fabriq:fs_move:' || coalesce(current_setting('app.tenant_id', true), ''), 0)),
	set_config('fabriq.fs_move_moved', $1, true),
	set_config('fabriq.fs_move_parent', $2, true)`

// fsMoveCycleCheckSQL re-walks the new parent's ancestor chain inside the
// command transaction. The moved node's row is already updated in this
// transaction, so reaching it from the new parent means the move closes a
// cycle — RAISE aborts the command and everything rolls back.
var fsMoveCycleCheckSQL = fmt.Sprintf(`DO $$
DECLARE
	moved text := current_setting('fabriq.fs_move_moved', true);
	cur   text := current_setting('fabriq.fs_move_parent', true);
	hops  int  := 0;
BEGIN
	WHILE cur IS NOT NULL AND cur <> '' LOOP
		IF cur = moved THEN
			-- check_violation so translatePg classifies this as a
			-- constraint violation instead of an opaque internal error.
			RAISE EXCEPTION USING
				ERRCODE = 'check_violation',
				CONSTRAINT = 'fs_nodes_acyclic',
				TABLE = 'fs_nodes',
				MESSAGE = format('fabriq: fs_node move would create a cycle: new parent chain reaches %%s', moved);
		END IF;
		hops := hops + 1;
		IF hops > %d THEN
			RAISE EXCEPTION 'fabriq: fs_node ancestry exceeds max depth %d (cycle?)';
		END IF;
		SELECT parent_id INTO cur FROM fs_nodes WHERE id = cur;
	END LOOP;
END $$`, fsMaxDepth, fsMaxDepth)

// fsMoveCycleGuardHook returns the command-plane lifecycle hook that closes
// MoveNode's guard/command race. It is inert unless the facade stashed
// fsMoveGuardKey on ctx for this aggregate, so it costs nothing for every
// other write. On SQL stores it runs entirely over tx.Exec (no extra
// connection); stores without raw SQL (the fabriqtest fake, which serializes
// transactions) degrade to a committed-state walk over the relational port.
func fsMoveCycleGuardHook(rel query.RelationalQuerier) command.LifecycleHook {
	return command.HookFunc(func(ctx context.Context, tx command.Tx, change command.Change) error {
		g, ok := ctx.Value(fsMoveGuardKey{}).(*fsMoveGuard)
		if !ok || g == nil || change.Envelope.AggID != g.movedID ||
			change.Entity.Spec.Name != "fs_node" || change.Op != command.OpUpdate {
			return nil
		}
		err := tx.Exec(ctx, fsMoveLockSQL, g.movedID, g.newParentID)
		if errors.Is(err, fabriqerr.ErrRawSQLUnsupported) {
			return fsMoveCycleWalk(ctx, rel, g)
		}
		if err != nil {
			return err
		}
		return tx.Exec(ctx, fsMoveCycleCheckSQL)
	})
}

// fsMoveCycleWalk is the portable re-check for stores whose Tx cannot run raw
// SQL: walk the new parent's committed ancestor chain and reject the move if
// it reaches the moved node. Sound only because those stores run transactions
// one at a time — any conflicting move is either already committed (visible
// here) or runs after this transaction (and sees ours).
func fsMoveCycleWalk(ctx context.Context, rel query.RelationalQuerier, g *fsMoveGuard) error {
	cur := g.newParentID
	for hops := 0; cur != ""; hops++ {
		if cur == g.movedID {
			return fabriqerr.New(fabriqerr.CodeConstraintViolation,
				"fs_node move would create a cycle.",
				fabriqerr.WithEntity("fs_node", g.movedID),
				fabriqerr.WithMeta(fabriqerr.Meta{Constraint: "fs_nodes_acyclic", Table: "fs_nodes"}))
		}
		if hops > fsMaxDepth {
			return fmt.Errorf("fabriq: fs_node ancestry exceeds max depth %d (cycle?)", fsMaxDepth)
		}
		var n domain.FsNode
		if err := rel.Get(ctx, "fs_node", cur, &n); err != nil {
			if errors.Is(err, fabriqerr.ErrNotFound) {
				return nil // chain leaves visible rows: nothing left to cycle through
			}
			return err
		}
		cur = n.ParentID
	}
	return nil
}
