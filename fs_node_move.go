package fabriq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/domain"
)

// fsMoveBarrierKey carries a test-only rendezvous func, invoked after
// MoveNode's pre-command ancestry guard and before the command executes. It
// exists so tests can force the guard/command interleaving of two concurrent
// moves; production contexts never carry it.
type fsMoveBarrierKey struct{}

// dirOf returns the parent-path portion of a node path ("/a/b/c" -> "/a/b",
// "/a" -> "").
func dirOf(path string) string {
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return ""
	}
	return path[:i]
}

// RenameNode renames a node in place. One command, one event — descendants
// are untouched because paths are derived from adjacency at read time.
func (f *Fabriq) RenameNode(ctx context.Context, id, newName string) (FsRef, error) {
	node, err := f.GetNode(ctx, id)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: RenameNode: %w", err)
	}
	if node.IsLocked {
		return FsRef{}, ErrNodeLocked
	}
	if exists, serr := f.siblingExists(ctx, node.ParentID, newName); serr != nil {
		return FsRef{}, fmt.Errorf("fabriq: RenameNode: %w", serr)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
	}
	oldPath, err := f.nodePathOf(ctx, node)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: RenameNode: %w", err)
	}
	return f.applyMove(ctx, &node, node.ParentID, newName, childPath(dirOf(oldPath), newName))
}

// MoveNode re-parents a node under newParentID. One command, one event —
// no descendant writes. Validates newParentID is a folder and rejects moving
// into its own subtree, both from a single walk of the new parent's ancestor
// chain (also used to derive the new path) rather than two.
func (f *Fabriq) MoveNode(ctx context.Context, id, newParentID string) (FsRef, error) {
	node, err := f.GetNode(ctx, id)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
	}
	if node.IsLocked {
		return FsRef{}, ErrNodeLocked
	}
	if newParentID == id {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: cannot move %q into its own subtree", id)
	}
	var newParentPath string
	if newParentID != "" {
		parent, err := f.GetNode(ctx, newParentID)
		if err != nil {
			return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
		}
		if parent.NodeType != "folder" {
			return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", ErrNotContainer)
		}
		chain, err := f.chainToRoot(ctx, parent)
		if err != nil {
			return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
		}
		for _, anc := range chain {
			if anc.ID == id {
				return FsRef{}, fmt.Errorf("fabriq: MoveNode: cannot move %q into its own subtree", id)
			}
		}
		newParentPath = pathFromChain(chain)
	}
	if exists, err := f.siblingExists(ctx, newParentID, node.Name); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
	}
	if fn, ok := ctx.Value(fsMoveBarrierKey{}).(func()); ok && fn != nil {
		fn()
	}
	// The walk above ran before the command's transaction, so a concurrent
	// move can invalidate it (a under b ∥ b under a both pass). Hand the pair
	// to fsMoveCycleGuardHook, which re-validates inside the transaction.
	// Moves to root (newParentID == "") skip it: they cannot create a cycle,
	// and staying guard-free keeps them usable as the corruption-repair path.
	if newParentID != "" {
		ctx = context.WithValue(ctx, fsMoveGuardKey{}, &fsMoveGuard{movedID: id, newParentID: newParentID})
	}
	return f.applyMove(ctx, &node, newParentID, node.Name, childPath(newParentPath, node.Name))
}

// applyMove issues the single node OpUpdate with the new parent/name.
// newPath is only echoed in the FsRef — nothing persists it.
func (f *Fabriq) applyMove(ctx context.Context, node *domain.FsNode, newParentID, newName, newPath string) (FsRef, error) {
	node.ParentID = newParentID
	node.Name = newName
	node.UpdatedAt = time.Now().UTC()
	res, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpUpdate, AggID: node.ID, Payload: node})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: move/rename: %w", err)
	}
	return FsRef{ID: node.ID, ParentID: newParentID, Name: newName, Path: newPath, NodeType: node.NodeType, Version: res.Version}, nil
}
