package fabriq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/domain"
)

// fsPathRewriteKey is the ctx key carrying the old/new path prefixes from a
// move/rename facade call to the in-tx path-rewrite hook. The hook cannot read
// the prior row value (it fires after ApplyChange), so the facade — which read
// the node before the command — hands the prefixes through ctx.
type fsPathRewriteKey struct{}

type fsPathRewrite struct {
	movedID   string
	oldPrefix string
	newPrefix string
}

// fsPathRewriteHook rewrites descendant paths inside the move/rename command's
// transaction. It is inert unless the facade set fsPathRewriteKey on ctx, so it
// is safe to register for every Fabriq and costs nothing for non-move writes.
var fsPathRewriteHook = command.HookFunc(func(ctx context.Context, tx command.Tx, change command.Change) error {
	pr, ok := ctx.Value(fsPathRewriteKey{}).(*fsPathRewrite)
	if !ok || pr == nil || change.Envelope.AggID != pr.movedID {
		return nil
	}
	// '/%' (not '%') so siblings like "/ax" don't match the prefix "/a".
	// id <> $3 leaves the moved node itself (already updated by the command).
	return tx.Exec(ctx,
		`UPDATE fabriq_fs_nodes
		    SET path = $1 || substr(path, length($2) + 1), updated_at = now()
		  WHERE tenant_id = current_setting('app.tenant_id', true)
		    AND path LIKE $2 || '/%'
		    AND id <> $3`,
		pr.newPrefix, pr.oldPrefix, pr.movedID)
})

// dirOf returns the parent-path portion of a node path ("/a/b/c" -> "/a/b",
// "/a" -> "").
func dirOf(path string) string {
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return ""
	}
	return path[:i]
}

// RenameNode renames a node in place, rewriting its own path and (in the same
// tx) all descendant paths. One event for the node.
func (f *Fabriq) RenameNode(ctx context.Context, id, newName string) (FsRef, error) {
	node, err := f.GetNode(ctx, id)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: RenameNode: %w", err)
	}
	if node.IsLocked {
		return FsRef{}, ErrNodeLocked
	}
	if exists, err := f.siblingExists(ctx, node.ParentID, newName); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: RenameNode: %w", err)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
	}
	oldPath := node.Path
	newPath := childPath(dirOf(oldPath), newName)
	return f.applyMove(ctx, &node, node.ParentID, newName, oldPath, newPath)
}

// MoveNode re-parents a node under newParentID, rewriting its own path and all
// descendant paths in the same tx. Rejects moving into its own subtree.
func (f *Fabriq) MoveNode(ctx context.Context, id, newParentID string) (FsRef, error) {
	node, err := f.GetNode(ctx, id)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
	}
	if node.IsLocked {
		return FsRef{}, ErrNodeLocked
	}
	newParentPath, err := f.parentContext(ctx, newParentID)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
	}
	// Cycle guard: the new parent must not be the node or under it.
	if newParentID == id || newParentPath == node.Path || strings.HasPrefix(newParentPath, node.Path+"/") {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: cannot move %q into its own subtree", id)
	}
	if exists, err := f.siblingExists(ctx, newParentID, node.Name); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
	}
	oldPath := node.Path
	newPath := childPath(newParentPath, node.Name)
	return f.applyMove(ctx, &node, newParentID, node.Name, oldPath, newPath)
}

// applyMove issues the single node OpUpdate with the new parent/name/path and
// hands the prefixes to the path-rewrite hook via ctx.
func (f *Fabriq) applyMove(ctx context.Context, node *domain.FsNode, newParentID, newName, oldPath, newPath string) (FsRef, error) {
	node.ParentID = newParentID
	node.Name = newName
	node.Path = newPath
	node.UpdatedAt = time.Now().UTC()
	rctx := context.WithValue(ctx, fsPathRewriteKey{}, &fsPathRewrite{movedID: node.ID, oldPrefix: oldPath, newPrefix: newPath})
	res, err := f.exec.Exec(rctx, command.Command{Entity: "fs_node", Op: command.OpUpdate, AggID: node.ID, Payload: node})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: move/rename: %w", err)
	}
	return FsRef{ID: node.ID, ParentID: newParentID, Name: newName, Path: newPath, NodeType: node.NodeType, Version: res.Version}, nil
}
