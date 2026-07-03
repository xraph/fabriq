package fabriq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/domain"
)

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
	if exists, err := f.siblingExists(ctx, node.ParentID, newName); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: RenameNode: %w", err)
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
// no descendant writes. Rejects moving into its own subtree by walking the
// new parent's ancestor chain.
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
	newParentPath, err := f.parentContext(ctx, newParentID)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
	}
	if newParentID != "" {
		parent, err := f.GetNode(ctx, newParentID)
		if err != nil {
			return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
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
	}
	if exists, err := f.siblingExists(ctx, newParentID, node.Name); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: MoveNode: %w", err)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
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
