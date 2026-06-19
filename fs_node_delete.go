package fabriq

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// subtree returns node plus all its descendants (by path prefix), live or
// trashed. Order is unspecified.
func (f *Fabriq) subtree(ctx context.Context, node domain.FsNode) ([]domain.FsNode, error) {
	var desc []domain.FsNode
	err := f.Relational().List(ctx, "fs_node", query.ListQuery{
		Where: query.Where{query.Like("path", node.Path+"/%")},
	}, &desc)
	if err != nil {
		return nil, err
	}
	return append([]domain.FsNode{node}, desc...), nil
}

// setDeleted stamps (or clears) deleted_at/deleted_by across the subtree, one
// OpUpdate event per node (so projections deindex/reindex each).
func (f *Fabriq) setDeleted(ctx context.Context, id string, deleted bool) error {
	root, err := f.GetNode(ctx, id)
	if err != nil {
		return err
	}
	nodes, err := f.subtree(ctx, root)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	cmds := make([]command.Command, 0, len(nodes))
	for i := range nodes {
		n := nodes[i] // copy
		if deleted {
			n.DeletedAt = &now
			n.DeletedBy = ""
		} else {
			n.DeletedAt = nil
			n.DeletedBy = ""
		}
		n.UpdatedAt = now
		cmds = append(cmds, command.Command{Entity: "fs_node", Op: command.OpUpdate, AggID: n.ID, Payload: &n})
	}
	if _, err := f.exec.ExecBatch(ctx, cmds); err != nil {
		return err
	}
	return nil
}

// TrashNode soft-deletes a node and its whole subtree.
func (f *Fabriq) TrashNode(ctx context.Context, id string) error {
	if err := f.setDeleted(ctx, id, true); err != nil {
		return fmt.Errorf("fabriq: TrashNode: %w", err)
	}
	return nil
}

// RestoreNode clears soft-delete on a node and its whole subtree.
func (f *Fabriq) RestoreNode(ctx context.Context, id string) error {
	if err := f.setDeleted(ctx, id, false); err != nil {
		return fmt.Errorf("fabriq: RestoreNode: %w", err)
	}
	return nil
}

// PermanentDeleteNode hard-deletes a node and its subtree (one OpDelete event
// each) and deletes each file node's blob_object so Phase-4 GC reclaims bytes.
func (f *Fabriq) PermanentDeleteNode(ctx context.Context, id string) error {
	root, err := f.GetNode(ctx, id)
	if err != nil {
		return fmt.Errorf("fabriq: PermanentDeleteNode: %w", err)
	}
	nodes, err := f.subtree(ctx, root)
	if err != nil {
		return fmt.Errorf("fabriq: PermanentDeleteNode: %w", err)
	}
	cmds := make([]command.Command, 0, len(nodes))
	var blobIDs []string
	for _, n := range nodes {
		cmds = append(cmds, command.Command{Entity: "fs_node", Op: command.OpDelete, AggID: n.ID})
		if n.NodeType == "file" && n.BlobID != "" {
			blobIDs = append(blobIDs, n.BlobID)
		}
	}
	if _, err := f.exec.ExecBatch(ctx, cmds); err != nil {
		return fmt.Errorf("fabriq: PermanentDeleteNode: nodes: %w", err)
	}
	for _, bid := range blobIDs {
		if err := f.DeleteBlob(ctx, bid); err != nil {
			return fmt.Errorf("fabriq: PermanentDeleteNode: blob %s: %w", bid, err)
		}
	}
	return nil
}

// LockNode marks a node locked by `by`.
func (f *Fabriq) LockNode(ctx context.Context, id, by string) error {
	return f.patchNode(ctx, id, func(n *domain.FsNode) { n.IsLocked = true; n.LockedBy = by })
}

// UnlockNode clears a node's lock.
func (f *Fabriq) UnlockNode(ctx context.Context, id string) error {
	return f.patchNode(ctx, id, func(n *domain.FsNode) { n.IsLocked = false; n.LockedBy = "" })
}

// patchNode loads, mutates, and OpUpdates a single node (full-row replace).
func (f *Fabriq) patchNode(ctx context.Context, id string, mutate func(*domain.FsNode)) error {
	n, err := f.GetNode(ctx, id)
	if err != nil {
		return err
	}
	mutate(&n)
	n.UpdatedAt = time.Now().UTC()
	if _, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpUpdate, AggID: id, Payload: &n}); err != nil {
		return err
	}
	return nil
}

// ReplaceFile swaps a file node's bytes for new content: PutBlob a new
// blob_object, repoint blob_id + denormalized facets, bump version.
// The previous blob_object is intentionally NOT deleted (a prior version may
// still be referenced elsewhere; Phase-4 GC reclaims genuinely-unreferenced bytes).
func (f *Fabriq) ReplaceFile(ctx context.Context, id string, r io.Reader, opts CreateFileOpts) (FsRef, error) {
	n, err := f.GetNode(ctx, id)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: ReplaceFile: %w", err)
	}
	if n.NodeType != "file" {
		return FsRef{}, fmt.Errorf("fabriq: ReplaceFile: %q is not a file", id)
	}
	if n.IsLocked {
		return FsRef{}, ErrNodeLocked
	}
	blob, err := f.PutBlob(ctx, r, PutBlobOpts{ContentType: opts.ContentType})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: ReplaceFile: put bytes: %w", err)
	}
	n.BlobID = blob.ID
	n.Size = blob.Size
	n.ContentType = opts.ContentType
	n.Checksum = blob.Hash
	n.UpdatedAt = time.Now().UTC()
	res, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpUpdate, AggID: id, Payload: &n})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: ReplaceFile: update node: %w", err)
	}
	return FsRef{ID: id, ParentID: n.ParentID, Name: n.Name, Path: n.Path, NodeType: "file", Version: res.Version}, nil
}
