package fabriq

import (
	"context"
	"fmt"
	"strings"

	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// fsMaxDepth bounds every parent-chain walk and the descendant CTE. The move
// cycle guard makes real cycles impossible; this is the defensive backstop so
// corrupt data degrades to an error instead of an infinite loop.
const fsMaxDepth = 512

// splitFsPath validates an absolute fs path and returns its segments.
// "/a/b" -> ["a","b"]. Rejects "", "/", relative paths and empty segments.
func splitFsPath(path string) ([]string, error) {
	if path == "" || path[0] != '/' {
		return nil, fmt.Errorf("fabriq: fs path must be absolute, got %q", path)
	}
	segs := strings.Split(path[1:], "/")
	for _, s := range segs {
		if s == "" {
			return nil, fmt.Errorf("fabriq: fs path %q has an empty segment", path)
		}
	}
	return segs, nil
}

// chainToRoot walks parent_id from node up to the root, returning
// [node, parent, ..., root]. O(depth) point reads.
func (f *Fabriq) chainToRoot(ctx context.Context, node domain.FsNode) ([]domain.FsNode, error) {
	chain := []domain.FsNode{node}
	for node.ParentID != "" {
		if len(chain) > fsMaxDepth {
			return nil, fmt.Errorf("fabriq: fs_node %s exceeds max depth %d (cycle?)", chain[0].ID, fsMaxDepth)
		}
		parent, err := f.GetNode(ctx, node.ParentID)
		if err != nil {
			return nil, err
		}
		chain = append(chain, parent)
		node = parent
	}
	return chain, nil
}

// nodePathOf computes node's absolute path ("/a/b/name") by walking the
// parent chain. Paths are read-time derivations now — nothing persists them.
func (f *Fabriq) nodePathOf(ctx context.Context, node domain.FsNode) (string, error) {
	chain, err := f.chainToRoot(ctx, node)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for i := len(chain) - 1; i >= 0; i-- {
		b.WriteString("/")
		b.WriteString(chain[i].Name)
	}
	return b.String(), nil
}

// NodePath returns the node's absolute path, computed from the adjacency
// chain. O(depth) reads — fine for filesystem depths.
func (f *Fabriq) NodePath(ctx context.Context, id string) (string, error) {
	node, err := f.GetNode(ctx, id)
	if err != nil {
		return "", fmt.Errorf("fabriq: NodePath: %w", err)
	}
	p, err := f.nodePathOf(ctx, node)
	if err != nil {
		return "", fmt.Errorf("fabriq: NodePath: %w", err)
	}
	return p, nil
}

// GetNodeByPath resolves a live node by descending the tree one segment at a
// time on the (parent_id, name) unique index. O(depth) point reads.
func (f *Fabriq) GetNodeByPath(ctx context.Context, path string) (domain.FsNode, error) {
	segs, err := splitFsPath(path)
	if err != nil {
		return domain.FsNode{}, fmt.Errorf("fabriq: GetNodeByPath: %w", err)
	}
	parentID := ""
	var node domain.FsNode
	for _, name := range segs {
		var rows []domain.FsNode
		err := f.Relational().List(ctx, "fs_node", query.ListQuery{
			Where: query.Where{query.Eq("parent_id", parentID), query.Eq("name", name), query.IsNull("deleted_at")},
			Limit: 1,
		}, &rows)
		if err != nil {
			return domain.FsNode{}, fmt.Errorf("fabriq: GetNodeByPath: %w", err)
		}
		if len(rows) == 0 {
			return domain.FsNode{}, fmt.Errorf("fabriq: GetNodeByPath %q: %w", path, fabriqerr.ErrNotFound)
		}
		node = rows[0]
		parentID = node.ID
	}
	return node, nil
}
