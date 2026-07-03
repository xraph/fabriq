package fabriq

import (
	"context"
	"errors"
	"fmt"
	"sort"
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

// pathFromChain builds the absolute path ("/a/b/name") from a
// [node, parent, ..., root] chain as returned by chainToRoot.
func pathFromChain(chain []domain.FsNode) string {
	var b strings.Builder
	for i := len(chain) - 1; i >= 0; i-- {
		b.WriteString("/")
		b.WriteString(chain[i].Name)
	}
	return b.String()
}

// nodePathOf computes node's absolute path ("/a/b/name") by walking the
// parent chain. Paths are read-time derivations now — nothing persists them.
func (f *Fabriq) nodePathOf(ctx context.Context, node domain.FsNode) (string, error) {
	chain, err := f.chainToRoot(ctx, node)
	if err != nil {
		return "", err
	}
	return pathFromChain(chain), nil
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

// descendantNodes returns every node under rootID (root excluded) in
// depth-first path order. SQL backends run one recursive CTE; backends that
// report ErrRawSQLUnsupported (e.g. the fabriqtest in-memory fake) fall back
// to descendantsAdjacencyWalk, a portable List-based walk with identical
// semantics. The recursion is NOT filtered by deleted_at — a live grandchild
// under a trashed folder is still found (matches the old path-prefix
// semantics); includeTrashed only controls the final result filter. Tenant
// scoping rides on the RelationalQuerier tenant guard (RLS on SQL backends;
// the fake's scope check on List). Ordering is byte-wise (COLLATE "C") to
// stay identical to descendantsAdjacencyWalk's Go `<` sort.
func (f *Fabriq) descendantNodes(ctx context.Context, rootID string, includeTrashed bool) ([]domain.FsNode, error) {
	var rows []domain.FsNode
	err := f.Relational().Query(ctx, &rows, `
		WITH RECURSIVE sub (id, rel_path, depth) AS (
			SELECT n.id, ''::text, 0
			  FROM fabriq_fs_nodes n
			 WHERE n.id = $1
			UNION ALL
			SELECT c.id, sub.rel_path || '/' || c.name, sub.depth + 1
			  FROM fabriq_fs_nodes c
			  JOIN sub ON c.parent_id = sub.id
			 WHERE sub.depth < $3
		)
		SELECT f.*
		  FROM fabriq_fs_nodes f
		  JOIN sub ON sub.id = f.id
		 WHERE f.id <> $1
		   AND ($2::boolean OR f.deleted_at IS NULL)
		 ORDER BY sub.rel_path COLLATE "C"`,
		rootID, includeTrashed, fsMaxDepth)
	if err != nil {
		if errors.Is(err, fabriqerr.ErrRawSQLUnsupported) {
			return f.descendantsAdjacencyWalk(ctx, rootID, includeTrashed)
		}
		return nil, err
	}
	return rows, nil
}

// descendantsAdjacencyWalk reproduces descendantNodes' CTE semantics on
// backends with no raw-SQL escape hatch: a breadth-first walk over parent_id
// via List, with no deleted_at filter during traversal (a live grandchild
// under a trashed folder must still be found), root excluded from the
// result, includeTrashed applied only to the final set, depth bounded by
// fsMaxDepth (cycle backstop — returns an error rather than looping
// silently), and the result sorted by relative path (byte-wise, COLLATE "C")
// to match "ORDER BY sub.rel_path". One known divergence: at fsMaxDepth the
// CTE silently truncates the subtree (its recursive term just stops
// expanding), while this walk returns an error.
func (f *Fabriq) descendantsAdjacencyWalk(ctx context.Context, rootID string, includeTrashed bool) ([]domain.FsNode, error) {
	type queued struct {
		node      domain.FsNode
		parentRel string
		depth     int
	}

	var frontier []queued
	frontier = append(frontier, queued{node: domain.FsNode{ID: rootID}, parentRel: "", depth: 0})

	type found struct {
		node    domain.FsNode
		relPath string
	}
	var results []found

	for len(frontier) > 0 {
		cur := frontier[0]
		frontier = frontier[1:]

		if cur.depth >= fsMaxDepth {
			return nil, fmt.Errorf("fabriq: fs_node %s exceeds max depth %d (cycle?)", rootID, fsMaxDepth)
		}

		var kids []domain.FsNode
		err := f.Relational().List(ctx, "fs_node", query.ListQuery{
			Where: query.Where{query.Eq("parent_id", cur.node.ID)},
		}, &kids)
		if err != nil {
			return nil, err
		}

		for _, kid := range kids {
			relPath := cur.parentRel + "/" + kid.Name
			results = append(results, found{node: kid, relPath: relPath})
			frontier = append(frontier, queued{node: kid, parentRel: relPath, depth: cur.depth + 1})
		}
	}

	sort.Slice(results, func(i, j int) bool { return results[i].relPath < results[j].relPath })

	out := make([]domain.FsNode, 0, len(results))
	for _, r := range results {
		if includeTrashed || r.node.DeletedAt == nil {
			out = append(out, r.node)
		}
	}
	return out, nil
}
