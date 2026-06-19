package fabriq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// FsRef identifies a created/updated fs_node (analog of BlobRef).
type FsRef struct {
	ID       string `json:"id"`
	ParentID string `json:"parentId"`
	Name     string `json:"name"`
	Path     string `json:"path"`
	NodeType string `json:"nodeType"`
	Version  int64  `json:"version"`
}

// CreateFileOpts carries optional metadata for a file create.
type CreateFileOpts struct {
	ContentType string `json:"contentType"`
}

var (
	// ErrNodeNameConflict is returned when a live sibling already has the name.
	ErrNodeNameConflict = errors.New("fabriq: fs_node name already exists in folder")
	// ErrNotContainer is returned when a child is created under a non-folder.
	ErrNotContainer = errors.New("fabriq: parent is not a folder")
	// ErrNodeLocked is returned when a mutating op targets a locked node.
	ErrNodeLocked = errors.New("fabriq: fs_node is locked")
)

// childPath builds a node's materialized path. Root nodes (parentPath == "")
// become "/name"; deeper nodes become "parentPath/name".
func childPath(parentPath, name string) string {
	return parentPath + "/" + name
}

// parentContext loads the parent's path and validates it is a container.
// For root creation (parentID == "") it returns "" with no parent lookup.
func (f *Fabriq) parentContext(ctx context.Context, parentID string) (parentPath string, err error) {
	if parentID == "" {
		return "", nil
	}
	parent, err := f.GetNode(ctx, parentID)
	if err != nil {
		return "", err
	}
	if parent.NodeType != "folder" {
		return "", ErrNotContainer
	}
	return parent.Path, nil
}

// siblingExists reports whether a live (non-trashed) sibling already uses name.
func (f *Fabriq) siblingExists(ctx context.Context, parentID, name string) (bool, error) {
	var rows []domain.FsNode
	err := f.Relational().List(ctx, "fs_node", query.ListQuery{
		Where: query.Where{query.Eq("parent_id", parentID), query.Eq("name", name), query.IsNull("deleted_at")},
		Limit: 1,
	}, &rows)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// CreateFolder creates a folder node under parentID ("" = root). One event.
func (f *Fabriq) CreateFolder(ctx context.Context, parentID, name string) (FsRef, error) {
	parentPath, err := f.parentContext(ctx, parentID)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateFolder: %w", err)
	}
	if exists, err := f.siblingExists(ctx, parentID, name); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateFolder: %w", err)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
	}
	now := time.Now().UTC()
	node := &domain.FsNode{
		ParentID: parentID, Name: name, Path: childPath(parentPath, name),
		NodeType: "folder", Metadata: map[string]any{}, CreatedAt: now, UpdatedAt: now,
	}
	res, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpCreate, Payload: node})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateFolder: %w", err)
	}
	return FsRef{ID: res.AggID, ParentID: parentID, Name: name, Path: node.Path, NodeType: "folder", Version: res.Version}, nil
}

// CreateFile stores bytes (PutBlob → blob_object) then creates a file node
// referencing it (1:1), with denormalized facets. One blob event + one node event.
func (f *Fabriq) CreateFile(ctx context.Context, parentID, name string, r io.Reader, opts CreateFileOpts) (FsRef, error) {
	parentPath, err := f.parentContext(ctx, parentID)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateFile: %w", err)
	}
	if exists, err := f.siblingExists(ctx, parentID, name); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateFile: %w", err)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
	}
	blob, err := f.PutBlob(ctx, r, PutBlobOpts{ContentType: opts.ContentType})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateFile: put bytes: %w", err)
	}
	now := time.Now().UTC()
	node := &domain.FsNode{
		ParentID: parentID, Name: name, Path: childPath(parentPath, name), NodeType: "file",
		BlobID: blob.ID, Size: blob.Size, ContentType: opts.ContentType, Checksum: blob.Hash,
		Metadata: map[string]any{}, CreatedAt: now, UpdatedAt: now,
	}
	res, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpCreate, Payload: node})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateFile: create node: %w", err)
	}
	return FsRef{ID: res.AggID, ParentID: parentID, Name: name, Path: node.Path, NodeType: "file", Version: res.Version}, nil
}

// GetNode loads a node by id (any state, including trashed).
func (f *Fabriq) GetNode(ctx context.Context, id string) (domain.FsNode, error) {
	var n domain.FsNode
	if err := f.Relational().Get(ctx, "fs_node", id, &n); err != nil {
		return domain.FsNode{}, fmt.Errorf("fabriq: GetNode: %w", err)
	}
	return n, nil
}

// GetNodeByPath resolves a live node by its materialized path.
func (f *Fabriq) GetNodeByPath(ctx context.Context, path string) (domain.FsNode, error) {
	var rows []domain.FsNode
	err := f.Relational().List(ctx, "fs_node", query.ListQuery{
		Where: query.Where{query.Eq("path", path), query.IsNull("deleted_at")},
		Limit: 1,
	}, &rows)
	if err != nil {
		return domain.FsNode{}, fmt.Errorf("fabriq: GetNodeByPath: %w", err)
	}
	if len(rows) == 0 {
		return domain.FsNode{}, fmt.Errorf("fabriq: GetNodeByPath %q: %w", path, fabriqerr.ErrNotFound)
	}
	return rows[0], nil
}

// ListChildren returns the live children of parentID, ordered by name.
func (f *Fabriq) ListChildren(ctx context.Context, parentID string, limit, offset int) ([]domain.FsNode, error) {
	var rows []domain.FsNode
	err := f.Relational().List(ctx, "fs_node", query.ListQuery{
		Where:   query.Where{query.Eq("parent_id", parentID), query.IsNull("deleted_at")},
		OrderBy: "name ASC",
		Limit:   limit, Offset: offset,
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("fabriq: ListChildren: %w", err)
	}
	return rows, nil
}

// Ancestors returns the chain from the root down to (but excluding) the node,
// by walking parent_id. O(depth) reads — fine for filesystem depths.
// The returned slice is root→node order.
func (f *Fabriq) Ancestors(ctx context.Context, id string) ([]domain.FsNode, error) {
	node, err := f.GetNode(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fabriq: Ancestors: %w", err)
	}
	var chain []domain.FsNode
	for node.ParentID != "" {
		parent, err := f.GetNode(ctx, node.ParentID)
		if err != nil {
			return nil, fmt.Errorf("fabriq: Ancestors: %w", err)
		}
		chain = append(chain, parent)
		node = parent
	}
	// chain is leaf→root; reverse to root→leaf.
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

// Descendants returns all live nodes under id (by path prefix), ordered by path.
func (f *Fabriq) Descendants(ctx context.Context, id string) ([]domain.FsNode, error) {
	node, err := f.GetNode(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fabriq: Descendants: %w", err)
	}
	var rows []domain.FsNode
	err = f.Relational().List(ctx, "fs_node", query.ListQuery{
		Where:   query.Where{query.Like("path", node.Path+"/%"), query.IsNull("deleted_at")},
		OrderBy: "path ASC",
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("fabriq: Descendants: %w", err)
	}
	return rows, nil
}

// SearchNodesByName does a live SQL ILIKE name search. The Elasticsearch
// projection (Search Spec) is the scalable/fuzzy path when ES is configured.
func (f *Fabriq) SearchNodesByName(ctx context.Context, q string, limit int) ([]domain.FsNode, error) {
	var rows []domain.FsNode
	err := f.Relational().List(ctx, "fs_node", query.ListQuery{
		Where:   query.Where{query.ILike("name", "%"+q+"%"), query.IsNull("deleted_at")},
		OrderBy: "name ASC",
		Limit:   limit,
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("fabriq: SearchNodesByName: %w", err)
	}
	return rows, nil
}

// WatchChildren returns a maintained result set of a folder's live children,
// ordered by name — the per-folder live view the explorer subscribes to.
// Trashed children (deleted_at IS NOT NULL) are excluded from the live window.
// Single-shard deployments only (the LiveQuery constraint).
func (f *Fabriq) WatchChildren(ctx context.Context, parentID string, limit int) (livequery.Snapshot, <-chan livequery.LiveDelta, *livequery.Handle, error) {
	return f.LiveQuery(ctx, livequery.LiveQuery{
		Entity: "fs_node",
		Where:  query.Where{query.Eq("parent_id", parentID), query.IsNull("deleted_at")},
		Sort:   []livequery.SortKey{{Column: "name"}},
		Limit:  limit,
	})
}
