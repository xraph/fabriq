package fabriq

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/domain"
)

// CreateMount creates a mount node (node_type=mount) under parentID with the
// given mount configuration. The sync engine that consumes the config lives in
// the seam, not in fabriq.
func (f *Fabriq) CreateMount(ctx context.Context, parentID, name string, mountConfig map[string]any) (FsRef, error) {
	if err := validateNodeName(name); err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateMount: %w", err)
	}
	if mountConfig == nil {
		mountConfig = map[string]any{}
	}
	parentPath, err := f.parentContext(ctx, parentID)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateMount: %w", err)
	}
	if exists, serr := f.siblingExists(ctx, parentID, name); serr != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateMount: %w", serr)
	} else if exists {
		return FsRef{}, ErrNodeNameConflict
	}
	now := time.Now().UTC()
	node := &domain.FsNode{
		ParentID: parentID, Name: name, NodeType: "mount",
		MountConfig: mountConfig, Metadata: map[string]any{}, CreatedAt: now, UpdatedAt: now,
	}
	res, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpCreate, Payload: node})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: CreateMount: %w", err)
	}
	return FsRef{ID: res.AggID, ParentID: parentID, Name: name, Path: childPath(parentPath, name), NodeType: "mount", Version: res.Version}, nil
}

// ResolveMount returns a mount node's configuration.
func (f *Fabriq) ResolveMount(ctx context.Context, id string) (map[string]any, error) {
	n, err := f.GetNode(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fabriq: ResolveMount: %w", err)
	}
	if n.NodeType != "mount" {
		return nil, fmt.Errorf("fabriq: ResolveMount: %q is not a mount", id)
	}
	return n.MountConfig, nil
}

// UpdateMount replaces a mount node's configuration.
func (f *Fabriq) UpdateMount(ctx context.Context, id string, mountConfig map[string]any) (FsRef, error) {
	n, err := f.GetNode(ctx, id)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: UpdateMount: %w", err)
	}
	if n.NodeType != "mount" {
		return FsRef{}, fmt.Errorf("fabriq: UpdateMount: %q is not a mount", id)
	}
	p, err := f.nodePathOf(ctx, n)
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: UpdateMount: %w", err)
	}
	n.MountConfig = mountConfig
	n.UpdatedAt = time.Now().UTC()
	res, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpUpdate, AggID: id, Payload: &n})
	if err != nil {
		return FsRef{}, fmt.Errorf("fabriq: UpdateMount: %w", err)
	}
	return FsRef{ID: id, ParentID: n.ParentID, Name: n.Name, Path: p, NodeType: "mount", Version: res.Version}, nil
}
